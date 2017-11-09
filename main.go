package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/big"
	"net/http"
	"regexp"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	ct "github.com/google/certificate-transparency-go"
	"github.com/google/certificate-transparency-go/client"
	"github.com/google/certificate-transparency-go/jsonclient"
	"github.com/google/certificate-transparency-go/scanner"
)

var logURI = flag.String("log_uri", "http://ct.googleapis.com/aviator", "CT log base URI")
var matchSubjectRegex = flag.String("match_subject_regex", ".*", "Regex to match CN/SAN")
var matchIssuerRegex = flag.String("match_issuer_regex", "", "Regex to match in issuer CN")
var serialNumber = flag.String("serial_number", "", "Serial number of certificate of interest")
var batchSize = flag.Int("batch_size", 1000, "Max number of entries to request at per call to get-entries")
var numWorkers = flag.Int("num_workers", 2, "Number of concurrent matchers")
var parallelFetch = flag.Int("parallel_fetch", 2, "Number of concurrent GetEntries fetches")
var startIndex = flag.Int64("start_index", 0, "Log index to start scanning at")
var kafkaBroker = flag.String("kafka_broker", "", "The url of the Kafka broker")
var kafkaTopic = flag.String("kafka_topic", "", "The topic of the Kafka stream to send to")

func createRegexes(regexValue string) (*regexp.Regexp, *regexp.Regexp) {
	// Make a regex matcher
	var certRegex *regexp.Regexp
	precertRegex := regexp.MustCompile(regexValue)
	certRegex = precertRegex

	return certRegex, precertRegex
}

func createMatcherFromFlags() (scanner.Matcher, error) {
	if *matchIssuerRegex != "" {
		certRegex, precertRegex := createRegexes(*matchIssuerRegex)
		return scanner.MatchIssuerRegex{
			CertificateIssuerRegex:    certRegex,
			PrecertificateIssuerRegex: precertRegex}, nil
	}
	if *serialNumber != "" {
		log.Printf("Using SerialNumber matcher on %s", *serialNumber)
		var sn big.Int
		_, success := sn.SetString(*serialNumber, 0)
		if !success {
			return nil, fmt.Errorf("Invalid serialNumber %s", *serialNumber)
		}
		return scanner.MatchSerialNumber{SerialNumber: sn}, nil
	}
	certRegex, precertRegex := createRegexes(*matchSubjectRegex)
	return scanner.MatchSubjectRegex{
		CertificateSubjectRegex:    certRegex,
		PrecertificateSubjectRegex: precertRegex}, nil
}

var kafkaProducer sarama.SyncProducer

func logToKafka(entry *ct.LogEntry) {
	certMsg, err := json.Marshal(entry)
	if err != nil {
		log.Errorf("Failed to marshal json of certificate message: %s", err)
		return
	}
	msg := &sarama.ProducerMessage{Topic: *kafkaTopic, Value: sarama.StringEncoder(certMsg)}
	partition, offset, err := kafkaProducer.SendMessage(msg)
	if err != nil {
		log.Errorf("FAILED to send message: %s\n", err)
		return
	}

	log.Debugf("> message sent to partition %d at offset %d\n", partition, offset)
}

func main() {
	flag.Parse()
	logClient, err := client.New(*logURI, &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			TLSHandshakeTimeout:   30 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
			MaxIdleConnsPerHost:   10,
			DisableKeepAlives:     false,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}, jsonclient.Options{})
	if err != nil {
		log.Fatal(err)
	}

	kafkaProducer, err = sarama.NewSyncProducer([]string{*kafkaBroker}, nil)
	if err != nil {
		log.Fatal(err)
	}

	matcher, err := createMatcherFromFlags()
	if err != nil {
		log.Fatal(err)
	}
	opts := scanner.ScannerOptions{
		Matcher:       matcher,
		BatchSize:     *batchSize,
		NumWorkers:    *numWorkers,
		ParallelFetch: *parallelFetch,
		StartIndex:    *startIndex,
		Quiet:         true,
	}
	scanner := scanner.NewScanner(logClient, opts)

	ctx := context.Background()
	scanner.Scan(ctx, logToKafka, logToKafka)
}
