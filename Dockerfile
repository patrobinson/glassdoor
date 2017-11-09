FROM golang:1.9.0
WORKDIR /go/src/github.com/patrobinson/glasshouse
COPY main.go .
RUN go get && CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo .

FROM alpine:latest  
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=0 /go/src/github.com/patrobinson/glasshouse/glasshouse .
ENTRYPOINT ["./glasshouse"]
