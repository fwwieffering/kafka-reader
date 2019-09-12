# kafka-reader
A basic cli for reading messages from a kafka topic

## installation
clone the repository and run `go install` in the repo to compile the binary

## usage
```
$ kafka-reader -h
kafka-reader is a utility to print messages from a kafka topic to the console

Usage:
  kafka-reader [flags]

Flags:
  -b, --broker string            kafka broker address
  -c, --clientcert string        path to PEM encoded x509 client cert for TLS connections
  -k, --clientkey string         path to PEM encoded x509 client private key for TLS connections
  -d, --direction string         direction to read from. One of [forwards, backwards] (default "forwards")
  -f, --format string            Format to display kafka messages. One of [json, text] (default "text")
  -h, --help                     help for kafka-reader
  -n, --number-messages string   Integer number of messages to read from the topic.  (default "all")
  -t, --topic string             kafka topic name
```