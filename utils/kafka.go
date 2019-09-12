package utils

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaConnection holds the kafka.Reader and kafka.Conn structs in order to do
// reading and get statistics
type KafkaConnection struct {
	direction      string
	numberMessages int
	readMessages   int
	reader         *kafka.Reader
	connection     *kafka.Conn
	lastOffset     int64
	Format         string
}

// PrintStats formats reader stats and prints them to stdout
func (k KafkaConnection) PrintStats() {
	if k.Format != "json" {
		stats := k.reader.Stats()
		Infof("Topic: %s\n", stats.Topic)
		Infof("Partition: %s\n", stats.Partition)
		Infof("Messages Read: %d\n", stats.Messages)
		Infof("Errors: %d\n", stats.Errors)
	}
}

// FormatStart prints the format needed prior to starting reading messages
// e.g. for json prints open bracket
func (k KafkaConnection) FormatStart() {
	switch k.Format {
	case "json":
		fmt.Printf("[\n")
	}
}

// FormatEnd prints the format needed after reading messages
// e.g. for json prints close bracket
func (k KafkaConnection) FormatEnd() {
	switch k.Format {
	case "json":
		fmt.Printf("\n]\n")
	}
}

// FormatBetweenMessages prints the format needed between two messages
// e.g. for json prints comma
func (k KafkaConnection) FormatBetweenMessages() {
	switch k.Format {
	case "json":
		fmt.Printf(",\n")
	}

}

// StopProcessingError error interface for stopping the kafka consuming
type StopProcessingError struct {
	msg string
}

func (s StopProcessingError) Error() string {
	return s.msg
}

// IsStopProcessingError returns true if the error should stop processing
func IsStopProcessingError(err error) bool {
	_, ok := err.(StopProcessingError)
	return ok
}

// ReadMessage reads a message from the kafka topic. If the reader is already at the latest offset
// it returns an error
func (k *KafkaConnection) ReadMessage(ctx context.Context) (kafka.Message, error) {
	// set lastOffset if it isn't set
	if k.lastOffset == 0 {
		latestOffset, err := k.connection.ReadLastOffset()
		if err != nil {
			return kafka.Message{}, fmt.Errorf("Error getting offset %s", err.Error())
		}
		k.lastOffset = latestOffset
	}
	// check if we've read the requested number of messages
	if k.numberMessages != -1 && k.readMessages >= k.numberMessages {
		return kafka.Message{}, StopProcessingError{msg: fmt.Sprintf("Read %d messages", k.numberMessages)}
	}
	// read backwards if we should
	if k.direction == "backwards" {
		return k.readMessageBackwards(ctx)
	}
	// check if the reader is at the end of the topic
	readerOffset := k.reader.Offset()
	if readerOffset == k.lastOffset {
		return kafka.Message{}, StopProcessingError{msg: fmt.Sprintf("No more messages")}
	}
	k.readMessages++

	return k.reader.ReadMessage(ctx)
}

// this is super slow for some reason
func (k *KafkaConnection) readMessageBackwards(ctx context.Context) (kafka.Message, error) {
	// if this is our first message set it to the end
	if k.readMessages == 0 {
		k.reader.SetOffset(k.lastOffset - 1)
	} else {
		// assuming that each offset corresponds to 1 message, set it back 2 so it reads the one before the one it read last
		k.reader.SetOffset(k.reader.Offset() - 2)
	}
	k.readMessages++
	return k.reader.ReadMessage(ctx)
}

// GetKafkaConn returns a kafka reader
func GetKafkaConn(topic string, broker string, clientCert, clientKey []byte, direction string, numberMessages int, format string) (*KafkaConnection, error) {
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	if clientCert != nil && clientKey != nil {
		t, err := NewTLSConfig(clientCert, clientKey)
		if err != nil {
			return nil, fmt.Errorf("Unable to make tls config with provided certs: %s", err.Error())
		}
		dialer.TLS = t
	}
	conn, err := dialer.DialLeader(context.Background(), "tcp", broker, topic, 0)
	if err != nil {
		return nil, err
	}

	return &KafkaConnection{
		numberMessages: numberMessages,
		direction:      direction,
		reader: kafka.NewReader(kafka.ReaderConfig{
			Dialer:  dialer,
			Brokers: []string{broker},
			Topic:   topic,
		}),
		connection: conn,
		Format:     format,
	}, nil
}

// NewTLSConfig takes in the client cert, client key, and ca cert file to
// produce a tls configuation that can be used in other pieces of code
func NewTLSConfig(clientCertPEMBlock, clientKeyPEMBlock []byte) (*tls.Config, error) {
	// Load client cert
	cert, err := tls.X509KeyPair(clientCertPEMBlock, clientKeyPEMBlock)
	if err != nil {
		return nil, err
	}

	tlsConfig := tls.Config{
		// make this work more often by not verifying ssl
		InsecureSkipVerify: true,
		Certificates:       []tls.Certificate{cert},
	}

	tlsConfig.BuildNameToCertificate()

	return &tlsConfig, err
}

// DisplayMessage displays the kafka message in the format specified by the format property
func (k KafkaConnection) DisplayMessage(msg kafka.Message) {
	if strings.ToLower(k.Format) == "json" {
		DisplayMessageJSON(msg)
	} else {
		DisplayMessageText(msg)
	}
}

// DisplayMessageText formats a kafka message as text and prints to stdout
func DisplayMessageText(msg kafka.Message) {
	fmt.Printf("Key: %s\n", string(msg.Key))
	fmt.Printf("Offset: %d\n", msg.Offset)
	fmt.Printf("Timestamp: %s\n", msg.Time.Format(time.RFC3339))
	fmt.Printf("Content: %s\n", string(msg.Value))
	fmt.Printf("\n")
}

type kafkaMessageJSON struct {
	Offset    int64       `json:"offset"`
	Timestamp time.Time   `json:"timestamp"`
	Key       string      `json:"key"`
	Value     interface{} `json:"value"`
}

// DisplayMessageJSON formats a kafka message as json and prints it to stdout
func DisplayMessageJSON(msg kafka.Message) {
	// attempt to unmarshal value, which may be json
	var value interface{}
	err := json.Unmarshal(msg.Value, &value)
	var jsonbytes []byte
	if err == nil {
		jsonbytes, err = json.Marshal(kafkaMessageJSON{
			Offset:    msg.Offset,
			Timestamp: msg.Time,
			Key:       string(msg.Key),
			Value:     value,
		})
	} else {
		jsonbytes, _ = json.Marshal(kafkaMessageJSON{
			Offset:    msg.Offset,
			Timestamp: msg.Time,
			Key:       string(msg.Key),
			Value:     string(msg.Value),
		})
	}
	fmt.Printf("\t%s", string(jsonbytes))
}
