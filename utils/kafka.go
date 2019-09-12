package utils

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaConnection holds the kafka.Reader and kafka.Conn structs in order to do
// reading and get statistics
type KafkaConnection struct {
	numberMessages int
	readMessages   int
	currentReader  int
	readers        []*kafkaReader
	readerStats    []kafka.ReaderStats
	lastOffset     int64
	Format         string
}

type kafkaReader struct {
	conn       *kafka.Conn
	reader     *kafka.Reader
	curOffset  int64
	lastOffset int64
}

// PrintStats formats reader stats and prints them to stdout
func (k KafkaConnection) PrintStats() {
	if k.Format != "json" {
		Infof("Total messages: %d\n", k.readMessages)
		Infof("Partition specific stats: \n")
		for _, stats := range k.readerStats {
			Infof("    Topic: %s\n", stats.Topic)
			Infof("    Partition: %s\n", stats.Partition)
			Infof("    Messages Read: %d\n", stats.Messages)
		}
	}
}

// getReader returns a reader index to use for the next operation
// chooses the reader with the lowest current offset
func (k *KafkaConnection) getReader() (int, error) {
	if len(k.readers) > 0 {
		var smallestOffset = k.readers[0].curOffset
		var index = 0
	
		for idx, r := range k.readers {
			if r.curOffset < smallestOffset {
				index = idx
				smallestOffset = r.curOffset
			}
		}
		return index, nil
	}
	return 0, StopProcessingError{msg: "No more messages"}
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

// ReadMessage chooses an active partition and reads a message from it.
// should return messages in increasing offset order
func (k *KafkaConnection) ReadMessage(ctx context.Context) (*kafka.Message, error) {
	reader, err := k.getReader()
	if err != nil {
		return nil, err
	}
	// check if we've read the requested number of messages
	if k.numberMessages != -1 && k.readMessages >= k.numberMessages {
		return nil, StopProcessingError{msg: fmt.Sprintf("Read %d messages", k.numberMessages)}
	}
	k.readMessages++

	msg, err := k.readers[reader].readMessage(ctx)

	// remove reader from k.readers if its done
	if err != nil && IsStopProcessingError(err) {
		k.readerStats = append(k.readerStats, k.readers[reader].reader.Stats())
		k.readers = append(k.readers[:reader], k.readers[reader+1:]...)
		return msg, nil
	}

	return msg, err
}

// readMessage reads a message from the kafka topic/partition. If the reader is already at the latest offset
// it returns an error
func (k *kafkaReader) readMessage(ctx context.Context) (*kafka.Message, error) {
	// set lastOffset if it isn't set
	if k.lastOffset == 0 {
		latestOffset, err := k.conn.ReadLastOffset()
		if err != nil {
			return nil, fmt.Errorf("Error getting offset %s", err.Error())
		}
		k.lastOffset = latestOffset
	}

	// check if the reader is at the end of the topic
	if k.curOffset >= k.lastOffset {
		return nil, StopProcessingError{msg: fmt.Sprintf("reader for partition %d is done", k.reader.Config().Partition)}
	}

	// get message
	msg, err := k.reader.ReadMessage(ctx)

	k.curOffset = k.reader.Offset()

	return &msg, err
}

// GetKafkaConn returns a kafka reader
func GetKafkaConn(topic string, broker string, clientCert, clientKey []byte, partition string, numberMessages int, format string) (*KafkaConnection, error) {
	// set up dialer with tls if provided
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
	var partitions []kafka.Partition
	// check if partition was specified. If not, discover partitions.
	if len(partition) > 0 {
		tmp, err := strconv.Atoi(partition)
		if err != nil {
			return nil, fmt.Errorf("partition must be an integer")
		}
		partitions = []kafka.Partition{{ID: tmp}}
	} else {
		// need the kafka.Conn struct to collect info about topic
		conn, err := dialer.DialLeader(context.Background(), "tcp", broker, topic, 0)
		if err != nil {
			return nil, err
		}
		// make kafkaReader per partition
		tmp, err := conn.ReadPartitions()
		if err != nil {
			return nil, fmt.Errorf("Unable to read partitions. Error %s", err.Error())
		}
		partitions = tmp
	}

	readers := make([]*kafkaReader, len(partitions))

	for idx, p := range partitions {
		// need the kafka.Conn struct to collect info about topic
		conn, err := dialer.DialLeader(context.Background(), "tcp", broker, topic, p.ID)
		if err != nil {
			return nil, err
		}
		// initialize a reader and set the offset to the first offset
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   []string{broker},
			Topic:     topic,
			Dialer:    dialer,
			Partition: p.ID,
		})
		firstOffset, _ := conn.ReadFirstOffset()
		reader.SetOffset(firstOffset)
		// init kafkaReader
		readers[idx] = &kafkaReader{
			reader: reader,
			conn:   conn,
			curOffset: firstOffset,
		}
	}

	return &KafkaConnection{
		numberMessages: numberMessages,
		readers:        readers,
		Format:         format,
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
	fmt.Printf("Partition: %d\n", msg.Partition)
	fmt.Printf("Offset: %d\n", msg.Offset)
	fmt.Printf("Timestamp: %s\n", msg.Time.Format(time.RFC3339))
	fmt.Printf("Content: %s\n", string(msg.Value))
	fmt.Printf("\n")
}

type kafkaMessageJSON struct {
	Partition int         `json:"partition"`
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
			Partition: msg.Partition,
			Offset:    msg.Offset,
			Timestamp: msg.Time,
			Key:       string(msg.Key),
			Value:     value,
		})
	} else {
		jsonbytes, _ = json.Marshal(kafkaMessageJSON{
			Partition: msg.Partition,
			Offset:    msg.Offset,
			Timestamp: msg.Time,
			Key:       string(msg.Key),
			Value:     string(msg.Value),
		})
	}
	fmt.Printf("\t%s", string(jsonbytes))
}
