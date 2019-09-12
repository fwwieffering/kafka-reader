package cmd

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"kafka-reader/utils"

	"github.com/spf13/cobra"
)

// not used but if more commands come can use this to register command with root
var consume = &cobra.Command{
	Use:   "consume",
	Short: "consume",
	Long:  "consumes from a given kafka topic/broker",
	Run:   consumeKafka,
}

// getCerts attempts to load the certs from the filesystem
// if it encounters an error it calls os.Exit(1)
func getCerts(cmd *cobra.Command) ([]byte, []byte) {
	// check for flag definitions
	var certPath = cmd.Flag("clientcert").Value.String()
	var keyPath = cmd.Flag("clientkey").Value.String()

	if len(certPath) == 0 && len(keyPath) == 0 {
		return nil, nil
	}

	// must have both certPath and keyPath
	if (len(certPath) > 0 && len(keyPath) == 0) ||
		(len(certPath) == 0 && len(keyPath) > 0) {
		utils.Errorf("Error:\nBoth clientcert and clientkey must be provided if one is provided\n")
		cmd.Usage()
		os.Exit(1)
	}

	// read cert content from filesystem
	var certBlock []byte
	var keyBlock []byte

	var certs = []struct {
		path string
		dest *[]byte
	}{
		{
			path: certPath,
			dest: &certBlock,
		},
		{
			path: keyPath,
			dest: &keyBlock,
		},
	}

	for _, certInfo := range certs {
		if _, err := os.Stat(certInfo.path); err != nil {
			if os.IsNotExist(err) {
				utils.Errorf("Error:\nFile %s does not exist\n", certInfo.path)

			} else {
				utils.Errorf("Error:\nUnable to stat file %s: %s\n", certInfo.path, err.Error())
			}
			os.Exit(1)
		}
		content, err := ioutil.ReadFile(certInfo.path)
		if err != nil {
			utils.Errorf("Error:\nUnable to open file %s: %s", certInfo.path, err.Error())
			os.Exit(1)
		}
		*certInfo.dest = content
	}
	return certBlock, keyBlock
}

func getKafkaReader(topic string, broker string, cmd *cobra.Command) *utils.KafkaConnection {
	cert, key := getCerts(cmd)
	numberMessagesString := cmd.Flag("number-messages").Value.String()
	var numberMessages int
	// "all" is default
	if numberMessagesString == "all" {
		// -1 will cause the consumer to read all messages
		numberMessages = -1
	} else {
		tmp, err := strconv.Atoi(numberMessagesString)
		numberMessages = tmp
		if err != nil {
			utils.Errorf("Error:\nnumber-messages must be an integer\n")
			cmd.Usage()
			os.Exit(1)
		}
	}
	format := cmd.Flag("format").Value.String()
	reader, err := utils.GetKafkaConn(topic, broker, cert, key, cmd.Flag("direction").Value.String(), numberMessages, format)
	if err != nil {
		utils.Errorf("Error:\n%s\n", err.Error())
		os.Exit(1)
	}
	return reader
}

// reads messages from the given kafka topic
func consumeKafka(cmd *cobra.Command, args []string) {
	// check for required flags
	var topic string
	var broker string

	var requiredFlags = []struct {
		name string
		dest *string
	}{
		{
			name: "topic",
			dest: &topic,
		},
		{
			name: "broker",
			dest: &broker,
		},
	}

	errs := make([]string, 0)
	for _, f := range requiredFlags {
		v := cmd.Flag(f.name)
		if v == nil || len(v.Value.String()) == 0 {
			errs = append(errs, fmt.Sprintf("%s is a required flag\n", f.name))
		}
		tmp := v.Value.String()
		*f.dest = tmp
	}
	if len(errs) > 0 {
		utils.Errorf("Error: \n%s", strings.Join(errs, ""))
		cmd.Usage()
		os.Exit(1)
	}

	// set up kafka reader
	k := getKafkaReader(topic, broker, cmd)
	// create cancelable context
	ctx, cancel := context.WithCancel(context.Background())
	// listen for signals
	signalChan := make(chan os.Signal)
	doneChan := make(chan bool)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL)
	go func() {
		// received signal
		<-signalChan
		// cancel context
		cancel()
		doneChan <- true
	}()

	// print any format info needed
	k.FormatStart()
	var firstMessage = true
	// read until canceled
	for {
		select {
		case <-doneChan:
			k.FormatEnd()
			k.PrintStats()
			os.Exit(0)
		default:
			msg, err := k.ReadMessage(ctx)
			if err != nil {
				if err.Error() == "context canceled" || utils.IsStopProcessingError(err) {
					k.FormatEnd()
					if k.Format == "text" {
						utils.Errorf("%s\n", err.Error())
						fmt.Printf("Stopping...\n")
						k.PrintStats()
					}
					os.Exit(0)
				}
				utils.Errorf("Error reading from %s: %s\n", topic, err.Error())
			} else {
				if !firstMessage {
					k.FormatBetweenMessages()
				}
				k.DisplayMessage(msg)
			}
			firstMessage = false
		}
	}
}
