package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "kafka-reader",
	Short: "kafka-reader is a utility to print messages from a kafka topic to the console",
	Long:  "kafka-reader is a utility to print messages from a kafka topic to the console",
	Run:   consumeKafka,
}

// Execute is called by main.main and is the entrypoint for all commands
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringP("topic", "t", "", "kafka topic name")
	rootCmd.PersistentFlags().StringP("broker", "b", "", "kafka broker address")
	rootCmd.PersistentFlags().StringP("clientcert", "c", "", "path to PEM encoded x509 client cert for TLS connections")
	rootCmd.PersistentFlags().StringP("clientkey", "k", "", "path to PEM encoded x509 client private key for TLS connections")
	rootCmd.PersistentFlags().StringP("number-messages", "n", "all", "Integer number of messages to read from the topic. ")
	rootCmd.PersistentFlags().StringP("direction", "d", "forwards", "direction to read from. One of [forwards, backwards]")
	rootCmd.PersistentFlags().StringP("format", "f", "text", "Format to display kafka messages. One of [json, text]")
}
