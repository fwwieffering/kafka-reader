package utils

import (
	"fmt"
	"strconv"

	"github.com/spf13/cobra"
)

// GetIntFlag returns the int value of a string field
func GetIntFlag(cmd *cobra.Command, flagname string) (int, error) {
	strVal := cmd.Flag(flagname).Value.String()
	intVal, err := strconv.Atoi(strVal)
	if err != nil {
		return 0, fmt.Errorf("Error:\n%s must be an integer. Was: %s\n", flagname, strVal)
	}
	return intVal, nil
}
