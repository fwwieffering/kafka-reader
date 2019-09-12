package utils

import (
	"fmt"
)

// color definitions
const (
	InfoColor    = "\033[1;34m%s\033[0m"
	NoticeColor  = "\033[1;36m%s\033[0m"
	WarningColor = "\033[1;33m%s\033[0m"
	ErrorColor   = "\033[1;31m%s\033[0m"
	DebugColor   = "\033[0;36m%s\033[0m"
)

// Errorf prints red
func Errorf(format string, a ...interface{}) {
	formattedInput := fmt.Sprintf(format, a...)
	fmt.Printf(ErrorColor, formattedInput)
}

// Infof prints blue
func Infof(format string, a ...interface{}) {
	formattedInput := fmt.Sprintf(format, a...)
	fmt.Printf(InfoColor, formattedInput)
}
