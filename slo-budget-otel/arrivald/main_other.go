//go:build !linux

package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "arrivald needs Linux")
	os.Exit(1)
}
