package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	humanize "github.com/dustin/go-humanize"
)

var files = []string{
	"1b",
	"5kib",
	"5mib - 1b",
	"5mib",
	"5mib + 1b",
	"1gib",
	"5gib - 1b",
	"5gib",
	"5gib + 1b",
}

func parseSizeExpression(expr string) (uint64, error) {
	if strings.Contains(expr, "+") || strings.Contains(expr, "-") {
		parts := strings.Fields(expr)
		if len(parts) != 3 {
			return 0, fmt.Errorf("invalid size expression: %s", expr)
		}

		left, err := humanize.ParseBytes(parts[0])
		if err != nil {
			return 0, err
		}

		right, err := humanize.ParseBytes(parts[2])
		if err != nil {
			return 0, err
		}

		switch parts[1] {
		case "+":
			return left + right, nil
		case "-":
			return left - right, nil
		default:
			return 0, fmt.Errorf("invalid operator: %s", parts[1])
		}
	}
	return humanize.ParseBytes(expr)
}

func formatFilename(file string) string {
	replacer := strings.NewReplacer(
		" ", ".",
		"+", "plus",
		"-", "minus",
	)
	return replacer.Replace(file)
}

func main() {
	for _, file := range files {
		size, err := parseSizeExpression(file)
		if err != nil {
			log.Fatal(err)
		}

		f, err := os.Create(formatFilename(file) + ".raw")
		if err != nil {
			log.Fatal(err)
		}

		err = f.Truncate(int64(size))
		if err != nil {
			f.Close()
			log.Fatal(err)
		}

		err = f.Close()
		if err != nil {
			log.Fatal(err)
		}
	}
}
