package utils

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"time"
)

func Identity() (string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("unable to get hostname: %w", err)
	}
	h := sha256.Sum256([]byte(hostname))
	hnHex := hex.EncodeToString(h[:])
	return fmt.Sprintf("%s-%d", hnHex[:16], time.Now().Unix()), nil
}
