package common

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"os"
	"time"
)

// This simulates Mongo's object ID function
// which utilizes a 4-byte encoded timestamp value in the beginning of the id
// Used to even further reduce any chance of collision across distributed containers
func GenerateObjectId() (string, error) {
	var objectId [12]byte

	// 4-byte timestamp
	binary.BigEndian.PutUint32(objectId[:4], uint32(time.Now().Unix()))

	// 3-byte machine identifier (simplified here as random bytes)
	if _, err := rand.Read(objectId[4:7]); err != nil {
		return "", err // rand should never fail
	}

	// 2-byte process id
	binary.BigEndian.PutUint16(objectId[7:9], uint16(os.Getpid()))

	// 3-byte counter (simplified here as random bytes)
	if _, err := rand.Read(objectId[9:]); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", objectId), nil
}
