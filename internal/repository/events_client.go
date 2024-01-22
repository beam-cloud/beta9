package repository

import (
	"encoding/json"
	"log"
	"net"
	"time"

	"github.com/beam-cloud/beam/internal/common"
	"github.com/beam-cloud/beam/internal/types"
)

func createEventObject(eventName string, data []byte) types.Event {
	return types.Event{
		Id:         common.GenerateObjectId(),
		Name:       eventName,
		Created:    time.Now().Unix(),
		ApiVersion: DataAPIVersion,
		Data:       data,
	}
}

func PushEvent(streamName, eventName string, data interface{}) error {
	conn, err := net.Dial("tcp", EventsTCPEndpoint+":"+EventsTCPPort+"/"+streamName)
	if err != nil {
		log.Println(err)
		return err
	}
	defer conn.Close()

	// serialize data to json
	dataBytes, err := json.Marshal(data)
	if err != nil {
		log.Println(err)
		return err
	}

	eventBytes, err := json.Marshal(
		createEventObject(eventName, dataBytes),
	)
	if err != nil {
		log.Println(err)
		return err
	}

	_, err = conn.Write(eventBytes)
	if err != nil {
		log.Println(err)
		return err
	}

	buffer := make([]byte, 1024)
	_, err = conn.Read(buffer)
	if err != nil {
		log.Println(err)
		return err
	}

	return nil
}
