package databroker

import (
	"fmt"
	"log"
	"os"
	"regexp"
	"runtime"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
)


func getCallingFunction(skip int) string {
	pc, _, _, ok := runtime.Caller(skip)
	details := runtime.FuncForPC(pc)
	if ok && details != nil {
		return details.Name()
	}

	return ""
}

func FirehosePutRecord(data) {
	log.Println("FirehosePutRecord Start")
	streamName := os.Getenv("FIREHOSE_DELIVERY_STREAM")
	region := os.Getenv("REGION")
	sess := session.Must(session.NewSession())

	// Create a Firehose client with additional configuration
	firehoseService := firehose.New(sess, aws.NewConfig().WithRegion(region))

	recordInput := &firehose.PutRecordInput{}
	recordInput = recordInput.SetDeliveryStreamName(streamName)
	timestamp := time.Now().Format("2006-01-02 15:04:05.999999")
	caller := getCallingFunction(2)


	// remove whitespaces occurring twice or more
	m1 := regexp.MustCompile(`\s{2,}`)
	data = m1.ReplaceAllString(data, "")

	dataBytes := []byte(data)
	dataBytes = append(dataBytes, uint8('\n'))  // add new line
	record := &firehose.Record{Data: dataBytes}
	recordInput = recordInput.SetRecord(record)

	resp, err := firehoseService.PutRecord(recordInput)
	if err != nil {
		fmt.Printf("PutRecord err: %v\n", err)
	} else {
		fmt.Printf("PutRecord: %v\n", resp)
	}

	log.Println("FirehosePutRecord End")
}
