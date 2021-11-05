package databroker

import (
	"fmt"
	"log"
	"os"
	"regexp"
	"runtime"

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

func FirehosePutRecord(data string) {
	log.Println("FirehosePutRecord Start")
	streamName := os.Getenv("FIREHOSE_DELIVERY_STREAM")
	region := os.Getenv("REGION")
	sess := session.Must(session.NewSession())

	// Create a Firehose client with additional configuration
	firehoseService := firehose.New(sess, aws.NewConfig().WithRegion(region))

	recordInput := &firehose.PutRecordInput{}
	recordInput = recordInput.SetDeliveryStreamName(streamName)



	// remove whitespaces occurring twice or more
	m1 := regexp.MustCompile(`\s{2,}`)
	processedData := m1.ReplaceAllString(data, "")

	dataBytes := []byte(processedData)
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
