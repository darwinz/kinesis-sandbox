package main

import (
	"bytes"
	"fmt"
	"github.com/amazon-ion/ion-go/ion"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/joho/godotenv"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

type ActionRecord struct {
	UserId      string    `ion:"UserId"`
	Action      string    `ion:"Action"`
	RuleVersion string    `ion:"RuleVersion"`
	Points      float64   `ion:"Points"`
	Created     time.Time `ion:"Created"`
	Hash        int       `ion:"Hash"`
	Data        string    `ion:"Data"`
	Date        time.Time `ion:"Date"`
}

// Initiate configuration
func init() {
	e := godotenv.Load() // Load .env file
	if e != nil {
		fmt.Print(e)
	}
	consumer = AWSKinesis{
		stream:          os.Getenv("KINESIS_STREAM_NAME"),
		region:          os.Getenv("KINESIS_REGION"),
		endpoint:        os.Getenv("AWS_ENDPOINT"),
		accessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		secretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		sessionToken:    os.Getenv("AWS_SESSION_TOKEN"),
	}
}

func main() {
	sess := session.New(&aws.Config{
		Region:      aws.String(consumer.region),
		Endpoint:    aws.String(consumer.endpoint),
		Credentials: credentials.NewStaticCredentials(consumer.accessKeyID, consumer.secretAccessKey, consumer.sessionToken),
	})

	// Create a Kinesis client with the session
	svc := kinesis.New(sess)
	var a *string

	// Get shard iterator
	shardIteratorInput := &kinesis.GetShardIteratorInput{
		ShardId:           aws.String("shardId-000000000000"),
		ShardIteratorType: aws.String("TRIM_HORIZON"),
		StreamName:        aws.String("QLDBActions"),
		//Timestamp:         aws.Time(timestamp),
	}
	shardIteratorOutput, err := svc.GetShardIterator(shardIteratorInput)
	if err != nil {
		log.Fatalf("Error getting shard iterator: %v", err)
	}

	shardIterator := shardIteratorOutput.ShardIterator

	for {
		recordsInput := &kinesis.GetRecordsInput{
			ShardIterator: shardIterator,
			Limit:         aws.Int64(1000),
		}

		recordsOutput, err := svc.GetRecords(recordsInput)
		if err != nil {
			log.Fatalf("Error getting records: %v", err)
		}

		if len(recordsOutput.Records) > 0 {
			for _, record := range recordsOutput.Records {
				reader := ion.NewReader(bytes.NewReader(record.Data))

				// Assuming the top-level Ion structure is a struct
				for reader.Next() {
					if reader.Type() == ion.StructType {
						reader.StepIn()

						for reader.Next() {
							fieldName, err := reader.FieldName()
							if err != nil {
								fmt.Printf("Error reading field name: %v\n", err)
								continue
							}

							switch reader.Type() {
							case ion.BoolType:
								val, err := reader.BoolValue()
								if err == nil {
									fmt.Printf("Field: %v, Bool Value: %v\n", fieldName, val)
								}
							case ion.IntType:
								val, err := reader.Int64Value()
								if err == nil {
									fmt.Printf("Field: %v, Int Value: %v\n", fieldName, val)
								}
							case ion.FloatType:
								val, err := reader.FloatValue()
								if err == nil {
									fmt.Printf("Field: %v, Float Value: %v\n", fieldName, val)
								}
							case ion.StringType:
								val, err := reader.StringValue()
								if err == nil {
									fmt.Printf("Field: %v, String Value: %v\n", fieldName, val)
								}
							case ion.TimestampType:
								val, err := reader.TimestampValue()
								if err == nil {
									fmt.Printf("Field: %v, Timestamp Value: %v\n", fieldName, val)
								}
							case ion.StructType:
								symbolToken, err := reader.FieldName()
								if err != nil {
									fmt.Printf("Error reading field name: %v\n", err)
									continue
								}

								if symbolToken != nil && symbolToken.Text != nil && *symbolToken.Text == "payload" {
									m := ActionRecord{}
									err := ion.Unmarshal(record.Data, &m)
									if err != nil {
										fmt.Printf("Error: %v", err)
									}
								}
							default:
								symbolToken, err := reader.FieldName()
								if err != nil {
									fmt.Printf("Error reading field name: %v\n", err)
									continue
								}
								if symbolToken != nil && symbolToken.Text != nil && *symbolToken.Text == "payload" {

								}

								fmt.Printf("Field: %v, Unknown Type: %v\n", fieldName, reader.Type())
							}
						}

						reader.StepOut()
					}
				}

				if err := reader.Err(); err != nil {
					fmt.Printf("Error reading Ion data: %v", err)
				}
			}
		} else if recordsOutput.NextShardIterator == a || shardIterator == recordsOutput.NextShardIterator || err != nil {
			log.Printf("GetRecords ERROR: %v\n", err)
			break
		}

		if recordsOutput.MillisBehindLatest != nil && *recordsOutput.MillisBehindLatest == 0 {
			break
		}

		shardIterator = recordsOutput.NextShardIterator

		// If no records were returned, wait for a second before the next request
		if len(recordsOutput.Records) == 0 {
			time.Sleep(1 * time.Second)
		}
	}
}
