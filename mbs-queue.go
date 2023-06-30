package mbs_queue

import (
	rmqClient "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
	"os"
)

func NewProducer(options ...ProducerOption) (*Producer, error) {
	producerOptions := ProducerOptions{}
	for _, option := range options {
		option(&producerOptions)
	}

	if producerOptions.ConsoleEnabled {
		os.Setenv("mq.consoleAppender.enabled", "true")
		rmqClient.ResetLogger()
	}

	producer, err := rmqClient.NewProducer(
		&rmqClient.Config{
			Endpoint:      producerOptions.Endpoint,
			ConsumerGroup: producerOptions.GroupName,
			Credentials: &credentials.SessionCredentials{
				AccessKey:    producerOptions.AccessKey,
				AccessSecret: producerOptions.SecretKey,
			},
		},
		rmqClient.WithTopics(producerOptions.Topic...),
	)
	if err != nil {
		return nil, err
	}

	err = producer.Start()
	if err != nil {
		return nil, err
	}

	pd := &Producer{
		Options:  producerOptions,
		Producer: producer,
	}

	return pd, nil
}
