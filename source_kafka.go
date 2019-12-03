package job

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/ywengineer/g-util/util"
	"go.uber.org/zap"
	logf "log"
	"os"
	"strings"
)

func init() {
	RegisterSource("kafka", func(conf *SourceConf) Source {
		return &KafkaSource{}
	})
}

func newKafkaConsumer(ctx context.Context, log *zap.Logger,
	brokers, topics, group string,
	verbose, oldest bool,
	version string) *kConsumer {
	if len(brokers) == 0 || len(topics) == 0 {
		log.Panic("missing kafka brokers or topics, consumer will not be disabled.")
		return nil
	}
	if verbose {
		sarama.Logger = logf.New(os.Stdout, "[Sarama] ", logf.LstdFlags)
	}

	ver, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		log.Panic("Error parsing Kafka version", zap.Error(err))
	}

	/**
	 * Construct a new Sarama configuration.
	 * The Kafka cluster version has to be defined before the consumer/producer is initialized.
	 */
	config := sarama.NewConfig()
	config.Version = ver

	if oldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	/**
	 * Setup a new Sarama consumer group
	 */
	consumer := kConsumer{
		ready: make(chan bool),
		stop:  make(chan bool),
		log:   log,
	}

	//ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), group, config)
	if err != nil {
		log.Panic("Error creating consumer group client", zap.Error(err))
	}
	//
	topicArray := strings.Split(topics, ",")
	//
	consumer.mc = make(chan *TaskData, len(topicArray))
	//
	go func() {
		defer func() {
			if err := client.Close(); err != nil {
				log.Panic("Error closing kafka consumer", zap.Error(err))
			}
			// 没有更多的消息需要处理
			close(consumer.mc)
		}()
		for {
			if err := client.Consume(ctx, topicArray, &consumer); err != nil {
				log.Error("Error from consumer", zap.Error(err))
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()
	return &consumer
}

// Consumer represents a Sarama consumer group consumer
type kConsumer struct {
	ready chan bool
	stop  chan bool
	mc    chan *TaskData
	log   *zap.Logger
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *kConsumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *kConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *kConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	// KAFKA_TOPIC_CURRENCY,KAFKA_TOPIC_ORDER,KAFKA_TOPIC_PLAYER_CREATE,KAFKA_TOPIC_PLAYER_SIGN,KAFKA_TOPIC_PLAYER_UPGRADE
	for message := range claim.Messages() {
		//BlockTimestamp time.Time       // only set if kafka is version 0.10+, outer (compressed) block timestamp
		dt := &TaskData{Payload: message.Value}
		dt.Metadata = map[string]interface{}{
			"timestamp": message.Timestamp, // only set if kafka is version 0.10+, inner message timestamp
			"topic":     message.Topic,
			"partition": message.Partition,
			"offset":    message.Offset,
			"key":       string(message.Key),
		}
		//
		consumer.mc <- dt
		//
		consumer.afterConsume(session, message)
	}
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *kConsumer) afterConsume(session sarama.ConsumerGroupSession, message *sarama.ConsumerMessage) {
	session.MarkMessage(message, "")
	consumer.log.Debug("consumed kafka message", zap.String("tag", "KafkaMessage"), zap.String("data", string(message.Value)))
}

type KafkaSource struct {
	consumer *kConsumer
	conf     *SourceConf
	log      *zap.Logger
}

func (kafka *KafkaSource) Init(conf *SourceConf, ctx context.Context, log *zap.Logger) {
	kafka.log = log
	kafka.conf = conf
	kafka.consumer = newKafkaConsumer(ctx, log,
		conf.GetString("brokers"),
		conf.GetString("topics"),
		conf.GetString("group"),
		conf.GetBool("verbose"),
		conf.GetBool("oldest"),
		conf.GetString("version"),
	)
	util.Watch(ctx, kafka.consumer.stop)
}

func (kafka *KafkaSource) Read() <-chan *TaskData {
	return kafka.consumer.mc
}

func (kafka *KafkaSource) Terminated() <-chan bool {
	return kafka.consumer.stop
}
