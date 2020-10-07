package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	kazoo "github.com/krallistic/kazoo-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	plog "github.com/prometheus/common/log"
	"github.com/prometheus/common/version"
	"github.com/rcrowley/go-metrics"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	namespace = "kafka"
	clientID  = "kafka_exporter"
)

var (
	clusterBrokers                     *prometheus.Desc
	topicPartitions                    *prometheus.Desc
	topicCurrentOffset                 *prometheus.Desc
	topicOldestOffset                  *prometheus.Desc
	topicPartitionLeader               *prometheus.Desc
	topicPartitionReplicas             *prometheus.Desc
	topicPartitionInSyncReplicas       *prometheus.Desc
	topicPartitionUsesPreferredReplica *prometheus.Desc
	topicUnderReplicatedPartition      *prometheus.Desc
	consumergroupCurrentOffset         *prometheus.Desc
	consumergroupCurrentOffsetSum      *prometheus.Desc
	consumergroupLag                   *prometheus.Desc
	consumergroupLagSum                *prometheus.Desc
	consumergroupLagZookeeper          *prometheus.Desc
	consumergroupMembers               *prometheus.Desc
)

// Exporter collects Kafka stats from the given server and exports them using
// the prometheus metrics package.
type Exporter struct {
	client                  sarama.Client
	topicFilter             *regexp.Regexp
	groupFilter             *regexp.Regexp
	mu                      sync.Mutex
	useZooKeeperLag         bool
	zookeeperClient         *kazoo.Kazoo
	nextMetadataRefresh     time.Time
	metadataRefreshInterval time.Duration
}

type kafkaOpts struct {
	uri                      []string
	useSASL                  bool
	useSASLHandshake         bool
	saslUsername             string
	saslPassword             string
	saslMechanism            string
	useTLS                   bool
	tlsCAFile                string
	tlsCertFile              string
	tlsKeyFile               string
	tlsInsecureSkipTLSVerify bool
	kafkaVersion             string
	useZooKeeperLag          bool
	uriZookeeper             []string
	labels                   string
	metadataRefreshInterval  string
}

// CanReadCertAndKey returns true if the certificate and key files already exists,
// otherwise returns false. If lost one of cert and key, returns error.
func CanReadCertAndKey(certPath, keyPath string) (bool, error) {
	certReadable := canReadFile(certPath)
	keyReadable := canReadFile(keyPath)

	if certReadable == false && keyReadable == false {
		return false, nil
	}

	if certReadable == false {
		return false, fmt.Errorf("error reading %s, certificate and key must be supplied as a pair", certPath)
	}

	if keyReadable == false {
		return false, fmt.Errorf("error reading %s, certificate and key must be supplied as a pair", keyPath)
	}

	return true, nil
}

// If the file represented by path exists and
// readable, returns true otherwise returns false.
func canReadFile(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}

	defer f.Close()

	return true
}

// NewExporter returns an initialized Exporter.
func NewExporter(opts kafkaOpts, topicFilter string, groupFilter string) (*Exporter, error) {
	var zookeeperClient *kazoo.Kazoo
	config := sarama.NewConfig()
	config.ClientID = clientID
	kafkaVersion, err := sarama.ParseKafkaVersion(opts.kafkaVersion)
	if err != nil {
		return nil, err
	}
	config.Version = kafkaVersion

	if opts.useSASL {
		// Convert to lowercase so that SHA512 and SHA256 is still valid
		opts.saslMechanism = strings.ToLower(opts.saslMechanism)
		switch opts.saslMechanism {
		case "scram-sha512":
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
			config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA512)
		case "scram-sha256":
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
			config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA256)

		case "plain":
		default:
			plog.Fatalf("invalid sasl mechanism \"%s\": can only be \"scram-sha256\", \"scram-sha512\" or \"plain\"", opts.saslMechanism)
		}

		config.Net.SASL.Enable = true
		config.Net.SASL.Handshake = opts.useSASLHandshake

		if opts.saslUsername != "" {
			config.Net.SASL.User = opts.saslUsername
		}

		if opts.saslPassword != "" {
			config.Net.SASL.Password = opts.saslPassword
		}
	}

	if opts.useTLS {
		config.Net.TLS.Enable = true

		config.Net.TLS.Config = &tls.Config{
			RootCAs:            x509.NewCertPool(),
			InsecureSkipVerify: opts.tlsInsecureSkipTLSVerify,
		}

		if opts.tlsCAFile != "" {
			if ca, err := ioutil.ReadFile(opts.tlsCAFile); err == nil {
				config.Net.TLS.Config.RootCAs.AppendCertsFromPEM(ca)
			} else {
				plog.Fatalln(err)
			}
		}

		canReadCertAndKey, err := CanReadCertAndKey(opts.tlsCertFile, opts.tlsKeyFile)
		if err != nil {
			plog.Fatalln(err)
		}
		if canReadCertAndKey {
			cert, err := tls.LoadX509KeyPair(opts.tlsCertFile, opts.tlsKeyFile)
			if err == nil {
				config.Net.TLS.Config.Certificates = []tls.Certificate{cert}
			} else {
				plog.Fatalln(err)
			}
		}
	}

	if opts.useZooKeeperLag {
		zookeeperClient, err = kazoo.NewKazoo(opts.uriZookeeper, nil)
	}

	interval, err := time.ParseDuration(opts.metadataRefreshInterval)
	if err != nil {
		plog.Errorln("Cannot parse metadata refresh interval")
		panic(err)
	}

	config.Metadata.RefreshFrequency = interval

	client, err := sarama.NewClient(opts.uri, config)

	if err != nil {
		plog.Errorln("Error Init Kafka Client")
		panic(err)
	}
	plog.Infoln("Done Init Clients")

	// Init our exporter.
	return &Exporter{
		client:                  client,
		topicFilter:             regexp.MustCompile(topicFilter),
		groupFilter:             regexp.MustCompile(groupFilter),
		useZooKeeperLag:         opts.useZooKeeperLag,
		zookeeperClient:         zookeeperClient,
		nextMetadataRefresh:     time.Now(),
		metadataRefreshInterval: interval,
	}, nil
}

// Describe describes all the metrics ever exported by the Kafka exporter. It
// implements prometheus.Collector.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- consumergroupCurrentOffset
	ch <- consumergroupCurrentOffsetSum
	ch <- consumergroupLag
	ch <- consumergroupLagZookeeper
	ch <- consumergroupLagSum
}

// Collect fetches the stats from configured Kafka location and delivers them
// as Prometheus metrics. It implements prometheus.Collector.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	var wg = sync.WaitGroup{}

	offset := make(map[string]map[int32]int64)

	now := time.Now()

	if now.After(e.nextMetadataRefresh) {
		plog.Info("Refreshing client metadata")

		if err := e.client.RefreshMetadata(); err != nil {
			plog.Errorf("Cannot refresh topics, using cached data: %v", err)
		}

		e.nextMetadataRefresh = now.Add(e.metadataRefreshInterval)
	}

	getConsumerGroupMetrics := func(broker *sarama.Broker) {
		defer wg.Done()
		if err := broker.Open(e.client.Config()); err != nil && err != sarama.ErrAlreadyConnected {
			plog.Errorf("Cannot connect to broker %d: %v", broker.ID(), err)
			return
		}
		defer broker.Close()

		groups, err := broker.ListGroups(&sarama.ListGroupsRequest{})
		if err != nil {
			plog.Errorf("Cannot get consumer group: %v", err)
			return
		}
		groupIds := make([]string, 0)
		for groupId := range groups.Groups {
			if e.groupFilter.MatchString(groupId) && groupId != "" {
				groupIds = append(groupIds, groupId)
			}
		}

		describeGroups, err := broker.DescribeGroups(&sarama.DescribeGroupsRequest{Groups: groupIds})
		if err != nil {
			plog.Errorf("Cannot get describe groups: %v", err)
			return
		}
		for _, group := range describeGroups.Groups {
			offsetFetchRequest := sarama.OffsetFetchRequest{ConsumerGroup: group.GroupId, Version: 1}
			for topic, partitions := range offset {
				for partition := range partitions {
					offsetFetchRequest.AddPartition(topic, partition)
				}
			}
			ch <- prometheus.MustNewConstMetric(
				consumergroupMembers, prometheus.GaugeValue, float64(len(group.Members)), group.GroupId,
			)
			if offsetFetchResponse, err := broker.FetchOffset(&offsetFetchRequest); err != nil {
				plog.Errorf("Cannot get offset of group %s: %v", group.GroupId, err)
			} else {
				for topic, partitions := range offsetFetchResponse.Blocks {
					// If the topic is not consumed by that consumer group, skip it
					topicConsumed := false
					for _, offsetFetchResponseBlock := range partitions {
						// Kafka will return -1 if there is no offset associated with a topic-partition under that consumer group
						if offsetFetchResponseBlock.Offset != -1 {
							topicConsumed = true
							break
						}
					}
					if topicConsumed {
						var currentOffsetSum int64
						var lagSum int64
						for partition, offsetFetchResponseBlock := range partitions {
							err := offsetFetchResponseBlock.Err
							if err != sarama.ErrNoError {
								plog.Errorf("Error for  partition %d :%v", partition, err.Error())
								continue
							}
							currentOffset := offsetFetchResponseBlock.Offset
							currentOffsetSum += currentOffset
							ch <- prometheus.MustNewConstMetric(
								consumergroupCurrentOffset, prometheus.GaugeValue, float64(currentOffset), group.GroupId, topic, strconv.FormatInt(int64(partition), 10),
							)
							e.mu.Lock()
							if offset, ok := offset[topic][partition]; ok {
								// If the topic is consumed by that consumer group, but no offset associated with the partition
								// forcing lag to -1 to be able to alert on that
								var lag int64
								if offsetFetchResponseBlock.Offset == -1 {
									lag = -1
								} else {
									lag = offset - offsetFetchResponseBlock.Offset
									lagSum += lag
								}
								ch <- prometheus.MustNewConstMetric(
									consumergroupLag, prometheus.GaugeValue, float64(lag), group.GroupId, topic, strconv.FormatInt(int64(partition), 10),
								)
							} else {
								plog.Errorf("No offset of topic %s partition %d, cannot get consumer group lag", topic, partition)
							}
							e.mu.Unlock()
						}
						ch <- prometheus.MustNewConstMetric(
							consumergroupCurrentOffsetSum, prometheus.GaugeValue, float64(currentOffsetSum), group.GroupId, topic,
						)
						ch <- prometheus.MustNewConstMetric(
							consumergroupLagSum, prometheus.GaugeValue, float64(lagSum), group.GroupId, topic,
						)
					}
				}
			}
		}
	}

	if len(e.client.Brokers()) > 0 {
		for _, broker := range e.client.Brokers() {
			wg.Add(1)
			go getConsumerGroupMetrics(broker)
		}
		wg.Wait()
	} else {
		plog.Errorln("No valid broker, cannot get consumer group metrics")
	}
}

func init() {
	metrics.UseNilMetrics = true
	prometheus.MustRegister(version.NewCollector("kafka_exporter"))
}

func main() {
	var (
		listenAddress = kingpin.Flag("web.listen-address", "Address to listen on for web interface and telemetry.").Default(":9308").String()
		metricsPath   = kingpin.Flag("web.telemetry-path", "Path under which to expose metrics.").Default("/metrics").String()
		topicFilter   = kingpin.Flag("topic.filter", "Regex that determines which topics to collect.").Default(".*").String()
		groupFilter   = kingpin.Flag("group.filter", "Regex that determines which consumer groups to collect.").Default(".*").String()
		logSarama     = kingpin.Flag("log.enable-sarama", "Turn on Sarama logging.").Default("false").Bool()

		opts = kafkaOpts{}
	)
	kingpin.Flag("kafka.server", "Address (host:port) of Kafka server.").Default("kafka:9092").StringsVar(&opts.uri)
	kingpin.Flag("sasl.enabled", "Connect using SASL/PLAIN.").Default("false").BoolVar(&opts.useSASL)
	kingpin.Flag("sasl.handshake", "Only set this to false if using a non-Kafka SASL proxy.").Default("true").BoolVar(&opts.useSASLHandshake)
	kingpin.Flag("sasl.username", "SASL user name.").Default("").StringVar(&opts.saslUsername)
	kingpin.Flag("sasl.password", "SASL user password.").Default("").StringVar(&opts.saslPassword)
	kingpin.Flag("sasl.mechanism", "The SASL SCRAM SHA algorithm sha256 or sha512 as mechanism").Default("plain").StringVar(&opts.saslMechanism)
	kingpin.Flag("tls.enabled", "Connect using TLS.").Default("false").BoolVar(&opts.useTLS)
	kingpin.Flag("tls.ca-file", "The optional certificate authority file for TLS client authentication.").Default("").StringVar(&opts.tlsCAFile)
	kingpin.Flag("tls.cert-file", "The optional certificate file for client authentication.").Default("").StringVar(&opts.tlsCertFile)
	kingpin.Flag("tls.key-file", "The optional key file for client authentication.").Default("").StringVar(&opts.tlsKeyFile)
	kingpin.Flag("tls.insecure-skip-tls-verify", "If true, the server's certificate will not be checked for validity. This will make your HTTPS connections insecure.").Default("false").BoolVar(&opts.tlsInsecureSkipTLSVerify)
	kingpin.Flag("kafka.version", "Kafka broker version").Default(sarama.V1_0_0_0.String()).StringVar(&opts.kafkaVersion)
	kingpin.Flag("use.consumelag.zookeeper", "if you need to use a group from zookeeper").Default("false").BoolVar(&opts.useZooKeeperLag)
	kingpin.Flag("zookeeper.server", "Address (hosts) of zookeeper server.").Default("localhost:2181").StringsVar(&opts.uriZookeeper)
	kingpin.Flag("kafka.labels", "Kafka cluster name").Default("").StringVar(&opts.labels)
	kingpin.Flag("refresh.metadata", "Metadata refresh interval").Default("5m").StringVar(&opts.metadataRefreshInterval)

	plog.AddFlags(kingpin.CommandLine)
	kingpin.Version(version.Print("kafka_exporter"))
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()

	plog.Infoln("Starting kafka_exporter", version.Info())
	plog.Infoln("Build context", version.BuildContext())

	labels := make(map[string]string)

	// Protect against empty labels
	if opts.labels != "" {
		for _, label := range strings.Split(opts.labels, ",") {
			splitted := strings.Split(label, "=")
			if len(splitted) >= 2 {
				labels[splitted[0]] = splitted[1]
			}
		}
	}

	consumergroupCurrentOffset = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroup", "current_offset"),
		"Current Offset of a ConsumerGroup at Topic/Partition",
		[]string{"consumergroup", "topic", "partition"}, labels,
	)

	consumergroupCurrentOffsetSum = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroup", "current_offset_sum"),
		"Current Offset of a ConsumerGroup at Topic for all partitions",
		[]string{"consumergroup", "topic"}, labels,
	)

	consumergroupLag = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroup", "lag"),
		"Current Approximate Lag of a ConsumerGroup at Topic/Partition",
		[]string{"consumergroup", "topic", "partition"}, labels,
	)

	consumergroupLagZookeeper = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroupzookeeper", "lag_zookeeper"),
		"Current Approximate Lag(zookeeper) of a ConsumerGroup at Topic/Partition",
		[]string{"consumergroup", "topic", "partition"}, nil,
	)

	consumergroupLagSum = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroup", "lag_sum"),
		"Current Approximate Lag of a ConsumerGroup at Topic for all partitions",
		[]string{"consumergroup", "topic"}, labels,
	)

	consumergroupMembers = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroup", "members"),
		"Amount of members in a consumer group",
		[]string{"consumergroup"}, labels,
	)

	if *logSarama {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	exporter, err := NewExporter(opts, *topicFilter, *groupFilter)
	if err != nil {
		plog.Fatalln(err)
	}
	defer exporter.client.Close()
	prometheus.MustRegister(exporter)

	http.Handle(*metricsPath, promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
	        <head><title>Kafka Exporter</title></head>
	        <body>
	        <h1>Kafka Exporter</h1>
	        <p><a href='` + *metricsPath + `'>Metrics</a></p>
	        </body>
	        </html>`))
	})

	plog.Infoln("Listening on", *listenAddress)
	plog.Fatal(http.ListenAndServe(*listenAddress, nil))
}
