package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	// RequestsCount is a prometheus metric. See info field
	RequestsCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "typed_requests_total",
		Help:      "Total requests to kafka by type",
	}, []string{"client_ip", "request_type"})

	// ProducerBatchLen is a prometheus metric. See info field
	ProducerBatchLen = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "producer_batch_length",
		Help:      "Length of producer request batch to kafka",
	}, []string{"client_ip"})

	// ProducerBatchSize is a prometheus metric. See info field
	ProducerBatchSize = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "producer_batch_size",
		Help:      "Total size of a batch in producer request to kafka",
	}, []string{"client_ip"})

	// BlocksRequested is a prometheus metric. See info field
	BlocksRequested = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "blocks_requested",
		Help:      "Total size of a batch in producer request to kafka",
	}, []string{"client_ip"})

	// ConsumerLag is a prometheus metric. See info field
	ConsumerLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "consumer_lag_seconds",
		Help:      "The difference between when the message was sent to the broker and when it was received from the broker in seconds",
	}, []string{"consumer_ip", "topic", "partition"})
)

func init() {
	prometheus.MustRegister(RequestsCount, ProducerBatchLen, ProducerBatchSize, BlocksRequested, ConsumerLag)
}

// ClientMetricsCollector is an interface, which allows to collect metrics for concrete client
type ClientMetricsCollector interface {
	CollectClientMetrics(srcHost string)
}
