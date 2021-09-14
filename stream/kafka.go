package stream

import (
	"bufio"
	"fmt"
	"io"
	"log"

	"github.com/d-ulyanov/kafka-sniffer/kafka"
	"github.com/d-ulyanov/kafka-sniffer/metrics"

	"github.com/google/gopacket"
	"github.com/google/gopacket/tcpassembly"
	"github.com/google/gopacket/tcpassembly/tcpreader"
)

// KafkaStreamFactory implements tcpassembly.StreamFactory
type KafkaStreamFactory struct {
	metricsStorage *metrics.Storage
	correlationMap *kafka.CorrelationMap
	brokerPort     string
	verbose        bool
}

// NewKafkaStreamFactory assembles streams
func NewKafkaStreamFactory(metricsStorage *metrics.Storage,
	correlationMap *kafka.CorrelationMap, port string, verbose bool) *KafkaStreamFactory {
	return &KafkaStreamFactory{
		metricsStorage: metricsStorage,
		correlationMap: correlationMap,
		brokerPort:     port,
		verbose:        verbose,
	}
}

// New assembles new stream
func (h *KafkaStreamFactory) New(net, transport gopacket.Flow) tcpassembly.Stream {
	s := &KafkaStream{
		net:            net,
		transport:      transport,
		r:              tcpreader.NewReaderStream(),
		metricsStorage: h.metricsStorage,
		correlationMap: h.correlationMap,
		brokerPort:     h.brokerPort,
		verbose:        h.verbose,
	}

	go s.run() // Important... we must guarantee that data from the reader stream is read.

	return &s.r
}

// KafkaStream will handle the actual decoding of http requests.
type KafkaStream struct {
	net, transport gopacket.Flow
	r              tcpreader.ReaderStream
	metricsStorage *metrics.Storage
	correlationMap *kafka.CorrelationMap
	brokerPort     string
	verbose        bool
}

func (h *KafkaStream) run() {
	srcHost := fmt.Sprint(h.net.Src())
	srcPort := fmt.Sprint(h.transport.Src())
	dstHost := fmt.Sprint(h.net.Dst())
	dstPort := fmt.Sprint(h.transport.Dst())

	log.Printf("%s:%s -> %s:%s", srcHost, srcPort, dstHost, dstPort)
	log.Printf("%s:%s -> %s:%s", dstHost, dstPort, srcHost, srcPort)

	buf := bufio.NewReaderSize(&h.r, 2<<15) // 65k

	// add new client ip to metric
	h.metricsStorage.AddActiveConnectionsTotal(h.net.Src().String())

	for {
		if h.transport.Src().String() != h.brokerPort {
			req, readBytes, err := kafka.DecodeRequest(buf)
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return
			}

			if err != nil {
				log.Printf("unable to read request to Broker - skipping packet: %s\n", err)

				if _, ok := err.(kafka.PacketDecodingError); ok {
					_, err := buf.Discard(readBytes)
					if err != nil {
						log.Printf("could not discard: %s\n", err)
					}
				}

				continue
			}

			if h.verbose {
				log.Printf("got request, key: %d, version: %d, correlationID: %d, clientID: %s\n",
					req.Key, req.Version, req.CorrelationID, req.ClientID)
			}

			req.Body.CollectClientMetrics(srcHost)

			switch body := req.Body.(type) {
			case *kafka.ProduceRequest:
				for _, topic := range body.ExtractTopics() {
					if h.verbose {
						log.Printf("client %s:%s wrote to topic %s", srcHost, srcPort, topic)
					}

					// add producer and topic relation info into metric
					h.metricsStorage.AddProducerTopicRelationInfo(h.net.Src().String(), topic)
				}
			case *kafka.FetchRequest:
				h.correlationMap.Add(req.CorrelationID, []int16{req.Key, req.Version})
				for _, topic := range body.ExtractTopics() {
					if h.verbose {
						log.Printf("client %s:%s read from topic %s", h.net.Src(), h.transport.Src(), topic)
					}

					// add consumer and topic relation info into metric
					h.metricsStorage.AddConsumerTopicRelationInfo(h.net.Src().String(), topic)
				}
			}
		} else {
			resp, readBytes, err := kafka.DecodeResponse(buf, h.correlationMap)
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return
			}

			if err != nil {
				log.Printf("unable to read response to Broker - skipping packet: %s\n", err)

				if _, ok := err.(kafka.PacketDecodingError); ok {
					_, err := buf.Discard(readBytes)
					if err != nil {
						log.Printf("could not discard: %s\n", err)
					}
				}

				continue
			}

			if h.verbose {
				log.Printf("got request, key: %d, version: %d, correlationID: %d, clientID: %s\n",
					resp.Key, resp.Version, resp.CorrelationID, resp.ClientID)
			}

			resp.Body.CollectClientMetrics(srcHost)
		}
	}
}
