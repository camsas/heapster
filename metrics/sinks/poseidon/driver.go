package poseidon

import (
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"k8s.io/heapster/metrics/core"
	"k8s.io/heapster/metrics/sinks/poseidon/stats"
	"log"
	"net/url"
	"sync"
)

const (
	BYTE     = 1
	KILOBYTE = 1024 * BYTE
)

type PoseidonSink struct {
	client stats.PoseidonStatsClient
	sync.RWMutex
}

func (sink *PoseidonSink) Name() string {
	return "Poseidon Sink"
}

func (sink *PoseidonSink) Stop() {
	// Do nothing.
}

func (sink *PoseidonSink) ExportData(dataBatch *core.DataBatch) {
	sink.Lock()
	defer sink.Unlock()

	for metricKey, metricSet := range dataBatch.MetricSets {
		if isNodeMetric(metricKey, metricSet) {
			nodeStats := &stats.NodeStats{
				Hostname:       metricSet.Labels[core.LabelHostname.Key],
				CpuAllocatable: float64(getMetricValue(metricSet, core.MetricNodeCpuAllocatable).FloatValue),
				CpuCapacity:    float64(getMetricValue(metricSet, core.MetricNodeCpuCapacity).FloatValue),
				CpuReservation: float64(getMetricValue(metricSet, core.MetricNodeCpuReservation).FloatValue),
				CpuUtilization: float64(getMetricValue(metricSet, core.MetricNodeCpuUtilization).FloatValue),
				MemAllocatable: float64(getMetricValue(metricSet, core.MetricNodeMemoryAllocatable).FloatValue),
				MemCapacity:    float64(getMetricValue(metricSet, core.MetricNodeMemoryCapacity).FloatValue),
				MemReservation: float64(getMetricValue(metricSet, core.MetricNodeMemoryReservation).FloatValue),
				MemUtilization: float64(getMetricValue(metricSet, core.MetricNodeMemoryUtilization).FloatValue),
			}
			stream, err := sink.client.ReceiveNodeStats(context.Background())
			if err != nil {
				log.Fatalf("Error connecting the server: %v", err)
			}
			nodeStatsResp, err := stream.Recv()
			if err != nil {
				log.Fatalf("Error receiving node stats response: %v", err)
			}
			switch nodeStatsResp.Type {
			case stats.NodeStatsResponseType_NODE_NOT_FOUND:
				log.Fatalf("Node %s not found", nodeStatsResp.GetHostname())
			case stats.NodeStatsResponseType_NODE_STATS_OK:
				err := stream.Send(nodeStats)
				if err != nil {
					log.Fatalf("Error sending node stats: %v", err)
				}
			default:
				panic(fmt.Sprintf("Unexpected node stats response: %v", err))
			}
		} else if isPodMetric(metricKey, metricSet) {
			podStats := &stats.PodStats{
				Name:      metricSet.Labels[core.LabelPodName.Key],
				Namespace: metricSet.Labels[core.LabelPodNamespace.Key],
				Hostname:  metricSet.Labels[core.LabelHostname.Key],
				// CPU stats
				CpuLimit:   getMetricValue(metricSet, core.MetricCpuLimit).IntValue,
				CpuRequest: getMetricValue(metricSet, core.MetricCpuRequest).IntValue,
				CpuUsage:   getMetricValue(metricSet, core.MetricCpuUsageRate).IntValue,
				// Memory stats
				MemLimit:            getMetricValue(metricSet, core.MetricMemoryLimit).IntValue / KILOBYTE,
				MemRequest:          getMetricValue(metricSet, core.MetricMemoryRequest).IntValue / KILOBYTE,
				MemUsage:            getMetricValue(metricSet, core.MetricMemoryUsage).IntValue / KILOBYTE,
				MemRss:              getMetricValue(metricSet, core.MetricMemoryRSS).IntValue / KILOBYTE,
				MemCache:            getMetricValue(metricSet, core.MetricMemoryCache).IntValue / KILOBYTE,
				MemWorkingSet:       getMetricValue(metricSet, core.MetricMemoryWorkingSet).IntValue / KILOBYTE,
				MemPageFaults:       getMetricValue(metricSet, core.MetricMemoryPageFaults).IntValue,
				MemPageFaultsRate:   float64(getMetricValue(metricSet, core.MetricMemoryPageFaultsRate).FloatValue),
				MajorPageFaults:     getMetricValue(metricSet, core.MetricMemoryMajorPageFaults).IntValue,
				MajorPageFaultsRate: float64(getMetricValue(metricSet, core.MetricMemoryMajorPageFaultsRate).FloatValue),
				//Network stats
				NetRx:           getMetricValue(metricSet, core.MetricNetworkRx).IntValue / KILOBYTE,
				NetRxErrors:     getMetricValue(metricSet, core.MetricNetworkRxErrors).IntValue,
				NetRxErrorsRate: float64(getMetricValue(metricSet, core.MetricNetworkRxErrorsRate).FloatValue),
				NetRxRate:       float64(getMetricValue(metricSet, core.MetricNetworkRxRate).FloatValue),
				NetTx:           getMetricValue(metricSet, core.MetricNetworkTx).IntValue / KILOBYTE,
				NetTxErrors:     getMetricValue(metricSet, core.MetricNetworkTxErrors).IntValue,
				NetTxErrorsRate: float64(getMetricValue(metricSet, core.MetricNetworkTxErrorsRate).FloatValue),
				NetTxRate:       float64(getMetricValue(metricSet, core.MetricNetworkTxRate).FloatValue),
			}
			stream, err := sink.client.ReceivePodStats(context.Background())
			if err != nil {
				log.Fatalf("Error connecting the server: %v", err)
			}
			podStatsResp, err := stream.Recv()
			if err != nil {
				log.Fatalf("Error receiving pod stats response: %v", err)
			}
			switch podStatsResp.Type {
			case stats.PodStatsResponseType_POD_NOT_FOUND:
				log.Fatalf("Pod %s/%s not found", podStatsResp.GetNamespace(), podStatsResp.GetName())
			case stats.PodStatsResponseType_POD_STATS_OK:
				err := stream.Send(podStats)
				if err != nil {
					log.Fatalf("Error sending node stats: %v", err)
				}
			default:
				panic(fmt.Sprintf("Unexpected pod stats response: %v", err))
			}
		} else {
			// other types (cluster, node container, pod container, etc.) are ignored
			continue
		}
	}
}

func getMetricValue(metricSet *core.MetricSet, metric core.Metric) core.MetricValue {
	return metricSet.MetricValues[metric.MetricDescriptor.Name]
}

func isNodeMetric(key string, metricSet *core.MetricSet) bool {
	// node key format: node:<node-name>
	return key == core.NodeKey(metricSet.Labels[core.LabelNodename.Key])
}

func isPodMetric(key string, metricSet *core.MetricSet) bool {
	// pod key format: namespace:<namespace-name>/pod:<pod-name>
	return key == core.PodKey(metricSet.Labels[core.LabelNamespaceName.Key],
		metricSet.Labels[core.LabelPodName.Key])
}

func NewPoseidonSink(uri *url.URL) (core.DataSink, error) {
	// Set up a connection to the gRPC server.
	// TODO(Eissa): figure out connection options and modify this accordingly
	conn, err := grpc.Dial(uri.Host, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Connection to the gRPC server failed: %v", err)
	}
	defer conn.Close()

	client := stats.NewPoseidonStatsClient(conn)
	return &PoseidonSink{client: client}, nil
}
