package poseidon

import (
	"fmt"
	"github.com/golang/glog"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"k8s.io/heapster/metrics/core"
	"k8s.io/heapster/metrics/sinks/poseidon/stats"
	"net/url"
	"strconv"
	"sync"
	"time"
)

const (
	BYTE     = 1
	KILOBYTE = 1024 * BYTE
)

type PoseidonSink struct {
	client stats.PoseidonStatsClient
	sync.RWMutex
	nodeStatChan chan *stats.NodeStats
	podStatChan  chan *stats.PodStats
}

func (sink *PoseidonSink) Name() string {
	return "Poseidon Sink"
}

func (sink *PoseidonSink) Stop() {

	glog.V(2).Infoln("PoseidonSink Stopping...")

}

func (sink *PoseidonSink) ProcessNodeStats(nodeMetrics *core.MetricSet) {
	glog.V(2).Infoln("ProcessNodeStats called...")
	nodeStats := &stats.NodeStats{
		Hostname:           nodeMetrics.Labels[core.LabelHostname.Key],
		Timestamp:          uint64(nodeMetrics.ScrapeTime.UnixNano()) / uint64(time.Microsecond),
		CpuAllocatable:     int64(getMetricValue(nodeMetrics, core.MetricNodeCpuAllocatable).FloatValue),
		CpuCapacity:        int64(getMetricValue(nodeMetrics, core.MetricNodeCpuCapacity).FloatValue),
		CpuReservation:     float64(getMetricValue(nodeMetrics, core.MetricNodeCpuReservation).FloatValue),
		CpuUtilization:     float64(getMetricValue(nodeMetrics, core.MetricNodeCpuUtilization).FloatValue),
		MemAllocatable:     int64(getMetricValue(nodeMetrics, core.MetricNodeMemoryAllocatable).FloatValue / KILOBYTE),
		MemCapacity:        int64(getMetricValue(nodeMetrics, core.MetricNodeMemoryCapacity).FloatValue / KILOBYTE),
		MemReservation:     float64(getMetricValue(nodeMetrics, core.MetricNodeMemoryReservation).FloatValue),
		MemUtilization:     float64(getMetricValue(nodeMetrics, core.MetricNodeMemoryUtilization).FloatValue),
		CpuCoreUtilization: getCoreValues(nodeMetrics),
	}
	glog.V(2).Infoln("Sending nodeStat to nodeStatChan", nodeStats)
	sink.nodeStatChan <- nodeStats
}

func (sink *PoseidonSink) SendPodStatToPoseidon() {
	glog.V(2).Infoln("SendPodStatToPoseidon called...")
	for {
		stream, err := sink.client.ReceivePodStats(context.Background())
		if err != nil {
			glog.Errorf("Error connecting to poseidon server: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}
		podStats := <-sink.podStatChan
		glog.V(2).Infoln("Sending podStats to posedion", podStats)
		err = stream.Send(podStats)
		if err != nil {
			glog.Errorf("Error sending pod stats to poseidon: %v", err)
			continue
		}
		podStatsResp, err := stream.Recv()
		if err != nil {
			glog.Errorf("Error receiving pod stats response from poseidon: %v", err)
			continue
		}
		switch podStatsResp.Type {
		case stats.PodStatsResponseType_POD_NOT_FOUND:
			glog.V(2).Infoln("Pod ", podStatsResp.GetNamespace(), "/", podStatsResp.GetName(), " not found")
		case stats.PodStatsResponseType_POD_STATS_OK:
			glog.V(2).Infoln("Pod ", podStatsResp.GetNamespace(), "/", podStatsResp.GetName(), " found")
		default:
			panic(fmt.Sprintf("Unexpected pod stats response: %v", err))
		}
	}
}

func (sink *PoseidonSink) SendNodeStatToPoseidon() {
	glog.V(2).Infoln("SendNodeStatToPoseidon called...")
	for {
		stream, err := sink.client.ReceiveNodeStats(context.Background())
		if err != nil {
			glog.Errorf("Error connecting to poseidon server: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		nodeStats := <-sink.nodeStatChan
		glog.V(2).Infoln("Sending NodeStats to poseidon", nodeStats)
		err = stream.Send(nodeStats)
		if err != nil {
			glog.Errorf("Error sending node stats to poseidon: %v", err)
			continue
		}
		nodeStatsResp, err := stream.Recv()
		if err != nil {
			glog.Errorf("Error receiving node stats response from poseidon: %v", err)
			continue
		}
		switch nodeStatsResp.Type {
		case stats.NodeStatsResponseType_NODE_NOT_FOUND:
			glog.V(2).Infoln("Node ", nodeStatsResp.GetHostname(), " not found")
		case stats.NodeStatsResponseType_NODE_STATS_OK:
			glog.V(2).Infoln("Node ", nodeStatsResp.GetHostname(), " found")
		default:
			panic(fmt.Sprintf("Unexpected node stats response: %v", err))
		}
	}
}

func (sink *PoseidonSink) ProcessPodStats(nodeMetrics *core.MetricSet) {
	podStats := &stats.PodStats{
		Name:      nodeMetrics.Labels[core.LabelPodName.Key],
		Namespace: nodeMetrics.Labels[core.LabelNamespaceName.Key],
		Hostname:  nodeMetrics.Labels[core.LabelHostname.Key],
		// CPU stats
		CpuLimit:   getMetricValue(nodeMetrics, core.MetricCpuLimit).IntValue,
		CpuRequest: getMetricValue(nodeMetrics, core.MetricCpuRequest).IntValue,
		CpuUsage:   getMetricValue(nodeMetrics, core.MetricCpuUsageRate).IntValue,
		// Memory stats
		MemLimit:            getMetricValue(nodeMetrics, core.MetricMemoryLimit).IntValue / KILOBYTE,
		MemRequest:          getMetricValue(nodeMetrics, core.MetricMemoryRequest).IntValue / KILOBYTE,
		MemUsage:            getMetricValue(nodeMetrics, core.MetricMemoryUsage).IntValue / KILOBYTE,
		MemRss:              getMetricValue(nodeMetrics, core.MetricMemoryRSS).IntValue / KILOBYTE,
		MemCache:            getMetricValue(nodeMetrics, core.MetricMemoryCache).IntValue / KILOBYTE,
		MemWorkingSet:       getMetricValue(nodeMetrics, core.MetricMemoryWorkingSet).IntValue / KILOBYTE,
		MemPageFaults:       getMetricValue(nodeMetrics, core.MetricMemoryPageFaults).IntValue,
		MemPageFaultsRate:   float64(getMetricValue(nodeMetrics, core.MetricMemoryPageFaultsRate).FloatValue),
		MajorPageFaults:     getMetricValue(nodeMetrics, core.MetricMemoryMajorPageFaults).IntValue,
		MajorPageFaultsRate: float64(getMetricValue(nodeMetrics, core.MetricMemoryMajorPageFaultsRate).FloatValue),
		//Network stats
		NetRx:           getMetricValue(nodeMetrics, core.MetricNetworkRx).IntValue / KILOBYTE,
		NetRxErrors:     getMetricValue(nodeMetrics, core.MetricNetworkRxErrors).IntValue,
		NetRxErrorsRate: float64(getMetricValue(nodeMetrics, core.MetricNetworkRxErrorsRate).FloatValue),
		NetRxRate:       float64(getMetricValue(nodeMetrics, core.MetricNetworkRxRate).FloatValue),
		NetTx:           getMetricValue(nodeMetrics, core.MetricNetworkTx).IntValue / KILOBYTE,
		NetTxErrors:     getMetricValue(nodeMetrics, core.MetricNetworkTxErrors).IntValue,
		NetTxErrorsRate: float64(getMetricValue(nodeMetrics, core.MetricNetworkTxErrorsRate).FloatValue),
		NetTxRate:       float64(getMetricValue(nodeMetrics, core.MetricNetworkTxRate).FloatValue),
	}
	sink.podStatChan <- podStats
}

func (sink *PoseidonSink) ExportData(dataBatch *core.DataBatch) {
	sink.Lock()
	defer sink.Unlock()

	glog.V(2).Infoln("ExportData called...")
	for metricKey, metricSet := range dataBatch.MetricSets {
		if isNodeMetric(metricKey, metricSet) {
			go func(nodeMetric *core.MetricSet) {
				sink.ProcessNodeStats(nodeMetric)
			}(metricSet)
		} else if isPodMetric(metricKey, metricSet) {
			go func(nodeMetric *core.MetricSet) {
				sink.ProcessPodStats(nodeMetric)
			}(metricSet)
		} else {
			// other types (cluster, node container, pod container, etc.) are ignored
			continue
		}
	}
}

func getCoreValues(metricSet *core.MetricSet) []int64 {
	//TODO:(shiv) Assuming 128 core to be the max core per node
	var coreUsage []int64
	for i := 0; i < 128; i++ {
		if value, ok := metricSet.MetricValues["cpucore/core_"+strconv.Itoa(i)]; ok {
			coreUsage = append(coreUsage, value.IntValue)
		} else {
			break
		}
	}
	return coreUsage
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
	glog.Info("PoseidonSink called", uri)
	globalConnn, err := grpc.Dial(uri.Host, grpc.WithInsecure())
	if err != nil {
		glog.Errorf("Connection to the gRPC server failed: %v", err)
	}

	client := stats.NewPoseidonStatsClient(globalConnn)
	newPoseidonSink := &PoseidonSink{client: client,
		nodeStatChan: make(chan *stats.NodeStats),
		podStatChan:  make(chan *stats.PodStats),
	}
	go newPoseidonSink.SendNodeStatToPoseidon()
	go newPoseidonSink.SendPodStatToPoseidon()

	return newPoseidonSink, nil
}
