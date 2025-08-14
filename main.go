// Package main implements a Docker state exporter for Prometheus metrics.
//
// This exporter connects to the Docker daemon and exports container state information
// as Prometheus metrics, including health status, running state, OOM killed status,
// and restart counts.
//
// The exporter provides the following metrics:
//   - container_state_health_status: Container health status (none, starting, healthy, unhealthy)
//   - container_state_status: Container status (paused, restarting, running, removing, dead, created, exited)
//   - container_state_oomkilled: Whether the container was killed by OOMKiller
//   - container_state_startedat: Unix timestamp when the container started
//   - container_state_finishedat: Unix timestamp when the container finished
//   - container_restartcount: Number of times the container has been restarted
//
// Usage:
//
//	docker_state_exporter [flags]
//
// Flags:
//
//	-listen-address string
//	    The address to listen on for HTTP requests (default ":8080")
//	-add-container-labels
//	    Add labels from docker containers as metric labels (default true)
//	-cache-period int
//	    The period of time the collector will reuse the results of docker inspect
//	    before polling again, in seconds (default 1)
//
// Examples:
//
//	# Start the exporter on default port 8080
//	docker_state_exporter
//
//	# Start on custom port with 5-second cache
//	docker_state_exporter -listen-address=:9090 -cache-period=5
//
//	# Start without container labels
//	docker_state_exporter -add-container-labels=false
//
// The exporter exposes metrics at /metrics and provides a health check at /-/healthy.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// addContainerLabels controls whether to include Docker container labels as Prometheus metric labels.
	// When enabled, all labels from the container will be added to the metrics with a "container_label_" prefix.
	addContainerLabels bool

	// cachePeriod defines how long to cache Docker inspect results before polling again.
	// This reduces the load on the Docker API while maintaining reasonable freshness of data.
	cachePeriod time.Duration
)

// dockerHealthCollector implements the prometheus.Collector interface to collect
// Docker container state metrics. It caches container information for a configurable
// period to reduce Docker API calls while maintaining reasonable freshness of data.
//
// The collector exports metrics about container health status, running state,
// OOM kill status, start/finish times, and restart counts.
type dockerHealthCollector struct {
	mu                 sync.Mutex                  // Protects concurrent access to cache
	containerClient    *client.Client              // Docker client for API calls
	containerInfoCache []container.InspectResponse // Cached container information
	lastseen           time.Time                   // Last time cache was refreshed
}

// descSource provides a helper for creating Prometheus metric descriptions
// with consistent naming and help text.
type descSource struct {
	name string // Metric name
	help string // Help text describing the metric
}

// Desc creates a Prometheus metric description with the given labels.
// It returns a new prometheus.Desc that can be used to create metrics.
func (desc *descSource) Desc(labels prometheus.Labels) *prometheus.Desc {
	return prometheus.NewDesc(desc.name, desc.help, nil, labels)
}

var (
	namespace        = "container_state_"
	healthStatusDesc = descSource{
		namespace + "health_status",
		"Container health status."}
	statusDesc = descSource{
		namespace + "status",
		"Container status."}
	oomkilledDesc = descSource{
		namespace + "oomkilled",
		"Container was killed by OOMKiller."}
	startedatDesc = descSource{
		namespace + "startedat",
		"Time when the Container started."}
	finishedatDesc = descSource{
		namespace + "finishedat",
		"Time when the Container finished."}
	restartcountDesc = descSource{
		"container_restartcount",
		"Number of times the container has been restarted"}
)

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector to the provided channel and returns once
// the last descriptor has been sent.
func (c *dockerHealthCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- healthStatusDesc.Desc(nil)
	ch <- statusDesc.Desc(nil)
	ch <- oomkilledDesc.Desc(nil)
	ch <- startedatDesc.Desc(nil)
	ch <- finishedatDesc.Desc(nil)
	ch <- restartcountDesc.Desc(nil)
}

// Collect is called by Prometheus when collecting metrics. It fetches current
// container state (using cache if fresh enough) and sends metrics to the channel.
func (c *dockerHealthCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	if now.Sub(c.lastseen) >= cachePeriod {
		c.collectContainer()
		c.lastseen = now
	}
	c.collectMetrics(ch)
}

func (c *dockerHealthCollector) collectMetrics(ch chan<- prometheus.Metric) {
	for _, info := range c.containerInfoCache {
		var labels = map[string]string{}

		rep := regexp.MustCompile("[^a-zA-Z0-9_]")

		if addContainerLabels && info.Config != nil {
			for k, v := range info.Config.Labels {
				label := strings.ToLower("container_label_" + k)
				labels[rep.ReplaceAllLiteralString(label, "_")] = v
			}
		}
		labels["id"] = "/docker/" + info.ID
		if info.Config != nil {
			labels["image"] = info.Config.Image
		}
		labels["name"] = strings.TrimPrefix(info.Name, "/")

		b2f := func(b bool) float64 {
			if b {
				return 1
			}
			return 0
		}
		mapcopy := func(_ map[string]string) prometheus.Labels {
			dst := map[string]string{}
			for k, v := range labels {
				dst[k] = v
			}
			return dst
		}

		// Health status metrics - handle nil health
		for _, lv := range []string{"none", "starting", "healthy", "unhealthy"} {
			tmpLabels := mapcopy(labels)
			tmpLabels["status"] = lv
			var value float64
			if info.State.Health != nil {
				value = b2f(info.State.Health.Status == lv)
			} else {
				value = b2f(lv == "none")
			}
			ch <- prometheus.MustNewConstMetric(
				healthStatusDesc.Desc(tmpLabels),
				prometheus.GaugeValue,
				value,
			)
		}
		for _, lv := range []string{"paused", "restarting", "running", "removing", "dead", "created", "exited"} {
			tmpLabels := mapcopy(labels)
			tmpLabels["status"] = lv
			ch <- prometheus.MustNewConstMetric(statusDesc.Desc(tmpLabels), prometheus.GaugeValue, b2f(info.State.Status == lv))
		}
		ch <- prometheus.MustNewConstMetric(oomkilledDesc.Desc(labels), prometheus.GaugeValue, b2f(info.State.OOMKilled))
		startedat, err := time.Parse(time.RFC3339Nano, info.State.StartedAt)
		errCheck(err)
		finishedat, err := time.Parse(time.RFC3339Nano, info.State.FinishedAt)
		errCheck(err)
		ch <- prometheus.MustNewConstMetric(startedatDesc.Desc(labels), prometheus.GaugeValue, float64(startedat.Unix()))
		ch <- prometheus.MustNewConstMetric(finishedatDesc.Desc(labels), prometheus.GaugeValue, float64(finishedat.Unix()))
		ch <- prometheus.MustNewConstMetric(restartcountDesc.Desc(labels), prometheus.GaugeValue, float64(info.RestartCount))
	}
}

func (c *dockerHealthCollector) collectContainer() {
	containers, err := c.containerClient.ContainerList(context.Background(), container.ListOptions{All: true})
	errCheck(err)
	c.containerInfoCache = []container.InspectResponse{}

	for _, container := range containers {
		info, err := c.containerClient.ContainerInspect(context.Background(), container.ID)
		errCheck(err)
		c.containerInfoCache = append(c.containerInfoCache, info)

		// Note: We don't modify the info struct as it's from the Docker API
		// The collectMetrics function will handle nil checks appropriately
	}
}

type loggerWrapper struct {
	Logger *log.Logger
}

func (l *loggerWrapper) Println(v ...interface{}) {
	if err := (*l.Logger).Log("messages", v); err != nil {
		// fallback to stderr if logging fails
		fmt.Fprintf(os.Stderr, "Failed to log: %v\n", err)
	}
}

// Define loggers.
var (
	normalLogger = log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
	errorLogger  = log.NewJSONLogger(log.NewSyncWriter(os.Stderr))
)

func errCheck(err error) {
	if err != nil {
		if logErr := errorLogger.Log("message", err); logErr != nil {
			fmt.Fprintf(os.Stderr, "Failed to log error: %v, original error: %v\n", logErr, err)
		}
		os.Exit(1)
	}
}

// Define flags.
var (
	address = flag.String(
		"listen-address",
		":8080",
		"The address to listen on for HTTP requests.",
	)
)

func init() {
	normalLogger = log.With(normalLogger, "timestamp", log.DefaultTimestampUTC)
	normalLogger = log.With(normalLogger, "severity", "info")
	errorLogger = log.With(errorLogger, "timestamp", log.DefaultTimestampUTC)
	errorLogger = log.With(errorLogger, "severity", "error")
	prometheus.MustRegister(collectors.NewBuildInfoCollector())
	cachePeriodFlag := flag.Int(
		"cache-period",
		1,
		"Period to reuse docker inspect results before polling again, in seconds",
	)
	cachePeriod = time.Duration(*cachePeriodFlag) * time.Second
	flag.BoolVar(&addContainerLabels, "add-container-labels", true, "Add labels from docker containers as metric labels")
}

func main() {
	flag.Parse()

	client, err := client.NewClientWithOpts(
		client.FromEnv,
		client.WithAPIVersionNegotiation(),
	)
	errCheck(err)
	defer client.Close()

	_, err = client.Ping(context.Background())
	errCheck(err)

	prometheus.MustRegister(&dockerHealthCollector{
		containerClient: client,
	})

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "<h1>docker state exporter</h1>")
	})

	http.HandleFunc("/-/healthy", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "up")
	})

	http.Handle("/metrics", promhttp.HandlerFor(
		prometheus.DefaultGatherer,
		promhttp.HandlerOpts{ErrorLog: &loggerWrapper{Logger: &errorLogger}, EnableOpenMetrics: true}))

	if err := normalLogger.Log("message", "Server listening...", "address", address); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to log server start: %v\n", err)
	}

	server := &http.Server{
		Addr:              *address,
		Handler:           nil,
		ReadHeaderTimeout: 30 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		err = server.ListenAndServe()
		if err != http.ErrServerClosed {
			errCheck(err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, os.Interrupt)
	<-quit
	if err := normalLogger.Log("message", "Server shutting down..."); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to log server shutdown: %v\n", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		if logErr := errorLogger.Log("message", fmt.Sprintf("Failed to gracefully shutdown: %v", err)); logErr != nil {
			fmt.Fprintf(os.Stderr, "Failed to log shutdown error: %v, original error: %v\n", logErr, err)
		}
	}
	if err := normalLogger.Log("message", "Server shutdown"); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to log server shutdown complete: %v\n", err)
	}
}
