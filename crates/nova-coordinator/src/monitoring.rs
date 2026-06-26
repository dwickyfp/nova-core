// Monitoring — Prometheus metrics + tracing.
//
// Phase 6.5: Export metrics for observability.
// - Query metrics: latency, throughput, errors
// - Cache metrics: hit rate, evictions, size
// - Storage metrics: MP count, bytes read/written
// - System metrics: memory, CPU, goroutines

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Metric counter.
#[derive(Debug, Default)]
pub struct Counter {
    value: AtomicU64,
}

impl Counter {
    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add(&self, n: u64) {
        self.value.fetch_add(n, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

/// Metric gauge.
#[derive(Debug, Default)]
pub struct Gauge {
    value: AtomicU64,
}

impl Gauge {
    pub fn set(&self, v: u64) {
        self.value.store(v, Ordering::Relaxed);
    }

    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec(&self) {
        self.value.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

/// Histogram for latency tracking.
#[derive(Debug)]
pub struct Histogram {
    buckets: Vec<(f64, AtomicU64)>,
    sum: AtomicU64,
    count: AtomicU64,
}

impl Histogram {
    pub fn new(bounds: Vec<f64>) -> Self {
        let buckets = bounds.into_iter().map(|b| (b, AtomicU64::new(0))).collect();
        Self {
            buckets,
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    pub fn observe(&self, value: f64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum
            .fetch_add((value * 1000.0) as u64, Ordering::Relaxed);
        for (bound, counter) in &self.buckets {
            if value <= *bound {
                counter.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    pub fn sum_ms(&self) -> u64 {
        self.sum.load(Ordering::Relaxed)
    }
}

/// Query metrics.
#[derive(Debug)]
pub struct QueryMetrics {
    pub total_queries: Counter,
    pub successful_queries: Counter,
    pub failed_queries: Counter,
    pub query_latency_ms: Histogram,
    pub active_queries: Gauge,
}

impl QueryMetrics {
    pub fn new() -> Self {
        Self {
            total_queries: Counter::default(),
            successful_queries: Counter::default(),
            failed_queries: Counter::default(),
            query_latency_ms: Histogram::new(vec![
                1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0,
            ]),
            active_queries: Gauge::default(),
        }
    }

    pub fn record_query_start(&self) -> Instant {
        self.total_queries.inc();
        self.active_queries.inc();
        Instant::now()
    }

    pub fn record_query_end(&self, start: Instant, success: bool) {
        let latency = start.elapsed().as_secs_f64() * 1000.0;
        self.query_latency_ms.observe(latency);
        self.active_queries.dec();
        if success {
            self.successful_queries.inc();
        } else {
            self.failed_queries.inc();
        }
    }
}

impl Default for QueryMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Cache metrics.
#[derive(Debug)]
pub struct CacheMetrics {
    pub l1_hits: Counter,
    pub l1_misses: Counter,
    pub l2_hits: Counter,
    pub l2_misses: Counter,
    pub l3_hits: Counter,
    pub l3_misses: Counter,
    pub evictions: Counter,
    pub bytes_cached: Gauge,
}

impl CacheMetrics {
    pub fn new() -> Self {
        Self {
            l1_hits: Counter::default(),
            l1_misses: Counter::default(),
            l2_hits: Counter::default(),
            l2_misses: Counter::default(),
            l3_hits: Counter::default(),
            l3_misses: Counter::default(),
            evictions: Counter::default(),
            bytes_cached: Gauge::default(),
        }
    }

    pub fn l1_hit_rate(&self) -> f64 {
        let hits = self.l1_hits.get() as f64;
        let misses = self.l1_misses.get() as f64;
        let total = hits + misses;
        if total == 0.0 { 0.0 } else { hits / total }
    }
}

impl Default for CacheMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Storage metrics.
#[derive(Debug)]
pub struct StorageMetrics {
    pub mps_created: Counter,
    pub mps_read: Counter,
    pub bytes_written: Counter,
    pub bytes_read: Counter,
    pub s3_requests: Counter,
    pub active_mps: Gauge,
}

impl StorageMetrics {
    pub fn new() -> Self {
        Self {
            mps_created: Counter::default(),
            mps_read: Counter::default(),
            bytes_written: Counter::default(),
            bytes_read: Counter::default(),
            s3_requests: Counter::default(),
            active_mps: Gauge::default(),
        }
    }
}

impl Default for StorageMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Unified metrics registry.
#[derive(Debug)]
pub struct MetricsRegistry {
    pub queries: QueryMetrics,
    pub cache: CacheMetrics,
    pub storage: StorageMetrics,
    pub start_time: Instant,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self {
            queries: QueryMetrics::new(),
            cache: CacheMetrics::new(),
            storage: StorageMetrics::new(),
            start_time: Instant::now(),
        }
    }

    /// Export metrics in Prometheus text format.
    pub fn export_prometheus(&self) -> String {
        let mut out = String::new();

        // Query metrics
        out.push_str("# HELP nova_queries_total Total number of queries\n");
        out.push_str("# TYPE nova_queries_total counter\n");
        out.push_str(&format!(
            "nova_queries_total {}\n",
            self.queries.total_queries.get()
        ));

        out.push_str("# HELP nova_queries_successful Successful queries\n");
        out.push_str("# TYPE nova_queries_successful counter\n");
        out.push_str(&format!(
            "nova_queries_successful {}\n",
            self.queries.successful_queries.get()
        ));

        out.push_str("# HELP nova_queries_failed Failed queries\n");
        out.push_str("# TYPE nova_queries_failed counter\n");
        out.push_str(&format!(
            "nova_queries_failed {}\n",
            self.queries.failed_queries.get()
        ));

        out.push_str("# HELP nova_queries_active Currently active queries\n");
        out.push_str("# TYPE nova_queries_active gauge\n");
        out.push_str(&format!(
            "nova_queries_active {}\n",
            self.queries.active_queries.get()
        ));

        out.push_str(&format!(
            "nova_query_latency_ms_count {}\n",
            self.queries.query_latency_ms.count()
        ));
        out.push_str(&format!(
            "nova_query_latency_ms_sum {}\n",
            self.queries.query_latency_ms.sum_ms()
        ));

        // Cache metrics
        out.push_str("# HELP nova_cache_l1_hits L1 cache hits\n");
        out.push_str("# TYPE nova_cache_l1_hits counter\n");
        out.push_str(&format!(
            "nova_cache_l1_hits {}\n",
            self.cache.l1_hits.get()
        ));

        out.push_str("# HELP nova_cache_l1_misses L1 cache misses\n");
        out.push_str("# TYPE nova_cache_l1_misses counter\n");
        out.push_str(&format!(
            "nova_cache_l1_misses {}\n",
            self.cache.l1_misses.get()
        ));

        out.push_str("# HELP nova_cache_evictions Cache evictions\n");
        out.push_str("# TYPE nova_cache_evictions counter\n");
        out.push_str(&format!(
            "nova_cache_evictions {}\n",
            self.cache.evictions.get()
        ));

        // Storage metrics
        out.push_str("# HELP nova_storage_mps_created Total MPs created\n");
        out.push_str("# TYPE nova_storage_mps_created counter\n");
        out.push_str(&format!(
            "nova_storage_mps_created {}\n",
            self.storage.mps_created.get()
        ));

        out.push_str("# HELP nova_storage_bytes_written Total bytes written\n");
        out.push_str("# TYPE nova_storage_bytes_written counter\n");
        out.push_str(&format!(
            "nova_storage_bytes_written {}\n",
            self.storage.bytes_written.get()
        ));

        out.push_str(&format!(
            "nova_uptime_seconds {}\n",
            self.start_time.elapsed().as_secs()
        ));

        out
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter() {
        let c = Counter::default();
        assert_eq!(c.get(), 0);
        c.inc();
        assert_eq!(c.get(), 1);
        c.add(5);
        assert_eq!(c.get(), 6);
    }

    #[test]
    fn test_gauge() {
        let g = Gauge::default();
        g.set(10);
        assert_eq!(g.get(), 10);
        g.inc();
        assert_eq!(g.get(), 11);
        g.dec();
        assert_eq!(g.get(), 10);
    }

    #[test]
    fn test_histogram() {
        let h = Histogram::new(vec![10.0, 50.0, 100.0]);
        h.observe(5.0);
        h.observe(25.0);
        h.observe(75.0);
        assert_eq!(h.count(), 3);
    }

    #[test]
    fn test_query_metrics() {
        let m = QueryMetrics::new();
        let start = m.record_query_start();
        assert_eq!(m.total_queries.get(), 1);
        assert_eq!(m.active_queries.get(), 1);

        m.record_query_end(start, true);
        assert_eq!(m.successful_queries.get(), 1);
        assert_eq!(m.failed_queries.get(), 0);
        assert_eq!(m.active_queries.get(), 0);
    }

    #[test]
    fn test_cache_metrics_hit_rate() {
        let m = CacheMetrics::new();
        m.l1_hits.add(8);
        m.l1_misses.add(2);
        assert!((m.l1_hit_rate() - 0.8).abs() < 0.01);
    }

    #[test]
    fn test_prometheus_export() {
        let m = MetricsRegistry::new();
        m.queries.total_queries.add(100);
        m.cache.l1_hits.add(50);

        let output = m.export_prometheus();
        assert!(output.contains("nova_queries_total 100"));
        assert!(output.contains("nova_cache_l1_hits 50"));
        assert!(output.contains("nova_uptime_seconds"));
    }
}
