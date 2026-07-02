// Advanced Statistics — histograms, MCV, cardinality estimation.
//
// Phase 5.3: Column-level statistics beyond min/max (from Phase 2.4).
// - Equi-height histogram: divide value range into buckets with equal row counts
// - Most Common Values (MCV): top-N most frequent values
// - Cardinality estimation: use histogram + MCV for accurate row count estimates

use std::collections::HashMap;

/// Equi-height histogram bucket.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HistogramBucket {
    pub lower: f64,
    pub upper: f64,
    pub row_count: u64,
    pub distinct_count: u64,
}

/// Column histogram (equi-height).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ColumnHistogram {
    pub buckets: Vec<HistogramBucket>,
}

impl ColumnHistogram {
    /// Build equi-height histogram from a sorted list of values.
    pub fn from_values(values: &[f64], num_buckets: usize) -> Self {
        if values.is_empty() {
            return Self { buckets: vec![] };
        }

        let mut sorted = values.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let n = sorted.len();
        let bucket_size = n.div_ceil(num_buckets);
        let mut buckets = Vec::with_capacity(num_buckets);

        let mut i = 0;
        while i < n {
            let end = (i + bucket_size).min(n);
            let lower = sorted[i];
            let upper = sorted[end - 1];
            let row_count = (end - i) as u64;

            // Count distinct values in bucket
            let mut distinct = 1;
            for j in (i + 1)..end {
                if sorted[j] != sorted[j - 1] {
                    distinct += 1;
                }
            }

            buckets.push(HistogramBucket {
                lower,
                upper,
                row_count,
                distinct_count: distinct,
            });
            i = end;
        }

        Self { buckets }
    }

    /// Estimate number of rows matching a predicate.
    pub fn estimate_selectivity(&self, op: &str, value: f64) -> f64 {
        if self.buckets.is_empty() {
            return 0.33; // unknown → default selectivity
        }

        let total_rows: u64 = self.buckets.iter().map(|b| b.row_count).sum();
        if total_rows == 0 {
            return 0.0;
        }

        let matching_rows: u64 = self
            .buckets
            .iter()
            .map(|b| {
                let fraction = match op {
                    "=" => {
                        if value >= b.lower && value <= b.upper {
                            // In bucket → 1/distinct_count
                            if b.distinct_count > 0 {
                                b.row_count as f64 / b.distinct_count as f64
                            } else {
                                0.0
                            }
                        } else {
                            0.0
                        }
                    }
                    ">" => {
                        if value <= b.lower {
                            b.row_count as f64 // all rows match
                        } else if value >= b.upper {
                            0.0 // no rows match
                        } else {
                            // Partial: (upper - value) / (upper - lower)
                            let range = b.upper - b.lower;
                            if range > 0.0 {
                                (b.upper - value) / range * b.row_count as f64
                            } else {
                                0.0
                            }
                        }
                    }
                    "<" => {
                        if value >= b.upper {
                            b.row_count as f64
                        } else if value <= b.lower {
                            0.0
                        } else {
                            let range = b.upper - b.lower;
                            if range > 0.0 {
                                (value - b.lower) / range * b.row_count as f64
                            } else {
                                0.0
                            }
                        }
                    }
                    ">=" => self.estimate_bucket_selectivity(b, ">", value),
                    "<=" => self.estimate_bucket_selectivity(b, "<", value),
                    _ => b.row_count as f64 * 0.33,
                };
                fraction as u64
            })
            .sum();

        matching_rows as f64 / total_rows as f64
    }

    fn estimate_bucket_selectivity(&self, bucket: &HistogramBucket, op: &str, value: f64) -> f64 {
        // >= is same as > but inclusive, <= is same as < but inclusive
        // Approximation: add 1 row for the boundary
        let base = self.estimate_selectivity(op, value);
        (base * bucket.row_count as f64 + 1.0).min(bucket.row_count as f64)
    }
}

/// Most Common Values (MCV) — top-N most frequent values.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MostCommonValues {
    pub values: Vec<f64>,
    pub counts: Vec<u64>,
    pub total_rows: u64,
}

impl MostCommonValues {
    /// Build MCV from a list of values.
    pub fn from_values(values: &[f64], top_n: usize) -> Self {
        let total_rows = values.len() as u64;
        let mut counts: HashMap<u64, u64> = HashMap::new();
        let mut value_map: HashMap<u64, f64> = HashMap::new();

        for &v in values {
            let bits = v.to_bits();
            *counts.entry(bits).or_insert(0) += 1;
            value_map.insert(bits, v);
        }

        let mut sorted: Vec<(u64, u64)> = counts.into_iter().collect();
        sorted.sort_by_key(|b| std::cmp::Reverse(b.1));

        let (values, counts): (Vec<f64>, Vec<u64>) = sorted
            .into_iter()
            .take(top_n)
            .map(|(bits, count)| (value_map[&bits], count))
            .unzip();

        Self {
            values,
            counts,
            total_rows,
        }
    }

    /// Get frequency of a specific value (0.0 if not in MCV).
    pub fn frequency(&self, value: f64) -> f64 {
        for (i, &v) in self.values.iter().enumerate() {
            if v == value {
                return self.counts[i] as f64 / self.total_rows as f64;
            }
        }
        0.0
    }
}

/// Table statistics collected by ANALYZE TABLE.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AdvancedTableStats {
    pub row_count: u64,
    pub column_histograms: HashMap<String, ColumnHistogram>,
    pub column_mcvs: HashMap<String, MostCommonValues>,
}

/// Estimate cardinality of a filter predicate using histograms + MCV.
pub fn estimate_cardinality(stats: &AdvancedTableStats, column: &str, op: &str, value: f64) -> u64 {
    // Try MCV first (exact match)
    if op == "="
        && let Some(mcv) = stats.column_mcvs.get(column)
    {
        let freq = mcv.frequency(value);
        if freq > 0.0 {
            return (freq * stats.row_count as f64) as u64;
        }
    }

    // Fall back to histogram
    if let Some(hist) = stats.column_histograms.get(column) {
        let selectivity = hist.estimate_selectivity(op, value);
        return (selectivity * stats.row_count as f64) as u64;
    }

    // Default selectivity if no stats
    (0.33 * stats.row_count as f64) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_histogram_basic() {
        let values: Vec<f64> = (0..1000).map(|i| i as f64).collect();
        let hist = ColumnHistogram::from_values(&values, 10);

        assert_eq!(hist.buckets.len(), 10);
        let total: u64 = hist.buckets.iter().map(|b| b.row_count).sum();
        assert_eq!(total, 1000);
    }

    #[test]
    fn test_histogram_empty() {
        let hist = ColumnHistogram::from_values(&[], 10);
        assert!(hist.buckets.is_empty());
    }

    #[test]
    fn test_histogram_estimate_eq() {
        let values: Vec<f64> = (0..1000).map(|i| i as f64).collect();
        let hist = ColumnHistogram::from_values(&values, 10);

        let sel = hist.estimate_selectivity("=", 500.0);
        // Should be ~0.1 (1 bucket out of 10, and 1 distinct value in that bucket)
        assert!(sel > 0.0 && sel < 0.2, "selectivity = {}", sel);
    }

    #[test]
    fn test_histogram_estimate_gt() {
        let values: Vec<f64> = (0..1000).map(|i| i as f64).collect();
        let hist = ColumnHistogram::from_values(&values, 10);

        let sel = hist.estimate_selectivity(">", 500.0);
        // ~50% of rows should be > 500
        assert!(sel > 0.3 && sel < 0.7, "selectivity = {}", sel);
    }

    #[test]
    fn test_histogram_estimate_lt() {
        let values: Vec<f64> = (0..1000).map(|i| i as f64).collect();
        let hist = ColumnHistogram::from_values(&values, 10);

        let sel = hist.estimate_selectivity("<", 250.0);
        assert!(sel > 0.1 && sel < 0.4, "selectivity = {}", sel);
    }

    #[test]
    fn test_mcv_basic() {
        let values = vec![1.0, 1.0, 1.0, 2.0, 2.0, 3.0];
        let mcv = MostCommonValues::from_values(&values, 3);

        assert_eq!(mcv.values.len(), 3);
        assert_eq!(mcv.values[0], 1.0); // most common
        assert_eq!(mcv.counts[0], 3);
    }

    #[test]
    fn test_mcv_frequency() {
        let values = vec![1.0, 1.0, 1.0, 2.0, 2.0, 3.0];
        let mcv = MostCommonValues::from_values(&values, 3);

        let freq = mcv.frequency(1.0);
        assert!(freq > 0.4 && freq < 0.6); // 3/6 = 0.5
    }

    #[test]
    fn test_mcv_frequency_not_found() {
        let values = vec![1.0, 1.0, 1.0, 2.0, 2.0, 3.0];
        let mcv = MostCommonValues::from_values(&values, 1);

        // Only top-1 (which is 1.0 with count=3), so 3.0 is not in MCV
        assert_eq!(mcv.frequency(3.0), 0.0);
    }

    #[test]
    fn test_estimate_cardinality_with_mcv() {
        let values: Vec<f64> = (0..1000).map(|i| (i % 10) as f64).collect();
        let mcv = MostCommonValues::from_values(&values, 10);
        let hist = ColumnHistogram::from_values(&values, 5);

        let mut column_mcvs = HashMap::new();
        column_mcvs.insert("cat".to_string(), mcv);
        let mut column_histograms = HashMap::new();
        column_histograms.insert("cat".to_string(), hist);

        let stats = AdvancedTableStats {
            row_count: 1000,
            column_histograms,
            column_mcvs,
        };

        // Each category has 100 rows
        let card = estimate_cardinality(&stats, "cat", "=", 5.0);
        assert!(card > 50 && card < 150, "cardinality = {}", card);
    }

    #[test]
    fn test_estimate_cardinality_no_stats() {
        let stats = AdvancedTableStats {
            row_count: 1000,
            column_histograms: HashMap::new(),
            column_mcvs: HashMap::new(),
        };

        // No stats → default 33% selectivity
        let card = estimate_cardinality(&stats, "unknown_col", "=", 42.0);
        assert_eq!(card, 330); // 33% of 1000
    }

    #[test]
    fn test_histogram_skewed_data() {
        // Skewed: 90% value=1, 10% value=2
        let mut values = vec![1.0; 900];
        values.extend(vec![2.0; 100]);
        let hist = ColumnHistogram::from_values(&values, 2);

        let total: u64 = hist.buckets.iter().map(|b| b.row_count).sum();
        assert_eq!(total, 1000);
    }
}
