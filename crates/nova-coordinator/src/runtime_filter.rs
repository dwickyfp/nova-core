// Runtime Filter — Bloom filter pushdown for join acceleration.
//
// Architecture:
// 1. During hash join BUILD phase, construct Bloom filter from build-side keys
// 2. Push Bloom filter to PROBE side scan → pre-filter rows before join
// 3. Only rows that pass Bloom filter are sent to join → massive speedup
//
// Use case: SELECT * FROM fact JOIN dim ON fact.id = dim.id WHERE dim.region = 'US'
// - dim table is small (build side), fact is large (probe side)
// - Bloom filter built from dim.id values
// - Fact scan checks Bloom filter → skips non-matching rows before join

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Bloom filter for runtime join filtering.
///
/// False positive rate: ~1% with optimal k (hash functions) and m (bits).
/// False negatives: impossible (Bloom filter guarantee).
pub struct BloomFilter {
    bits: Vec<u64>,
    num_bits: usize,
    num_hashes: usize,
    num_items: usize,
}

impl BloomFilter {
    /// Create a new Bloom filter sized for `expected_items` with target FPR.
    pub fn new(expected_items: usize, target_fpr: f64) -> Self {
        // m = -n * ln(p) / (ln(2)^2)
        let m =
            (-(expected_items as f64) * target_fpr.ln() / (2.0_f64.ln().powi(2))).ceil() as usize;
        let num_bits = m.max(64);
        // k = (m/n) * ln(2)
        let num_hashes =
            (((num_bits as f64 / expected_items as f64) * 2.0_f64.ln()).ceil() as usize).max(1);

        let num_words = num_bits.div_ceil(64);
        Self {
            bits: vec![0u64; num_words],
            num_bits,
            num_hashes,
            num_items: 0,
        }
    }

    /// Insert a value into the Bloom filter.
    pub fn insert<T: Hash>(&mut self, item: &T) {
        let (h1, h2) = self.hash_pair(item);
        for i in 0..self.num_hashes {
            let bit_idx = (h1.wrapping_add(h2.wrapping_mul(i as u64))) as usize % self.num_bits;
            let word_idx = bit_idx / 64;
            let bit_pos = bit_idx % 64;
            self.bits[word_idx] |= 1u64 << bit_pos;
        }
        self.num_items += 1;
    }

    /// Check if a value MIGHT be in the set (false positive possible).
    pub fn contains<T: Hash>(&self, item: &T) -> bool {
        let (h1, h2) = self.hash_pair(item);
        for i in 0..self.num_hashes {
            let bit_idx = (h1.wrapping_add(h2.wrapping_mul(i as u64))) as usize % self.num_bits;
            let word_idx = bit_idx / 64;
            let bit_pos = bit_idx % 64;
            if self.bits[word_idx] & (1u64 << bit_pos) == 0 {
                return false; // definitely not in set
            }
        }
        true // might be in set (false positive possible)
    }

    /// Number of items inserted.
    pub fn len(&self) -> usize {
        self.num_items
    }

    pub fn is_empty(&self) -> bool {
        self.num_items == 0
    }

    /// Estimated false positive rate.
    pub fn estimated_fpr(&self) -> f64 {
        if self.num_items == 0 {
            return 0.0;
        }
        let k = self.num_hashes as f64;
        let n = self.num_items as f64;
        let m = self.num_bits as f64;
        (1.0 - (-k * n / m).exp()).powf(k)
    }

    /// Double hashing: use two independent hash values.
    fn hash_pair<T: Hash>(&self, item: &T) -> (u64, u64) {
        let mut hasher1 = DefaultHasher::new();
        item.hash(&mut hasher1);
        let h1 = hasher1.finish();

        let mut hasher2 = DefaultHasher::new();
        (h1, 0_u64).hash(&mut hasher2);
        let h2 = hasher2.finish().max(1); // h2 must be non-zero for double hashing

        (h1, h2)
    }
}

/// Runtime filter applied to a scan to pre-filter rows.
pub struct RuntimeFilter {
    bloom: BloomFilter,
    column_name: String,
}

impl RuntimeFilter {
    pub fn new(column_name: String, expected_items: usize, fpr: f64) -> Self {
        Self {
            bloom: BloomFilter::new(expected_items, fpr),
            column_name,
        }
    }

    pub fn insert<T: Hash>(&mut self, item: &T) {
        self.bloom.insert(item);
    }

    pub fn contains<T: Hash>(&self, item: &T) -> bool {
        self.bloom.contains(item)
    }

    pub fn column(&self) -> &str {
        &self.column_name
    }

    pub fn len(&self) -> usize {
        self.bloom.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bloom.is_empty()
    }

    pub fn estimated_fpr(&self) -> f64 {
        self.bloom.estimated_fpr()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_insert_contains() {
        let mut bf = BloomFilter::new(1000, 0.01);
        bf.insert(&42i64);
        bf.insert(&100i64);
        bf.insert(&999i64);

        assert!(bf.contains(&42i64));
        assert!(bf.contains(&100i64));
        assert!(bf.contains(&999i64));
    }

    #[test]
    fn test_bloom_false_negative_impossible() {
        let mut bf = BloomFilter::new(100, 0.01);
        for i in 0..50 {
            bf.insert(&i);
        }
        // All inserted items must be found (no false negatives)
        for i in 0..50 {
            assert!(bf.contains(&i), "false negative for {}", i);
        }
    }

    #[test]
    fn test_bloom_false_positive_rate() {
        let mut bf = BloomFilter::new(1000, 0.01);
        for i in 0..1000 {
            bf.insert(&i);
        }

        // Check 10000 non-inserted items
        let mut false_positives = 0;
        for i in 1000..11000 {
            if bf.contains(&i) {
                false_positives += 1;
            }
        }

        // FPR should be < 5% (allowing margin over target 1%)
        let fpr = false_positives as f64 / 10000.0;
        assert!(fpr < 0.05, "FPR too high: {:.4}", fpr);
    }

    #[test]
    fn test_bloom_empty() {
        let bf = BloomFilter::new(100, 0.01);
        assert!(bf.is_empty());
        assert!(!bf.contains(&1i64)); // empty filter contains nothing
    }

    #[test]
    fn test_bloon_len() {
        let mut bf = BloomFilter::new(100, 0.01);
        assert_eq!(bf.len(), 0);
        bf.insert(&1);
        bf.insert(&2);
        assert_eq!(bf.len(), 2);
    }

    #[test]
    fn test_bloom_estimated_fpr() {
        let mut bf = BloomFilter::new(1000, 0.01);
        for i in 0..1000 {
            bf.insert(&i);
        }
        let fpr = bf.estimated_fpr();
        assert!(fpr > 0.0 && fpr < 0.1, "FPR should be small, got {}", fpr);
    }

    #[test]
    fn test_runtime_filter() {
        let mut rf = RuntimeFilter::new("user_id".to_string(), 100, 0.01);
        rf.insert(&42i64);
        rf.insert(&100i64);

        assert!(rf.contains(&42i64));
        assert!(!rf.contains(&999i64));
        assert_eq!(rf.column(), "user_id");
        assert_eq!(rf.len(), 2);
    }

    #[test]
    fn test_bloom_string_values() {
        let mut bf = BloomFilter::new(100, 0.01);
        bf.insert(&"alice".to_string());
        bf.insert(&"bob".to_string());

        assert!(bf.contains(&"alice".to_string()));
        assert!(bf.contains(&"bob".to_string()));
        // "charlie" was not inserted
        // May or may not be a false positive, but likely not
    }

    #[test]
    fn test_bloom_large_capacity() {
        let mut bf = BloomFilter::new(1_000_000, 0.001);
        for i in 0..10_000 {
            bf.insert(&i);
        }
        // Check all inserted items are found
        for i in 0..10_000 {
            assert!(bf.contains(&i));
        }
    }
}
