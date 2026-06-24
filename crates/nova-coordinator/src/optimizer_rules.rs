// Late Materialization — optimizer rule to reduce I/O for selective queries.
//
// Phase 5.4: For SELECT with filter + projection:
// 1. Scan filter columns first → get matching row IDs
// 2. Scan projection columns ONLY for matching row IDs
//
// This avoids reading wide columns for rows that will be filtered out.
// Best for: SELECT name, address FROM users WHERE active = true
// (active is cheap to scan, address is expensive — only fetch for active users)

/// Late materialization plan — determines scan order.
#[derive(Debug, Clone)]
pub struct LateMaterializationPlan {
    /// Columns to scan first (filter columns).
    pub filter_columns: Vec<String>,
    /// Columns to scan second (only for matching rows).
    pub projection_columns: Vec<String>,
    /// Estimated selectivity of the filter (0.0–1.0).
    pub filter_selectivity: f64,
    /// Estimated I/O savings (0.0–1.0).
    pub estimated_savings: f64,
}

/// Analyze a query and decide if late materialization is beneficial.
///
/// Returns Some(plan) if beneficial, None if not.
pub fn plan_late_materialization(
    filter_columns: &[String],
    projection_columns: &[String],
    filter_selectivity: f64,
    column_sizes: &std::collections::HashMap<String, u64>,
) -> Option<LateMaterializationPlan> {
    if filter_columns.is_empty() || projection_columns.is_empty() {
        return None;
    }

    // Calculate I/O cost without late materialization (read ALL columns)
    let all_columns: Vec<&String> = filter_columns
        .iter()
        .chain(
            projection_columns
                .iter()
                .filter(|c| !filter_columns.contains(c)),
        )
        .collect();
    let total_io: u64 = all_columns
        .iter()
        .map(|c| *column_sizes.get(*c).unwrap_or(&0))
        .sum();

    // Calculate I/O cost with late materialization
    // Phase 1: read filter columns (full scan)
    let filter_io: u64 = filter_columns
        .iter()
        .map(|c| *column_sizes.get(c).unwrap_or(&0))
        .sum();

    // Phase 2: read projection columns (only for matching rows = selectivity * total)
    let projection_only: Vec<String> = projection_columns
        .iter()
        .filter(|c| !filter_columns.contains(c))
        .cloned()
        .collect();
    let projection_io: u64 = (projection_only
        .iter()
        .map(|c| *column_sizes.get(c).unwrap_or(&0))
        .sum::<u64>() as f64
        * filter_selectivity) as u64;

    let late_io = filter_io + projection_io;
    let savings = if total_io > 0 {
        1.0 - (late_io as f64 / total_io as f64)
    } else {
        0.0
    };

    // Only beneficial if savings > 20%
    if savings < 0.20 {
        return None;
    }

    Some(LateMaterializationPlan {
        filter_columns: filter_columns.to_vec(),
        projection_columns: projection_only,
        filter_selectivity,
        estimated_savings: savings,
    })
}

/// Dictionary encoding — optimize low-cardinality string columns.
///
/// Phase 5.5: Replace string values with INT codes for SIMD-friendly operations.
/// Only decode at output.
#[derive(Debug, Clone)]
pub struct DictionaryEncoding {
    /// Mapping from string value to integer code.
    pub dictionary: std::collections::HashMap<String, u32>,
    /// Reverse mapping: code → string value.
    pub reverse_dict: Vec<String>,
}

impl DictionaryEncoding {
    /// Build dictionary from a list of string values.
    pub fn build(values: &[String]) -> Self {
        let mut dictionary = std::collections::HashMap::new();
        let mut reverse_dict = Vec::new();

        for value in values {
            if !dictionary.contains_key(value) {
                let code = reverse_dict.len() as u32;
                dictionary.insert(value.clone(), code);
                reverse_dict.push(value.clone());
            }
        }

        Self {
            dictionary,
            reverse_dict,
        }
    }

    /// Encode a value to its integer code.
    pub fn encode(&self, value: &str) -> Option<u32> {
        self.dictionary.get(value).copied()
    }

    /// Decode an integer code back to string.
    pub fn decode(&self, code: u32) -> Option<&str> {
        self.reverse_dict.get(code as usize).map(|s| s.as_str())
    }

    /// Number of distinct values in dictionary.
    pub fn cardinality(&self) -> usize {
        self.reverse_dict.len()
    }

    /// Is dictionary encoding beneficial?
    /// Rule of thumb: beneficial when cardinality < 10% of row count.
    pub fn is_beneficial(cardinality: usize, row_count: usize) -> bool {
        row_count > 0 && cardinality < (row_count / 10)
    }

    /// Encode an entire column.
    pub fn encode_column(&self, values: &[String]) -> Vec<u32> {
        values
            .iter()
            .map(|v| self.encode(v).unwrap_or(u32::MAX))
            .collect()
    }

    /// Decode an entire column.
    pub fn decode_column(&self, codes: &[u32]) -> Vec<String> {
        codes
            .iter()
            .map(|c| self.decode(*c).unwrap_or("").to_string())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_late_mat_beneficial() {
        let mut sizes = HashMap::new();
        sizes.insert("active".to_string(), 100_u64); // small filter column
        sizes.insert("address".to_string(), 10_000_u64); // large projection column
        sizes.insert("name".to_string(), 1_000_u64);

        let plan = plan_late_materialization(
            &["active".to_string()],
            &["name".to_string(), "address".to_string()],
            0.01, // 1% selectivity
            &sizes,
        );

        assert!(plan.is_some());
        let plan = plan.unwrap();
        assert!(plan.estimated_savings > 0.5); // > 50% savings
        assert!(plan.projection_columns.contains(&"address".to_string()));
    }

    #[test]
    fn test_late_mat_not_beneficial_high_selectivity() {
        let mut sizes = HashMap::new();
        sizes.insert("active".to_string(), 100_u64);
        sizes.insert("name".to_string(), 1_000_u64);

        // 90% selectivity → almost all rows match → no benefit
        let plan =
            plan_late_materialization(&["active".to_string()], &["name".to_string()], 0.90, &sizes);

        assert!(plan.is_none());
    }

    #[test]
    fn test_late_mat_no_filter() {
        let sizes = HashMap::new();
        let plan = plan_late_materialization(&[], &["name".to_string()], 1.0, &sizes);
        assert!(plan.is_none());
    }

    #[test]
    fn test_late_mat_no_projection() {
        let sizes = HashMap::new();
        let plan = plan_late_materialization(&["active".to_string()], &[], 0.1, &sizes);
        assert!(plan.is_none());
    }

    #[test]
    fn test_late_mat_filter_is_projection() {
        let mut sizes = HashMap::new();
        sizes.insert("id".to_string(), 100_u64);

        // filter column is same as projection column
        let plan = plan_late_materialization(&["id".to_string()], &["id".to_string()], 0.1, &sizes);
        // No extra projection columns → projection_only is empty → no savings
        assert!(plan.is_none());
    }

    #[test]
    fn test_dict_encoding_basic() {
        let values = vec![
            "US".to_string(),
            "UK".to_string(),
            "US".to_string(),
            "JP".to_string(),
        ];
        let dict = DictionaryEncoding::build(&values);

        assert_eq!(dict.cardinality(), 3); // US, UK, JP
        assert_eq!(dict.encode("US"), Some(0));
        assert_eq!(dict.encode("UK"), Some(1));
        assert_eq!(dict.encode("JP"), Some(2));
        assert_eq!(dict.encode("DE"), None); // not in dictionary
    }

    #[test]
    fn test_dict_encoding_decode() {
        let values = vec!["US".to_string(), "UK".to_string(), "JP".to_string()];
        let dict = DictionaryEncoding::build(&values);

        assert_eq!(dict.decode(0), Some("US"));
        assert_eq!(dict.decode(1), Some("UK"));
        assert_eq!(dict.decode(2), Some("JP"));
        assert_eq!(dict.decode(99), None);
    }

    #[test]
    fn test_dict_encoding_column() {
        let values = vec!["US".to_string(), "UK".to_string(), "US".to_string()];
        let dict = DictionaryEncoding::build(&values);

        let encoded = dict.encode_column(&values);
        assert_eq!(encoded, vec![0, 1, 0]);

        let decoded = dict.decode_column(&encoded);
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_dict_encoding_beneficial() {
        // 5 distinct values in 1000 rows → 0.5% < 10% → beneficial
        assert!(DictionaryEncoding::is_beneficial(5, 1000));
    }

    #[test]
    fn test_dict_encoding_not_beneficial() {
        // 500 distinct values in 1000 rows → 50% > 10% → not beneficial
        assert!(!DictionaryEncoding::is_beneficial(500, 1000));
    }

    #[test]
    fn test_dict_encoding_empty() {
        let dict = DictionaryEncoding::build(&[]);
        assert_eq!(dict.cardinality(), 0);
    }

    #[test]
    fn test_dict_encoding_roundtrip() {
        let values: Vec<String> = (0..100).map(|i| format!("val_{}", i % 10)).collect();
        let dict = DictionaryEncoding::build(&values);
        let encoded = dict.encode_column(&values);
        let decoded = dict.decode_column(&encoded);
        assert_eq!(decoded, values);
    }
}
