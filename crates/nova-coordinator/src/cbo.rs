// CBO — Cost-Based Optimizer: join reordering + cost model.
//
// Architecture (System-R style):
// 1. Enumerate all possible join orders (bushy plans, not just left-deep)
// 2. For each join order, estimate cost using statistics
// 3. Select the plan with lowest estimated cost
//
// Cost model:
// - Cost = CPU cost + I/O cost
// - CPU cost: proportional to cardinality of join input/output
// - I/O cost: proportional to bytes read from storage
// - Join types: Hash (best for equi-join), Sort-Merge (ordered), Nested-Loop (fallback)

use std::collections::HashMap;

/// Table reference in a join graph.
#[derive(Debug, Clone)]
pub struct TableRef {
    pub table_id: u64,
    pub name: String,
    pub row_count: u64,
    pub byte_size: u64,
    pub columns: Vec<String>,
}

/// Join edge between two tables.
#[derive(Debug, Clone)]
pub struct JoinEdge {
    pub left_table: String,
    pub right_table: String,
    pub left_col: String,
    pub right_col: String,
}

/// Join physical operator type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    /// Hash join: build hash table on smaller side, probe with larger.
    Hash,
    /// Sort-merge join: both sides sorted, merge.
    SortMerge,
    /// Nested-loop join: brute force (fallback for non-equi joins).
    NestedLoop,
}

/// Estimated cost of a physical plan.
#[derive(Debug, Clone, Copy)]
pub struct PlanCost {
    pub cpu_cost: f64,
    pub io_cost: f64,
    pub cardinality: u64,
}

impl PlanCost {
    pub fn total(&self) -> f64 {
        self.cpu_cost + self.io_cost
    }
}

/// Join plan node — represents a single join in the plan tree.
#[derive(Debug, Clone)]
pub enum JoinPlan {
    /// Leaf: scan a single table.
    Scan { table: TableRef, cost: PlanCost },
    /// Internal: join two sub-plans.
    Join {
        left: Box<JoinPlan>,
        right: Box<JoinPlan>,
        join_type: JoinType,
        join_cols: (String, String),
        cost: PlanCost,
    },
}

impl JoinPlan {
    /// Get estimated cardinality of this plan node's output.
    pub fn cardinality(&self) -> u64 {
        match self {
            JoinPlan::Scan { cost, .. } => cost.cardinality,
            JoinPlan::Join { cost, .. } => cost.cardinality,
        }
    }

    /// Get total cost of this plan.
    pub fn total_cost(&self) -> f64 {
        match self {
            JoinPlan::Scan { cost, .. } => cost.total(),
            JoinPlan::Join { cost, .. } => cost.total(),
        }
    }

    /// Pretty-print the plan tree.
    pub fn display(&self, indent: usize) -> String {
        let pad = "  ".repeat(indent);
        match self {
            JoinPlan::Scan { table, cost } => {
                format!(
                    "{pad}Scan({}) rows={} cost={:.0}",
                    table.name,
                    cost.cardinality,
                    cost.total()
                )
            }
            JoinPlan::Join {
                left,
                right,
                join_type,
                join_cols,
                cost,
            } => {
                format!(
                    "{pad}Join({:?} on {}={}) rows={} cost={:.0}\n{}\n{}",
                    join_type,
                    join_cols.0,
                    join_cols.1,
                    cost.cardinality,
                    cost.total(),
                    left.display(indent + 1),
                    right.display(indent + 1),
                )
            }
        }
    }
}

/// CBO optimizer — finds optimal join order via dynamic programming.
pub struct CboOptimizer {
    /// Filter selectivity (0.0–1.0) per table, if any.
    filters: HashMap<String, f64>,
}

impl CboOptimizer {
    pub fn new() -> Self {
        Self {
            filters: HashMap::new(),
        }
    }

    /// Set filter selectivity for a table (e.g., 0.1 = 10% of rows match).
    pub fn set_filter(&mut self, table_name: &str, selectivity: f64) {
        self.filters.insert(table_name.to_string(), selectivity);
    }

    /// Optimize a multi-table join using dynamic programming.
    ///
    /// Returns the lowest-cost join plan.
    pub fn optimize(&self, tables: &[TableRef], edges: &[JoinEdge]) -> Option<JoinPlan> {
        if tables.is_empty() {
            return None;
        }
        if tables.len() == 1 {
            return Some(self.make_scan(&tables[0]));
        }

        // DP: best plan for each subset of tables
        // Key = bitmask of table indices
        let n = tables.len();
        let mut best_plans: HashMap<u32, JoinPlan> = HashMap::new();

        // Base case: single-table scans
        for (i, table) in tables.iter().enumerate() {
            let mask = 1u32 << i;
            best_plans.insert(mask, self.make_scan(table));
        }

        // Build case: try all subsets of increasing size
        for size in 2..=n {
            for subset in self.subsets(n, size) {
                let mut best_cost = f64::MAX;
                let mut best_plan = None;

                // Try all ways to split subset into left ⊕ right
                for &left_mask in self.submasks(subset).iter() {
                    if left_mask == 0 || left_mask == subset {
                        continue;
                    }
                    let right_mask = subset & !left_mask;

                    if let (Some(left_plan), Some(right_plan)) =
                        (best_plans.get(&left_mask), best_plans.get(&right_mask))
                    {
                        // Find join edge between left and right subsets
                        if let Some((edge, join_type)) =
                            self.find_join_edge(left_plan, right_plan, edges, tables)
                        {
                            let cost =
                                self.estimate_join_cost(left_plan, right_plan, &edge, join_type);
                            let total = cost.total();

                            if total < best_cost {
                                best_cost = total;
                                best_plan = Some(JoinPlan::Join {
                                    left: Box::new(left_plan.clone()),
                                    right: Box::new(right_plan.clone()),
                                    join_type,
                                    join_cols: (edge.left_col.clone(), edge.right_col.clone()),
                                    cost,
                                });
                            }
                        }
                    }
                }

                if let Some(plan) = best_plan {
                    best_plans.insert(subset, plan);
                }
            }
        }

        // Return plan for full set (all bits set)
        let full_mask = (1u32 << n) - 1;
        best_plans.get(&full_mask).cloned()
    }

    /// Create a scan plan for a single table.
    fn make_scan(&self, table: &TableRef) -> JoinPlan {
        let effective_rows = if let Some(sel) = self.filters.get(&table.name) {
            (table.row_count as f64 * sel) as u64
        } else {
            table.row_count
        };

        JoinPlan::Scan {
            table: table.clone(),
            cost: PlanCost {
                cpu_cost: 0.0,
                io_cost: table.byte_size as f64,
                cardinality: effective_rows,
            },
        }
    }

    /// Estimate cost of joining two sub-plans.
    fn estimate_join_cost(
        &self,
        left: &JoinPlan,
        right: &JoinPlan,
        _edge: &JoinEdge,
        join_type: JoinType,
    ) -> PlanCost {
        let left_rows = left.cardinality();
        let right_rows = right.cardinality();

        // Cardinality estimation: |R| × |S| / max(NDV(left), NDV(right))
        // ponytail: NDV (number of distinct values) not tracked yet.
        // Assume 10% selectivity for equi-join.
        let join_cardinality = (left_rows as f64 * right_rows as f64 * 0.1) as u64;

        let (cpu_cost, io_cost) = match join_type {
            JoinType::Hash => {
                // Build hash table on smaller side, probe with larger
                let build = left_rows.min(right_rows) as f64;
                let probe = left_rows.max(right_rows) as f64;
                (build + probe, 0.0)
            }
            JoinType::SortMerge => {
                // Both sides need sorting + merge
                let sort_left = (left_rows as f64) * (left_rows as f64).log2().max(1.0);
                let sort_right = (right_rows as f64) * (right_rows as f64).log2().max(1.0);
                (
                    sort_left + sort_right + left_rows as f64 + right_rows as f64,
                    0.0,
                )
            }
            JoinType::NestedLoop => {
                // Brute force: left × right
                (left_rows as f64 * right_rows as f64, 0.0)
            }
        };

        PlanCost {
            cpu_cost,
            io_cost,
            cardinality: join_cardinality,
        }
    }

    /// Find a join edge connecting two plan subsets.
    fn find_join_edge(
        &self,
        left: &JoinPlan,
        right: &JoinPlan,
        edges: &[JoinEdge],
        tables: &[TableRef],
    ) -> Option<(JoinEdge, JoinType)> {
        let left_tables = self.plan_tables(left, tables);
        let right_tables = self.plan_tables(right, tables);

        for edge in edges {
            let left_in_left = left_tables.contains(&edge.left_table);
            let right_in_right = right_tables.contains(&edge.right_table);
            let left_in_right = left_tables.contains(&edge.right_table);
            let right_in_left = right_tables.contains(&edge.left_table);

            if (left_in_left && right_in_right) || (left_in_right && right_in_left) {
                // Select join type: Hash for equi-join (default)
                let join_type = JoinType::Hash;
                return Some((edge.clone(), join_type));
            }
        }
        None
    }

    /// Get all table names in a plan subtree.
    #[allow(clippy::only_used_in_recursion)]
    fn plan_tables(&self, plan: &JoinPlan, _tables: &[TableRef]) -> Vec<String> {
        match plan {
            JoinPlan::Scan { table, .. } => vec![table.name.clone()],
            JoinPlan::Join { left, right, .. } => {
                let mut names = self.plan_tables(left, _tables);
                names.extend(self.plan_tables(right, _tables));
                names
            }
        }
    }

    /// Generate all subsets of size `k` from `n` items (as bitmasks).
    fn subsets(&self, n: usize, k: usize) -> Vec<u32> {
        let mut result = Vec::new();
        self.subsets_recursive(n, k, 0, 0, &mut result);
        result
    }

    #[allow(clippy::only_used_in_recursion)]
    fn subsets_recursive(
        &self,
        n: usize,
        k: usize,
        start: usize,
        mask: u32,
        result: &mut Vec<u32>,
    ) {
        if k == 0 {
            result.push(mask);
            return;
        }
        for i in start..n {
            self.subsets_recursive(n, k - 1, i + 1, mask | (1 << i), result);
        }
    }

    /// Generate all proper submasks of a bitmask.
    fn submasks(&self, mask: u32) -> Vec<u32> {
        let mut result = Vec::new();
        let mut sub = (mask - 1) & mask;
        while sub > 0 {
            result.push(sub);
            sub = (sub - 1) & mask;
        }
        result
    }
}

impl Default for CboOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mock_table(name: &str, rows: u64, bytes: u64) -> TableRef {
        TableRef {
            table_id: {
                use std::hash::{Hash, Hasher};
                let mut h = std::collections::hash_map::DefaultHasher::new();
                name.hash(&mut h);
                h.finish()
            },
            name: name.to_string(),
            row_count: rows,
            byte_size: bytes,
            columns: vec!["id".to_string()],
        }
    }

    #[test]
    fn test_single_table_no_join() {
        let optimizer = CboOptimizer::new();
        let tables = vec![mock_table("t1", 1000, 1024)];
        let plan = optimizer.optimize(&tables, &[]).unwrap();

        assert_eq!(plan.cardinality(), 1000);
        match plan {
            JoinPlan::Scan { table, .. } => assert_eq!(table.name, "t1"),
            _ => panic!("expected Scan"),
        }
    }

    #[test]
    fn test_two_table_join() {
        let optimizer = CboOptimizer::new();
        let tables = vec![
            mock_table("orders", 1_000_000, 100_000_000),
            mock_table("customers", 100_000, 10_000_000),
        ];
        let edges = vec![JoinEdge {
            left_table: "orders".to_string(),
            right_table: "customers".to_string(),
            left_col: "customer_id".to_string(),
            right_col: "id".to_string(),
        }];

        let plan = optimizer.optimize(&tables, &edges).unwrap();
        match plan {
            JoinPlan::Join {
                join_type, cost, ..
            } => {
                assert_eq!(join_type, JoinType::Hash);
                assert!(cost.cardinality > 0);
            }
            _ => panic!("expected Join"),
        }
    }

    #[test]
    fn test_three_table_join_reorder() {
        let optimizer = CboOptimizer::new();
        let tables = vec![
            mock_table("orders", 1_000_000, 100_000_000),
            mock_table("customers", 100_000, 10_000_000),
            mock_table("products", 10_000, 1_000_000),
        ];
        let edges = vec![
            JoinEdge {
                left_table: "orders".to_string(),
                right_table: "customers".to_string(),
                left_col: "customer_id".to_string(),
                right_col: "id".to_string(),
            },
            JoinEdge {
                left_table: "orders".to_string(),
                right_table: "products".to_string(),
                left_col: "product_id".to_string(),
                right_col: "id".to_string(),
            },
        ];

        let plan = optimizer.optimize(&tables, &edges).unwrap();
        // Should be a Join with Join children
        assert!(matches!(plan, JoinPlan::Join { .. }));
        // Cost should be finite and positive
        assert!(plan.total_cost() > 0.0);
    }

    #[test]
    fn test_filter_selectivity_applied() {
        let mut optimizer = CboOptimizer::new();
        optimizer.set_filter("orders", 0.01); // 1% selectivity

        let tables = vec![mock_table("orders", 1_000_000, 100_000_000)];
        let plan = optimizer.optimize(&tables, &[]).unwrap();

        // Cardinality should be 1% of 1M = 10K
        assert_eq!(plan.cardinality(), 10_000);
    }

    #[test]
    fn test_star_schema_5_tables() {
        let optimizer = CboOptimizer::new();
        let tables = vec![
            mock_table("fact", 10_000_000, 1_000_000_000),
            mock_table("dim1", 1_000, 100_000),
            mock_table("dim2", 5_000, 500_000),
            mock_table("dim3", 10_000, 1_000_000),
            mock_table("dim4", 500, 50_000),
        ];
        let edges = vec![
            JoinEdge {
                left_table: "fact".to_string(),
                right_table: "dim1".to_string(),
                left_col: "d1_id".to_string(),
                right_col: "id".to_string(),
            },
            JoinEdge {
                left_table: "fact".to_string(),
                right_table: "dim2".to_string(),
                left_col: "d2_id".to_string(),
                right_col: "id".to_string(),
            },
            JoinEdge {
                left_table: "fact".to_string(),
                right_table: "dim3".to_string(),
                left_col: "d3_id".to_string(),
                right_col: "id".to_string(),
            },
            JoinEdge {
                left_table: "fact".to_string(),
                right_table: "dim4".to_string(),
                left_col: "d4_id".to_string(),
                right_col: "id".to_string(),
            },
        ];

        let plan = optimizer.optimize(&tables, &edges).unwrap();
        assert!(matches!(plan, JoinPlan::Join { .. }));
        // Print for visual inspection
        println!("{}", plan.display(0));
    }

    #[test]
    fn test_hash_vs_sortmerge_cost() {
        let optimizer = CboOptimizer::new();
        let left = JoinPlan::Scan {
            table: mock_table("a", 1000, 1000),
            cost: PlanCost {
                cpu_cost: 0.0,
                io_cost: 1000.0,
                cardinality: 1000,
            },
        };
        let right = JoinPlan::Scan {
            table: mock_table("b", 1000, 1000),
            cost: PlanCost {
                cpu_cost: 0.0,
                io_cost: 1000.0,
                cardinality: 1000,
            },
        };
        let edge = JoinEdge {
            left_table: "a".to_string(),
            right_table: "b".to_string(),
            left_col: "id".to_string(),
            right_col: "a_id".to_string(),
        };

        let hash_cost = optimizer.estimate_join_cost(&left, &right, &edge, JoinType::Hash);
        let sm_cost = optimizer.estimate_join_cost(&left, &right, &edge, JoinType::SortMerge);

        // Hash should be cheaper for small tables
        assert!(hash_cost.total() <= sm_cost.total());
    }

    #[test]
    fn test_nested_loop_most_expensive() {
        let optimizer = CboOptimizer::new();
        let left = JoinPlan::Scan {
            table: mock_table("a", 10000, 10000),
            cost: PlanCost {
                cpu_cost: 0.0,
                io_cost: 10000.0,
                cardinality: 10000,
            },
        };
        let right = JoinPlan::Scan {
            table: mock_table("b", 10000, 10000),
            cost: PlanCost {
                cpu_cost: 0.0,
                io_cost: 10000.0,
                cardinality: 10000,
            },
        };
        let edge = JoinEdge {
            left_table: "a".to_string(),
            right_table: "b".to_string(),
            left_col: "id".to_string(),
            right_col: "a_id".to_string(),
        };

        let nl_cost = optimizer.estimate_join_cost(&left, &right, &edge, JoinType::NestedLoop);
        let hash_cost = optimizer.estimate_join_cost(&left, &right, &edge, JoinType::Hash);

        // Nested-loop should be much more expensive
        assert!(nl_cost.total() > hash_cost.total());
    }

    #[test]
    fn test_empty_tables() {
        let optimizer = CboOptimizer::new();
        assert!(optimizer.optimize(&[], &[]).is_none());
    }

    #[test]
    fn test_plan_display() {
        let optimizer = CboOptimizer::new();
        let tables = vec![mock_table("a", 100, 1024), mock_table("b", 200, 2048)];
        let edges = vec![JoinEdge {
            left_table: "a".to_string(),
            right_table: "b".to_string(),
            left_col: "id".to_string(),
            right_col: "a_id".to_string(),
        }];

        let plan = optimizer.optimize(&tables, &edges).unwrap();
        let display = plan.display(0);
        assert!(display.contains("Join"));
        assert!(display.contains("Scan"));
    }
}
