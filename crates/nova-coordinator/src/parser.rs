//! SQL Parser — wraps sqlparser-rs with Nova custom dialect support.

// TODO: Phase 1 Milestone 1.5 — integrate sqlparser-rs, support CREATE TABLE, INSERT, SELECT

/// Nova SQL parser with custom dialect extensions.
pub struct SqlParser;

impl SqlParser {
    pub fn new() -> Self {
        Self
    }

    // TODO: Phase 1 Milestone 1.5
    // pub fn parse(&self, sql: &str) -> Result<Statement> { ... }
    // Supports: CREATE DATABASE, CREATE TABLE, DROP TABLE, INSERT INTO, SELECT
    // Custom: AT(TIMESTAMP => ...), CREATE TABLE ... CLONE, CREATE STREAM
}

impl Default for SqlParser {
    fn default() -> Self {
        Self::new()
    }
}
