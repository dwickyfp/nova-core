// MySQL column types
//
// Maps between MySQL wire protocol types and Arrow types

use arrow::datatypes::DataType as ArrowDataType;

/// MySQL column types (wire protocol)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ColumnType {
    Decimal = 0x00,
    Tiny = 0x01,
    Short = 0x02,
    Long = 0x03,
    Float = 0x04,
    Double = 0x05,
    Null = 0x06,
    Timestamp = 0x07,
    LongLong = 0x08,
    Int24 = 0x09,
    Date = 0x0a,
    Time = 0x0b,
    DateTime = 0x0c,
    Year = 0x0d,
    NewDate = 0x0e,
    Varchar = 0x0f,
    Bit = 0x10,
    Timestamp2 = 0x11,
    DateTime2 = 0x12,
    Time2 = 0x13,
    Json = 0xf5,
    NewDecimal = 0xf6,
    Enum = 0xf7,
    Set = 0xf8,
    TinyBlob = 0xf9,
    MediumBlob = 0xfa,
    LongBlob = 0xfb,
    Blob = 0xfc,
    VarString = 0xfd,
    String = 0xfe,
    Geometry = 0xff,
}

impl ColumnType {
    /// Convert from u8
    pub fn from_u8(val: u8) -> Option<Self> {
        match val {
            0x00 => Some(Self::Decimal),
            0x01 => Some(Self::Tiny),
            0x02 => Some(Self::Short),
            0x03 => Some(Self::Long),
            0x04 => Some(Self::Float),
            0x05 => Some(Self::Double),
            0x06 => Some(Self::Null),
            0x07 => Some(Self::Timestamp),
            0x08 => Some(Self::LongLong),
            0x09 => Some(Self::Int24),
            0x0a => Some(Self::Date),
            0x0b => Some(Self::Time),
            0x0c => Some(Self::DateTime),
            0x0d => Some(Self::Year),
            0x0e => Some(Self::NewDate),
            0x0f => Some(Self::Varchar),
            0x10 => Some(Self::Bit),
            0x11 => Some(Self::Timestamp2),
            0x12 => Some(Self::DateTime2),
            0x13 => Some(Self::Time2),
            0xf5 => Some(Self::Json),
            0xf6 => Some(Self::NewDecimal),
            0xf7 => Some(Self::Enum),
            0xf8 => Some(Self::Set),
            0xf9 => Some(Self::TinyBlob),
            0xfa => Some(Self::MediumBlob),
            0xfb => Some(Self::LongBlob),
            0xfc => Some(Self::Blob),
            0xfd => Some(Self::VarString),
            0xfe => Some(Self::String),
            0xff => Some(Self::Geometry),
            _ => None,
        }
    }

    /// Convert to u8
    pub fn to_u8(self) -> u8 {
        self as u8
    }

    /// Get type name
    pub fn name(self) -> &'static str {
        match self {
            Self::Decimal => "DECIMAL",
            Self::Tiny => "TINY",
            Self::Short => "SHORT",
            Self::Long => "LONG",
            Self::Float => "FLOAT",
            Self::Double => "DOUBLE",
            Self::Null => "NULL",
            Self::Timestamp => "TIMESTAMP",
            Self::LongLong => "LONGLONG",
            Self::Int24 => "INT24",
            Self::Date => "DATE",
            Self::Time => "TIME",
            Self::DateTime => "DATETIME",
            Self::Year => "YEAR",
            Self::NewDate => "NEWDATE",
            Self::Varchar => "VARCHAR",
            Self::Bit => "BIT",
            Self::Timestamp2 => "TIMESTAMP2",
            Self::DateTime2 => "DATETIME2",
            Self::Time2 => "TIME2",
            Self::Json => "JSON",
            Self::NewDecimal => "NEWDECIMAL",
            Self::Enum => "ENUM",
            Self::Set => "SET",
            Self::TinyBlob => "TINYBLOB",
            Self::MediumBlob => "MEDIUMBLOB",
            Self::LongBlob => "LONGBLOB",
            Self::Blob => "BLOB",
            Self::VarString => "VARSTRING",
            Self::String => "STRING",
            Self::Geometry => "GEOMETRY",
        }
    }

    /// Check if this is a numeric type
    pub fn is_numeric(self) -> bool {
        matches!(
            self,
            Self::Decimal
                | Self::Tiny
                | Self::Short
                | Self::Long
                | Self::Float
                | Self::Double
                | Self::LongLong
                | Self::Int24
                | Self::Year
                | Self::NewDecimal
        )
    }

    /// Check if this is a string type
    pub fn is_string(self) -> bool {
        matches!(
            self,
            Self::Varchar
                | Self::VarString
                | Self::String
                | Self::Blob
                | Self::TinyBlob
                | Self::MediumBlob
                | Self::LongBlob
        )
    }

    /// Check if this is a temporal type
    pub fn is_temporal(self) -> bool {
        matches!(
            self,
            Self::Date
                | Self::Time
                | Self::DateTime
                | Self::Timestamp
                | Self::Year
                | Self::NewDate
                | Self::Timestamp2
                | Self::DateTime2
                | Self::Time2
        )
    }
}

/// Column flags
pub struct ColumnFlags;

impl ColumnFlags {
    pub const NOT_NULL: u16 = 0x0001;
    pub const PRIMARY_KEY: u16 = 0x0002;
    pub const UNIQUE_KEY: u16 = 0x0004;
    pub const MULTIPLE_KEY: u16 = 0x0008;
    pub const BLOB: u16 = 0x0010;
    pub const UNSIGNED: u16 = 0x0020;
    pub const ZEROFILL: u16 = 0x0040;
    pub const BINARY: u16 = 0x0080;
    pub const ENUM: u16 = 0x0100;
    pub const AUTO_INCREMENT: u16 = 0x0200;
    pub const TIMESTAMP: u16 = 0x0400;
    pub const SET: u16 = 0x0800;
    pub const NO_DEFAULT_VALUE: u16 = 0x1000;
    pub const ON_UPDATE_NOW: u16 = 0x2000;
    pub const NUM: u16 = 0x8000;
}

/// Convert Arrow DataType to MySQL ColumnType
pub fn arrow_to_mysql(data_type: &ArrowDataType) -> ColumnType {
    match data_type {
        ArrowDataType::Null => ColumnType::Null,
        ArrowDataType::Boolean => ColumnType::Tiny,
        ArrowDataType::Int8 => ColumnType::Tiny,
        ArrowDataType::Int16 => ColumnType::Short,
        ArrowDataType::Int32 => ColumnType::Long,
        ArrowDataType::Int64 => ColumnType::LongLong,
        ArrowDataType::UInt8 => ColumnType::Tiny,
        ArrowDataType::UInt16 => ColumnType::Short,
        ArrowDataType::UInt32 => ColumnType::Long,
        ArrowDataType::UInt64 => ColumnType::LongLong,
        ArrowDataType::Float16 => ColumnType::Float,
        ArrowDataType::Float32 => ColumnType::Float,
        ArrowDataType::Float64 => ColumnType::Double,
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => ColumnType::VarString,
        ArrowDataType::Binary | ArrowDataType::LargeBinary => ColumnType::Blob,
        ArrowDataType::Date32 | ArrowDataType::Date64 => ColumnType::Date,
        ArrowDataType::Time32(_) | ArrowDataType::Time64(_) => ColumnType::Time,
        ArrowDataType::Timestamp(_, _) => ColumnType::DateTime,
        ArrowDataType::Decimal128(_, _) | ArrowDataType::Decimal256(_, _) => ColumnType::NewDecimal,
        _ => ColumnType::VarString, // Default fallback
    }
}

/// Get column length for MySQL type
pub fn column_length(col_type: ColumnType) -> u32 {
    match col_type {
        ColumnType::Tiny => 4,
        ColumnType::Short => 6,
        ColumnType::Long => 11,
        ColumnType::LongLong => 20,
        ColumnType::Float => 12,
        ColumnType::Double => 22,
        ColumnType::Date => 10,
        ColumnType::Time => 10,
        ColumnType::DateTime | ColumnType::Timestamp => 19,
        ColumnType::Year => 4,
        ColumnType::VarString | ColumnType::String => 255,
        ColumnType::Blob => 65535,
        ColumnType::TinyBlob => 255,
        ColumnType::MediumBlob => 16777215,
        ColumnType::LongBlob => 4294967295,
        _ => 255,
    }
}

/// Get decimals (precision) for MySQL type
pub fn column_decimals(col_type: ColumnType) -> u8 {
    match col_type {
        ColumnType::Float | ColumnType::Double | ColumnType::NewDecimal => 31,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_type_conversion() {
        assert_eq!(ColumnType::from_u8(0x03), Some(ColumnType::Long));
        assert_eq!(ColumnType::Long.to_u8(), 0x03);
    }

    #[test]
    fn test_column_type_properties() {
        assert!(ColumnType::Long.is_numeric());
        assert!(!ColumnType::Long.is_string());
        assert!(ColumnType::VarString.is_string());
        assert!(ColumnType::DateTime.is_temporal());
    }

    #[test]
    fn test_arrow_to_mysql() {
        assert_eq!(arrow_to_mysql(&ArrowDataType::Int32), ColumnType::Long);
        assert_eq!(arrow_to_mysql(&ArrowDataType::Utf8), ColumnType::VarString);
        assert_eq!(arrow_to_mysql(&ArrowDataType::Float64), ColumnType::Double);
    }
}
