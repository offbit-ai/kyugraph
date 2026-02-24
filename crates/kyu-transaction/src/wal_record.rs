//! WAL record types and binary serialization.
//!
//! Format: `[type: u8][payload_len: u32 LE][payload bytes]`
//! All multi-byte integers are little-endian.

use kyu_common::id::TableId;

/// WAL record type discriminant.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalRecordType {
    Invalid = 0,
    BeginTransaction = 1,
    Commit = 2,
    CreateCatalogEntry = 14,
    DropCatalogEntry = 16,
    CatalogSnapshot = 15,
    AlterTableEntry = 17,
    TableInsertion = 30,
    NodeDeletion = 31,
    NodeUpdate = 32,
    RelDeletion = 33,
    RelUpdate = 35,
    Checkpoint = 254,
}

impl WalRecordType {
    fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(Self::Invalid),
            1 => Some(Self::BeginTransaction),
            2 => Some(Self::Commit),
            14 => Some(Self::CreateCatalogEntry),
            15 => Some(Self::CatalogSnapshot),
            16 => Some(Self::DropCatalogEntry),
            17 => Some(Self::AlterTableEntry),
            30 => Some(Self::TableInsertion),
            31 => Some(Self::NodeDeletion),
            32 => Some(Self::NodeUpdate),
            33 => Some(Self::RelDeletion),
            35 => Some(Self::RelUpdate),
            254 => Some(Self::Checkpoint),
            _ => None,
        }
    }
}

/// A single WAL record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WalRecord {
    BeginTransaction,
    Commit,
    Checkpoint,
    TableInsertion {
        table_id: TableId,
        num_rows: u64,
    },
    NodeDeletion {
        table_id: TableId,
        node_offset: u64,
    },
    NodeUpdate {
        table_id: TableId,
        column_id: u32,
        node_offset: u64,
    },
    RelDeletion {
        table_id: TableId,
        src_offset: u64,
        dst_offset: u64,
        rel_offset: u64,
    },
    RelUpdate {
        table_id: TableId,
        column_id: u32,
        src_offset: u64,
        dst_offset: u64,
        rel_offset: u64,
    },
    CreateCatalogEntry {
        entry_type: u8,
        table_id: TableId,
    },
    DropCatalogEntry {
        entry_type: u8,
        entry_id: u64,
    },
    AlterTableEntry {
        table_id: TableId,
    },
    /// Full catalog state snapshot (JSON). Used for DDL recovery.
    CatalogSnapshot {
        json_bytes: Vec<u8>,
    },
}

impl WalRecord {
    /// Serialize this record into the given byte buffer.
    /// Format: [type: u8][payload_len: u32 LE][payload bytes]
    pub fn serialize(&self, buf: &mut Vec<u8>) {
        let type_byte = self.record_type() as u8;
        buf.push(type_byte);

        // Reserve space for payload length.
        let len_pos = buf.len();
        buf.extend_from_slice(&[0u8; 4]);

        let payload_start = buf.len();

        match self {
            Self::BeginTransaction | Self::Commit | Self::Checkpoint => {}
            Self::TableInsertion { table_id, num_rows } => {
                buf.extend_from_slice(&table_id.0.to_le_bytes());
                buf.extend_from_slice(&num_rows.to_le_bytes());
            }
            Self::NodeDeletion {
                table_id,
                node_offset,
            } => {
                buf.extend_from_slice(&table_id.0.to_le_bytes());
                buf.extend_from_slice(&node_offset.to_le_bytes());
            }
            Self::NodeUpdate {
                table_id,
                column_id,
                node_offset,
            } => {
                buf.extend_from_slice(&table_id.0.to_le_bytes());
                buf.extend_from_slice(&column_id.to_le_bytes());
                buf.extend_from_slice(&node_offset.to_le_bytes());
            }
            Self::RelDeletion {
                table_id,
                src_offset,
                dst_offset,
                rel_offset,
            } => {
                buf.extend_from_slice(&table_id.0.to_le_bytes());
                buf.extend_from_slice(&src_offset.to_le_bytes());
                buf.extend_from_slice(&dst_offset.to_le_bytes());
                buf.extend_from_slice(&rel_offset.to_le_bytes());
            }
            Self::RelUpdate {
                table_id,
                column_id,
                src_offset,
                dst_offset,
                rel_offset,
            } => {
                buf.extend_from_slice(&table_id.0.to_le_bytes());
                buf.extend_from_slice(&column_id.to_le_bytes());
                buf.extend_from_slice(&src_offset.to_le_bytes());
                buf.extend_from_slice(&dst_offset.to_le_bytes());
                buf.extend_from_slice(&rel_offset.to_le_bytes());
            }
            Self::CreateCatalogEntry {
                entry_type,
                table_id,
            } => {
                buf.push(*entry_type);
                buf.extend_from_slice(&table_id.0.to_le_bytes());
            }
            Self::DropCatalogEntry {
                entry_type,
                entry_id,
            } => {
                buf.push(*entry_type);
                buf.extend_from_slice(&entry_id.to_le_bytes());
            }
            Self::AlterTableEntry { table_id } => {
                buf.extend_from_slice(&table_id.0.to_le_bytes());
            }
            Self::CatalogSnapshot { json_bytes } => {
                buf.extend_from_slice(&(json_bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(json_bytes);
            }
        }

        let payload_len = (buf.len() - payload_start) as u32;
        buf[len_pos..len_pos + 4].copy_from_slice(&payload_len.to_le_bytes());
    }

    /// Deserialize a record from a byte slice.
    /// Returns the record and the number of bytes consumed.
    pub fn deserialize(data: &[u8]) -> Result<(Self, usize), WalDeserializeError> {
        if data.is_empty() {
            return Err(WalDeserializeError::UnexpectedEof);
        }

        let type_byte = data[0];
        let record_type = WalRecordType::from_byte(type_byte)
            .ok_or(WalDeserializeError::InvalidType(type_byte))?;

        if data.len() < 5 {
            return Err(WalDeserializeError::UnexpectedEof);
        }
        let payload_len = u32::from_le_bytes(data[1..5].try_into().unwrap()) as usize;
        let total_len = 5 + payload_len;

        if data.len() < total_len {
            return Err(WalDeserializeError::UnexpectedEof);
        }

        let payload = &data[5..total_len];
        let record = Self::deserialize_payload(record_type, payload)?;
        Ok((record, total_len))
    }

    fn deserialize_payload(
        record_type: WalRecordType,
        payload: &[u8],
    ) -> Result<Self, WalDeserializeError> {
        match record_type {
            WalRecordType::BeginTransaction => Ok(Self::BeginTransaction),
            WalRecordType::Commit => Ok(Self::Commit),
            WalRecordType::Checkpoint => Ok(Self::Checkpoint),
            WalRecordType::Invalid => Err(WalDeserializeError::InvalidType(0)),
            WalRecordType::TableInsertion => {
                ensure_len(payload, 16)?;
                Ok(Self::TableInsertion {
                    table_id: TableId(read_u64_le(payload, 0)),
                    num_rows: read_u64_le(payload, 8),
                })
            }
            WalRecordType::NodeDeletion => {
                ensure_len(payload, 16)?;
                Ok(Self::NodeDeletion {
                    table_id: TableId(read_u64_le(payload, 0)),
                    node_offset: read_u64_le(payload, 8),
                })
            }
            WalRecordType::NodeUpdate => {
                ensure_len(payload, 20)?;
                Ok(Self::NodeUpdate {
                    table_id: TableId(read_u64_le(payload, 0)),
                    column_id: read_u32_le(payload, 8),
                    node_offset: read_u64_le(payload, 12),
                })
            }
            WalRecordType::RelDeletion => {
                ensure_len(payload, 32)?;
                Ok(Self::RelDeletion {
                    table_id: TableId(read_u64_le(payload, 0)),
                    src_offset: read_u64_le(payload, 8),
                    dst_offset: read_u64_le(payload, 16),
                    rel_offset: read_u64_le(payload, 24),
                })
            }
            WalRecordType::RelUpdate => {
                ensure_len(payload, 36)?;
                Ok(Self::RelUpdate {
                    table_id: TableId(read_u64_le(payload, 0)),
                    column_id: read_u32_le(payload, 8),
                    src_offset: read_u64_le(payload, 12),
                    dst_offset: read_u64_le(payload, 20),
                    rel_offset: read_u64_le(payload, 28),
                })
            }
            WalRecordType::CreateCatalogEntry => {
                ensure_len(payload, 9)?;
                Ok(Self::CreateCatalogEntry {
                    entry_type: payload[0],
                    table_id: TableId(read_u64_le(payload, 1)),
                })
            }
            WalRecordType::DropCatalogEntry => {
                ensure_len(payload, 9)?;
                Ok(Self::DropCatalogEntry {
                    entry_type: payload[0],
                    entry_id: read_u64_le(payload, 1),
                })
            }
            WalRecordType::AlterTableEntry => {
                ensure_len(payload, 8)?;
                Ok(Self::AlterTableEntry {
                    table_id: TableId(read_u64_le(payload, 0)),
                })
            }
            WalRecordType::CatalogSnapshot => {
                ensure_len(payload, 4)?;
                let len = read_u32_le(payload, 0) as usize;
                ensure_len(payload, 4 + len)?;
                Ok(Self::CatalogSnapshot {
                    json_bytes: payload[4..4 + len].to_vec(),
                })
            }
        }
    }

    fn record_type(&self) -> WalRecordType {
        match self {
            Self::BeginTransaction => WalRecordType::BeginTransaction,
            Self::Commit => WalRecordType::Commit,
            Self::Checkpoint => WalRecordType::Checkpoint,
            Self::TableInsertion { .. } => WalRecordType::TableInsertion,
            Self::NodeDeletion { .. } => WalRecordType::NodeDeletion,
            Self::NodeUpdate { .. } => WalRecordType::NodeUpdate,
            Self::RelDeletion { .. } => WalRecordType::RelDeletion,
            Self::RelUpdate { .. } => WalRecordType::RelUpdate,
            Self::CreateCatalogEntry { .. } => WalRecordType::CreateCatalogEntry,
            Self::DropCatalogEntry { .. } => WalRecordType::DropCatalogEntry,
            Self::AlterTableEntry { .. } => WalRecordType::AlterTableEntry,
            Self::CatalogSnapshot { .. } => WalRecordType::CatalogSnapshot,
        }
    }
}

/// WAL deserialization error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalDeserializeError {
    UnexpectedEof,
    InvalidType(u8),
    PayloadTooShort,
}

impl std::fmt::Display for WalDeserializeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnexpectedEof => write!(f, "unexpected end of WAL data"),
            Self::InvalidType(t) => write!(f, "invalid WAL record type: {t}"),
            Self::PayloadTooShort => write!(f, "WAL record payload too short"),
        }
    }
}

impl std::error::Error for WalDeserializeError {}

fn read_u64_le(data: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap())
}

fn read_u32_le(data: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap())
}

fn ensure_len(data: &[u8], min: usize) -> Result<(), WalDeserializeError> {
    if data.len() < min {
        Err(WalDeserializeError::PayloadTooShort)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(record: &WalRecord) -> WalRecord {
        let mut buf = Vec::new();
        record.serialize(&mut buf);
        let (decoded, consumed) = WalRecord::deserialize(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        decoded
    }

    #[test]
    fn begin_transaction_roundtrip() {
        let r = WalRecord::BeginTransaction;
        assert_eq!(roundtrip(&r), r);
    }

    #[test]
    fn commit_roundtrip() {
        let r = WalRecord::Commit;
        assert_eq!(roundtrip(&r), r);
    }

    #[test]
    fn checkpoint_roundtrip() {
        let r = WalRecord::Checkpoint;
        assert_eq!(roundtrip(&r), r);
    }

    #[test]
    fn table_insertion_roundtrip() {
        let r = WalRecord::TableInsertion {
            table_id: TableId(42),
            num_rows: 1000,
        };
        assert_eq!(roundtrip(&r), r);
    }

    #[test]
    fn node_deletion_roundtrip() {
        let r = WalRecord::NodeDeletion {
            table_id: TableId(1),
            node_offset: 999,
        };
        assert_eq!(roundtrip(&r), r);
    }

    #[test]
    fn node_update_roundtrip() {
        let r = WalRecord::NodeUpdate {
            table_id: TableId(5),
            column_id: 3,
            node_offset: 42,
        };
        assert_eq!(roundtrip(&r), r);
    }

    #[test]
    fn rel_deletion_roundtrip() {
        let r = WalRecord::RelDeletion {
            table_id: TableId(10),
            src_offset: 100,
            dst_offset: 200,
            rel_offset: 300,
        };
        assert_eq!(roundtrip(&r), r);
    }

    #[test]
    fn rel_update_roundtrip() {
        let r = WalRecord::RelUpdate {
            table_id: TableId(7),
            column_id: 2,
            src_offset: 10,
            dst_offset: 20,
            rel_offset: 30,
        };
        assert_eq!(roundtrip(&r), r);
    }

    #[test]
    fn create_catalog_entry_roundtrip() {
        let r = WalRecord::CreateCatalogEntry {
            entry_type: 1,
            table_id: TableId(99),
        };
        assert_eq!(roundtrip(&r), r);
    }

    #[test]
    fn drop_catalog_entry_roundtrip() {
        let r = WalRecord::DropCatalogEntry {
            entry_type: 2,
            entry_id: 555,
        };
        assert_eq!(roundtrip(&r), r);
    }

    #[test]
    fn alter_table_entry_roundtrip() {
        let r = WalRecord::AlterTableEntry {
            table_id: TableId(33),
        };
        assert_eq!(roundtrip(&r), r);
    }

    #[test]
    fn multiple_records_in_buffer() {
        let records = vec![
            WalRecord::BeginTransaction,
            WalRecord::TableInsertion {
                table_id: TableId(1),
                num_rows: 100,
            },
            WalRecord::NodeDeletion {
                table_id: TableId(1),
                node_offset: 50,
            },
            WalRecord::Commit,
        ];

        let mut buf = Vec::new();
        for r in &records {
            r.serialize(&mut buf);
        }

        let mut offset = 0;
        let mut decoded = Vec::new();
        while offset < buf.len() {
            let (record, consumed) = WalRecord::deserialize(&buf[offset..]).unwrap();
            decoded.push(record);
            offset += consumed;
        }

        assert_eq!(decoded, records);
    }

    #[test]
    fn deserialize_empty() {
        assert_eq!(
            WalRecord::deserialize(&[]),
            Err(WalDeserializeError::UnexpectedEof)
        );
    }

    #[test]
    fn deserialize_invalid_type() {
        assert_eq!(
            WalRecord::deserialize(&[99, 0, 0, 0, 0]),
            Err(WalDeserializeError::InvalidType(99))
        );
    }

    #[test]
    fn deserialize_truncated() {
        let r = WalRecord::TableInsertion {
            table_id: TableId(1),
            num_rows: 100,
        };
        let mut buf = Vec::new();
        r.serialize(&mut buf);
        // Truncate.
        let result = WalRecord::deserialize(&buf[..buf.len() - 1]);
        assert!(result.is_err());
    }

    #[test]
    fn wal_record_type_from_byte() {
        assert_eq!(
            WalRecordType::from_byte(1),
            Some(WalRecordType::BeginTransaction)
        );
        assert_eq!(
            WalRecordType::from_byte(254),
            Some(WalRecordType::Checkpoint)
        );
        assert_eq!(WalRecordType::from_byte(255), None);
    }
}
