use std::collections::HashMap;

/// Selects which DynamoDB operations should be routed by partition key.
///
/// See [`KeyRouteAffinityConfig`] for the full configuration object that
/// carries this mode plus any pre-configured partition key names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KeyRouteAffinityType {
    /// Standard round-robin load balancing across all live nodes. No
    /// affinity is applied to any request. This is the default.
    #[default]
    None,
    /// Route only read-before-write operations to the same coordinator
    /// per partition key. This covers conditional `PutItem`,
    /// `UpdateItem`, and `DeleteItem` (with `ConditionExpression`,
    /// `Expected`, or non-`None` `ReturnValues`), plus any `UpdateItem`
    /// with an update expression or RMW-style attribute updates.
    ///
    /// This is the recommended mode for workloads that use lightweight
    /// transactions (LWT): routing same-key requests to the same
    /// coordinator reduces Paxos round-trips.
    Rmw,
    /// Route all write operations (`PutItem`, `UpdateItem`, `DeleteItem`)
    /// to the same coordinator per partition key, regardless of
    /// conditions. `BatchWriteItem` is excluded — a single batch can
    /// target multiple partition keys across multiple tables and can't be
    /// routed to one coordinator.
    AnyWrite,
}

/// Configuration for key route affinity.
///
/// Combine an affinity [`KeyRouteAffinityType`] with optional pre-configured
/// partition key names. When a table's PK name is not pre-configured, the
/// resolver discovers it asynchronously via `DescribeTable` on first use;
/// requests for that table fall back to round-robin until discovery
/// completes.
///
/// # Examples
///
/// Build directly from a mode for the common case:
///
/// ```ignore
/// let cfg: KeyRouteAffinityConfig = KeyRouteAffinityType::Rmw.into();
/// ```
///
/// Or when passing it directly to `AlternatorClientBuilder`:
/// ```ignore
/// let client = AlternatorDynamoDbClient::builder()
///     .key_route_affinity(KeyRouteAffinityType::Rmw)
///     .build();
/// ```
///
/// Or via the builder to pre-configure PK names and skip the
/// `DescribeTable` lookup:
///
/// ```ignore
/// let cfg = KeyRouteAffinityConfig::builder()
///     .with_type(KeyRouteAffinityType::Rmw)
///     .with_pk_info("users", "user_id")
///     .with_pk_info("orders", "order_id")
///     .build();
/// ```
#[derive(Debug, Clone)]
pub struct KeyRouteAffinityConfig {
    /// Which operations qualify for partition-key routing.
    pub affinity_type: KeyRouteAffinityType,
    /// Pre-configured table name to partition key attribute name. Tables
    /// not in this map are resolved at runtime via `DescribeTable`.
    pub pk_info_per_table: HashMap<String, String>,
}

impl From<KeyRouteAffinityType> for KeyRouteAffinityConfig {
    fn from(affinity_type: KeyRouteAffinityType) -> Self {
        KeyRouteAffinityConfig {
            affinity_type,
            pk_info_per_table: HashMap::new(),
        }
    }
}

impl KeyRouteAffinityConfig {
    pub fn builder() -> KeyRouteAffinityConfigBuilder {
        KeyRouteAffinityConfigBuilder::new()
    }

    /// `true` if any affinity-routing should happen for this configuration.
    /// `false` when `affinity_type` is [`KeyRouteAffinityType::None`].
    pub fn is_enabled(&self) -> bool {
        self.affinity_type != KeyRouteAffinityType::None
    }
}

/// Builder for [`KeyRouteAffinityConfig`].
#[derive(Debug, Clone, Default)]
pub struct KeyRouteAffinityConfigBuilder {
    affinity_type: KeyRouteAffinityType,
    pk_info_per_table: HashMap<String, String>,
}

impl KeyRouteAffinityConfigBuilder {
    /// Creates an empty builder with default affinity (`None`) and no pre-configured PK names.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the route affinity type.
    pub fn with_type(mut self, affinity_type: KeyRouteAffinityType) -> Self {
        self.affinity_type = affinity_type;
        self
    }

    /// Pre-configures the partition key attribute name for `table`,
    /// skipping the `DescribeTable` lookup for that table. Calling this
    /// multiple times for the same table overwrites the previous value.
    pub fn with_pk_info(mut self, table: impl Into<String>, pk_info: impl Into<String>) -> Self {
        self.pk_info_per_table.insert(table.into(), pk_info.into());
        self
    }

    /// Consumes the builder and produces a [`KeyRouteAffinityConfig`].
    pub fn build(self) -> KeyRouteAffinityConfig {
        KeyRouteAffinityConfig {
            affinity_type: self.affinity_type,
            pk_info_per_table: self.pk_info_per_table,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_none_and_disabled() {
        let cfg: KeyRouteAffinityConfig = KeyRouteAffinityType::default().into();
        assert_eq!(cfg.affinity_type, KeyRouteAffinityType::None);
        assert!(!cfg.is_enabled());
        assert!(cfg.pk_info_per_table.is_empty());
    }

    #[test]
    fn is_enabled_reflects_affinity_type() {
        let none: KeyRouteAffinityConfig = KeyRouteAffinityType::None.into();
        let rmw: KeyRouteAffinityConfig = KeyRouteAffinityType::Rmw.into();
        let any: KeyRouteAffinityConfig = KeyRouteAffinityType::AnyWrite.into();

        assert!(!none.is_enabled());
        assert!(rmw.is_enabled());
        assert!(any.is_enabled());
    }

    #[test]
    fn from_type_produces_empty_pk_map() {
        let cfg: KeyRouteAffinityConfig = KeyRouteAffinityType::Rmw.into();
        assert_eq!(cfg.affinity_type, KeyRouteAffinityType::Rmw);
        assert!(cfg.pk_info_per_table.is_empty());
    }

    #[test]
    fn builder_default_matches_none_type() {
        let cfg = KeyRouteAffinityConfig::builder().build();
        assert_eq!(cfg.affinity_type, KeyRouteAffinityType::None);
        assert!(cfg.pk_info_per_table.is_empty());
    }

    #[test]
    fn builder_sets_type() {
        let cfg = KeyRouteAffinityConfig::builder()
            .with_type(KeyRouteAffinityType::AnyWrite)
            .build();
        assert_eq!(cfg.affinity_type, KeyRouteAffinityType::AnyWrite);
    }

    #[test]
    fn builder_accumulates_pk_info() {
        let cfg = KeyRouteAffinityConfig::builder()
            .with_type(KeyRouteAffinityType::Rmw)
            .with_pk_info("users", "user_id")
            .with_pk_info("orders", "order_id")
            .build();

        assert_eq!(cfg.affinity_type, KeyRouteAffinityType::Rmw);
        assert_eq!(cfg.pk_info_per_table.len(), 2);
        assert_eq!(
            cfg.pk_info_per_table.get("users").map(String::as_str),
            Some("user_id")
        );
        assert_eq!(
            cfg.pk_info_per_table.get("orders").map(String::as_str),
            Some("order_id")
        );
    }

    #[test]
    fn builder_pk_info_overwrites_on_duplicate_table() {
        let cfg = KeyRouteAffinityConfig::builder()
            .with_pk_info("users", "old_pk")
            .with_pk_info("users", "new_pk")
            .build();
        assert_eq!(
            cfg.pk_info_per_table.get("users").map(String::as_str),
            Some("new_pk")
        );
        assert_eq!(cfg.pk_info_per_table.len(), 1);
    }
}
