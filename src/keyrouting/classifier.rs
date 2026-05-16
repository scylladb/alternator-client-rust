use aws_sdk_dynamodb::operation::{
    delete_item::DeleteItemInput, put_item::PutItemInput, update_item::UpdateItemInput,
};
use aws_sdk_dynamodb::types::{AttributeAction, AttributeValue, ReturnValue};
use aws_smithy_runtime_api::client::interceptors::context::Input;

use super::affinity_config::KeyRouteAffinityType;

/// Typed view over the DynamoDB operations key route affinity cares about.
///
/// `BatchWriteItem` is intentionally absent: a single request can target
/// multiple partition keys across multiple tables, so it can't be routed
/// deterministically to one coordinator. Those requests fall back to
/// round-robin balancing.
pub(crate) enum DynamoOp<'a> {
    Put(&'a PutItemInput),
    Update(&'a UpdateItemInput),
    Delete(&'a DeleteItemInput),
}

impl<'a> DynamoOp<'a> {
    /// Downcast the SDK's type-erased input into one of the variants we care
    /// about. Anything else (Scan, BatchWriteItem, TransactWriteItems, etc.)
    /// yields `None` and the caller treats the request as non-applicable.
    pub fn from_input(input: &'a Input) -> Option<Self> {
        if let Some(r) = input.downcast_ref::<PutItemInput>() {
            return Some(Self::Put(r));
        }
        if let Some(r) = input.downcast_ref::<UpdateItemInput>() {
            return Some(Self::Update(r));
        }
        if let Some(r) = input.downcast_ref::<DeleteItemInput>() {
            return Some(Self::Delete(r));
        }
        None
    }

    /// Returns `true` if this request qualifies for partition-key routing
    /// under the given affinity mode. Always `false` when `mode` is `None`.
    pub fn should_apply(&self, mode: KeyRouteAffinityType) -> bool {
        if mode == KeyRouteAffinityType::None {
            return false;
        }

        match self {
            Self::Put(r) => should_apply_put(mode, r),
            Self::Delete(r) => should_apply_delete(mode, r),
            Self::Update(r) => should_apply_update(mode, r),
        }
    }

    /// Returns the request's target table name, or `None` if the SDK input doesn't have one set.
    pub fn table_name(&self) -> Option<&str> {
        match self {
            Self::Put(r) => r.table_name(),
            Self::Update(r) => r.table_name(),
            Self::Delete(r) => r.table_name(),
        }
    }

    /// Looks up the partition key value by attribute name. Returns `None`
    /// if the key map is unset or the attribute is missing.
    pub fn partition_key(&self, pk_name: &str) -> Option<&AttributeValue> {
        match self {
            Self::Put(r) => r.item().and_then(|m| m.get(pk_name)),
            Self::Update(r) => r.key().and_then(|m| m.get(pk_name)),
            Self::Delete(r) => r.key().and_then(|m| m.get(pk_name)),
        }
    }
}

/// Checks if a `PutItem` operation qualifies for Key Route Affinity based on the configured mode.
fn should_apply_put(mode: KeyRouteAffinityType, r: &PutItemInput) -> bool {
    if mode == KeyRouteAffinityType::AnyWrite {
        return true;
    }
    r.condition_expression().is_some_and(|s| !s.is_empty())
        || r.expected().is_some_and(|e| !e.is_empty())
        || r.return_values().is_some_and(|rv| *rv != ReturnValue::None)
}

/// Checks if a `DeleteItem` operation qualifies for Key Route Affinity based on the configured mode.
fn should_apply_delete(mode: KeyRouteAffinityType, r: &DeleteItemInput) -> bool {
    if mode == KeyRouteAffinityType::AnyWrite {
        return true;
    }
    r.condition_expression().is_some_and(|s| !s.is_empty())
        || r.expected().is_some_and(|e| !e.is_empty())
        || r.return_values().is_some_and(|rv| *rv != ReturnValue::None)
}

/// Checks if an `UpdateItem` operation qualifies for Key Route Affinity based on the configured mode.
fn should_apply_update(mode: KeyRouteAffinityType, r: &UpdateItemInput) -> bool {
    if mode == KeyRouteAffinityType::AnyWrite {
        return true;
    }
    // UpdateExpression operations are LWT-based in Alternator.
    if r.update_expression().is_some_and(|s| !s.is_empty()) {
        return true;
    }
    if r.condition_expression().is_some_and(|s| !s.is_empty()) {
        return true;
    }
    if r.expected().is_some_and(|e| !e.is_empty()) {
        return true;
    }
    // Return values that require reading the item:
    //   ALL_OLD / UPDATED_OLD: state before the update
    //   ALL_NEW: full item after the update (non-updated attrs need a read)
    //   UPDATED_NEW: computable from the expression alone — no read needed.
    if let Some(rv) = r.return_values()
        && matches!(
            rv,
            ReturnValue::AllOld | ReturnValue::UpdatedOld | ReturnValue::AllNew
        )
    {
        return true;
    }

    // Legacy AttributeUpdates: ADD is always RMW; DELETE with a value is RMW.
    if let Some(updates) = r.attribute_updates() {
        for u in updates.values() {
            let Some(action) = u.action() else { continue };
            if *action == AttributeAction::Add {
                return true;
            }
            if *action == AttributeAction::Delete && u.value().is_some() {
                return true;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodb::operation::scan::ScanInput;
    use aws_sdk_dynamodb::types::{
        AttributeAction, AttributeValue, AttributeValueUpdate, ExpectedAttributeValue, ReturnValue,
    };

    fn s(v: &str) -> AttributeValue {
        AttributeValue::S(v.to_string())
    }

    // ----- PutItem -----

    #[test]
    fn put_rmw_plain_does_not_apply() {
        let r = PutItemInput::builder()
            .table_name("t")
            .item("pk", s("k"))
            .build()
            .unwrap();
        assert!(!should_apply_put(KeyRouteAffinityType::Rmw, &r));
    }

    #[test]
    fn put_rmw_with_condition_applies() {
        let r = PutItemInput::builder()
            .table_name("t")
            .item("pk", s("k"))
            .condition_expression("attribute_not_exists(pk)")
            .build()
            .unwrap();
        assert!(should_apply_put(KeyRouteAffinityType::Rmw, &r));
    }

    #[test]
    fn put_rmw_with_empty_condition_does_not_apply() {
        let r = PutItemInput::builder()
            .table_name("t")
            .item("pk", s("k"))
            .condition_expression("")
            .build()
            .unwrap();
        assert!(!should_apply_put(KeyRouteAffinityType::Rmw, &r));
    }

    #[test]
    fn put_rmw_with_expected_applies() {
        let r = PutItemInput::builder()
            .table_name("t")
            .item("pk", s("k"))
            .expected("a", ExpectedAttributeValue::builder().value(s("v")).build())
            .build()
            .unwrap();
        assert!(should_apply_put(KeyRouteAffinityType::Rmw, &r));
    }

    #[test]
    fn put_rmw_return_values_none_does_not_apply() {
        let r = PutItemInput::builder()
            .table_name("t")
            .item("pk", s("k"))
            .return_values(ReturnValue::None)
            .build()
            .unwrap();
        assert!(!should_apply_put(KeyRouteAffinityType::Rmw, &r));
    }

    #[test]
    fn put_rmw_return_values_all_old_applies() {
        let r = PutItemInput::builder()
            .table_name("t")
            .item("pk", s("k"))
            .return_values(ReturnValue::AllOld)
            .build()
            .unwrap();
        assert!(should_apply_put(KeyRouteAffinityType::Rmw, &r));
    }

    #[test]
    fn put_any_write_always_applies() {
        let r = PutItemInput::builder()
            .table_name("t")
            .item("pk", s("k"))
            .build()
            .unwrap();
        assert!(should_apply_put(KeyRouteAffinityType::AnyWrite, &r));
    }

    // ----- DeleteItem (mirror of Put) -----

    #[test]
    fn delete_rmw_plain_does_not_apply() {
        let r = DeleteItemInput::builder()
            .table_name("t")
            .key("pk", s("k"))
            .build()
            .unwrap();
        assert!(!should_apply_delete(KeyRouteAffinityType::Rmw, &r));
    }

    #[test]
    fn delete_rmw_with_condition_applies() {
        let r = DeleteItemInput::builder()
            .table_name("t")
            .key("pk", s("k"))
            .condition_expression("attribute_exists(pk)")
            .build()
            .unwrap();
        assert!(should_apply_delete(KeyRouteAffinityType::Rmw, &r));
    }

    #[test]
    fn delete_any_write_always_applies() {
        let r = DeleteItemInput::builder()
            .table_name("t")
            .key("pk", s("k"))
            .build()
            .unwrap();
        assert!(should_apply_delete(KeyRouteAffinityType::AnyWrite, &r));
    }

    // ----- UpdateItem -----

    #[test]
    fn update_rmw_plain_does_not_apply() {
        let r = UpdateItemInput::builder()
            .table_name("t")
            .key("pk", s("k"))
            .build()
            .unwrap();
        assert!(!should_apply_update(KeyRouteAffinityType::Rmw, &r));
    }

    #[test]
    fn update_rmw_with_update_expression_applies() {
        let r = UpdateItemInput::builder()
            .table_name("t")
            .key("pk", s("k"))
            .update_expression("SET a = :v")
            .build()
            .unwrap();
        assert!(should_apply_update(KeyRouteAffinityType::Rmw, &r));
    }

    #[test]
    fn update_rmw_with_empty_update_expression_does_not_apply() {
        let r = UpdateItemInput::builder()
            .table_name("t")
            .key("pk", s("k"))
            .update_expression("")
            .build()
            .unwrap();
        assert!(!should_apply_update(KeyRouteAffinityType::Rmw, &r));
    }

    #[test]
    fn update_rmw_with_empty_condition_does_not_apply() {
        let r = UpdateItemInput::builder()
            .table_name("t")
            .key("pk", s("k"))
            .condition_expression("")
            .build()
            .unwrap();
        assert!(!should_apply_update(KeyRouteAffinityType::Rmw, &r));
    }

    #[test]
    fn update_rmw_updated_new_does_not_apply() {
        // UPDATED_NEW is computable without a read — must NOT trigger.
        let r = UpdateItemInput::builder()
            .table_name("t")
            .key("pk", s("k"))
            .return_values(ReturnValue::UpdatedNew)
            .build()
            .unwrap();
        assert!(!should_apply_update(KeyRouteAffinityType::Rmw, &r));
    }

    #[test]
    fn update_rmw_all_old_applies() {
        let r = UpdateItemInput::builder()
            .table_name("t")
            .key("pk", s("k"))
            .return_values(ReturnValue::AllOld)
            .build()
            .unwrap();
        assert!(should_apply_update(KeyRouteAffinityType::Rmw, &r));
    }

    #[test]
    fn update_rmw_attribute_update_add_applies() {
        let r = UpdateItemInput::builder()
            .table_name("t")
            .key("pk", s("k"))
            .attribute_updates(
                "counter",
                AttributeValueUpdate::builder()
                    .action(AttributeAction::Add)
                    .value(AttributeValue::N("1".to_string()))
                    .build(),
            )
            .build()
            .unwrap();
        assert!(should_apply_update(KeyRouteAffinityType::Rmw, &r));
    }

    #[test]
    fn update_rmw_attribute_update_delete_with_value_applies() {
        let r = UpdateItemInput::builder()
            .table_name("t")
            .key("pk", s("k"))
            .attribute_updates(
                "tags",
                AttributeValueUpdate::builder()
                    .action(AttributeAction::Delete)
                    .value(s("v"))
                    .build(),
            )
            .build()
            .unwrap();
        assert!(should_apply_update(KeyRouteAffinityType::Rmw, &r));
    }

    #[test]
    fn update_rmw_attribute_update_delete_without_value_does_not_apply() {
        let r = UpdateItemInput::builder()
            .table_name("t")
            .key("pk", s("k"))
            .attribute_updates(
                "attr",
                AttributeValueUpdate::builder()
                    .action(AttributeAction::Delete)
                    .build(),
            )
            .build()
            .unwrap();
        assert!(!should_apply_update(KeyRouteAffinityType::Rmw, &r));
    }

    #[test]
    fn update_rmw_attribute_update_put_does_not_apply() {
        let r = UpdateItemInput::builder()
            .table_name("t")
            .key("pk", s("k"))
            .attribute_updates(
                "attr",
                AttributeValueUpdate::builder()
                    .action(AttributeAction::Put)
                    .value(s("v"))
                    .build(),
            )
            .build()
            .unwrap();
        assert!(!should_apply_update(KeyRouteAffinityType::Rmw, &r));
    }

    #[test]
    fn update_any_write_always_applies() {
        let r = UpdateItemInput::builder()
            .table_name("t")
            .key("pk", s("k"))
            .build()
            .unwrap();
        assert!(should_apply_update(KeyRouteAffinityType::AnyWrite, &r));
    }

    // ----- Enum dispatch / None mode -----

    #[test]
    fn none_mode_never_applies_even_for_conditional_writes() {
        let r = PutItemInput::builder()
            .table_name("t")
            .item("pk", s("k"))
            .condition_expression("attribute_not_exists(pk)")
            .build()
            .unwrap();
        let input = Input::erase(r);
        let op = DynamoOp::from_input(&input).unwrap();
        assert!(!op.should_apply(KeyRouteAffinityType::None));
    }

    #[test]
    fn from_input_recognizes_put_update_delete() {
        let p = PutItemInput::builder()
            .table_name("t")
            .item("pk", s("k"))
            .build()
            .unwrap();
        assert!(matches!(
            DynamoOp::from_input(&Input::erase(p)),
            Some(DynamoOp::Put(_))
        ));

        let u = UpdateItemInput::builder()
            .table_name("t")
            .key("pk", s("k"))
            .build()
            .unwrap();
        assert!(matches!(
            DynamoOp::from_input(&Input::erase(u)),
            Some(DynamoOp::Update(_))
        ));

        let d = DeleteItemInput::builder()
            .table_name("t")
            .key("pk", s("k"))
            .build()
            .unwrap();
        assert!(matches!(
            DynamoOp::from_input(&Input::erase(d)),
            Some(DynamoOp::Delete(_))
        ));
    }

    #[test]
    fn from_input_returns_none_for_unsupported_op() {
        // Scan, BatchWriteItem, Query, etc. are intentionally not in the enum.
        let scan = ScanInput::builder().table_name("t").build().unwrap();
        assert!(DynamoOp::from_input(&Input::erase(scan)).is_none());
    }

    // ----- Partition key + table name extraction -----

    #[test]
    fn put_partition_key_from_item() {
        let r = PutItemInput::builder()
            .table_name("t")
            .item("pk", s("alice"))
            .item("other", s("x"))
            .build()
            .unwrap();
        assert_eq!(DynamoOp::Put(&r).partition_key("pk"), Some(&s("alice")));
    }

    #[test]
    fn update_partition_key_from_key() {
        let r = UpdateItemInput::builder()
            .table_name("t")
            .key("pk", s("bob"))
            .build()
            .unwrap();
        assert_eq!(DynamoOp::Update(&r).partition_key("pk"), Some(&s("bob")));
    }

    #[test]
    fn partition_key_missing_attribute_returns_none() {
        let r = PutItemInput::builder()
            .table_name("t")
            .item("other", s("x"))
            .build()
            .unwrap();
        assert!(DynamoOp::Put(&r).partition_key("pk").is_none());
    }

    #[test]
    fn table_name_extracts_correctly() {
        let p = PutItemInput::builder()
            .table_name("users")
            .item("pk", s("a"))
            .build()
            .unwrap();
        assert_eq!(DynamoOp::Put(&p).table_name(), Some("users"));
    }
}
