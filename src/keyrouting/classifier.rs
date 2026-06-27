use aws_sdk_dynamodb::operation::{
    batch_write_item::BatchWriteItemInput, delete_item::DeleteItemInput, put_item::PutItemInput,
    update_item::UpdateItemInput,
};
use aws_sdk_dynamodb::types::{AttributeAction, AttributeValue, ReturnValue};
use aws_smithy_runtime_api::client::interceptors::context::Input;
use std::collections::HashMap;

use super::affinity_config::KeyRouteAffinityType;

/// Typed view over the DynamoDB operations key route affinity cares about.
pub(crate) enum DynamoOp<'a> {
    Put(&'a PutItemInput),
    Update(&'a UpdateItemInput),
    Delete(&'a DeleteItemInput),
    BatchWrite(&'a BatchWriteItemInput),
}

pub(crate) struct PartitionKeyCandidate<'a> {
    pub table_name: &'a str,
    pub attributes: &'a HashMap<String, AttributeValue>,
}

impl<'a> DynamoOp<'a> {
    /// Downcast the SDK's type-erased input into one of the variants we care
    /// about. Anything else (Scan, Query, TransactWriteItems, etc.) yields
    /// `None` and the caller treats the request as non-applicable.
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
        if let Some(r) = input.downcast_ref::<BatchWriteItemInput>() {
            return Some(Self::BatchWrite(r));
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
            Self::BatchWrite(_) => mode == KeyRouteAffinityType::AnyWrite,
        }
    }

    /// Returns candidate table/key maps that can provide a partition key.
    /// Single-item operations return at most one candidate; BatchWriteItem can
    /// return multiple candidates so the caller can apply batch-level routing
    /// policy across all usable keys.
    pub fn partition_key_candidates(&self) -> Vec<PartitionKeyCandidate<'a>> {
        match self {
            Self::Put(r) => r
                .table_name()
                .zip(r.item())
                .map(|(table_name, attributes)| {
                    vec![PartitionKeyCandidate {
                        table_name,
                        attributes,
                    }]
                })
                .unwrap_or_default(),
            Self::Update(r) => r
                .table_name()
                .zip(r.key())
                .map(|(table_name, attributes)| {
                    vec![PartitionKeyCandidate {
                        table_name,
                        attributes,
                    }]
                })
                .unwrap_or_default(),
            Self::Delete(r) => r
                .table_name()
                .zip(r.key())
                .map(|(table_name, attributes)| {
                    vec![PartitionKeyCandidate {
                        table_name,
                        attributes,
                    }]
                })
                .unwrap_or_default(),
            Self::BatchWrite(r) => r
                .request_items()
                .into_iter()
                .flat_map(|items| items.iter())
                .flat_map(|(table_name, writes)| {
                    writes.iter().filter_map(|write| {
                        let attributes = batch_write_request_attributes(write)?;
                        Some(PartitionKeyCandidate {
                            table_name: table_name.as_str(),
                            attributes,
                        })
                    })
                })
                .collect(),
        }
    }
}

fn batch_write_request_attributes(
    write: &aws_sdk_dynamodb::types::WriteRequest,
) -> Option<&HashMap<String, AttributeValue>> {
    if let Some(delete) = write.delete_request() {
        return Some(delete.key());
    }
    write.put_request().map(|put| put.item())
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
        AttributeAction, AttributeValue, AttributeValueUpdate, DeleteRequest,
        ExpectedAttributeValue, PutRequest, ReturnValue, WriteRequest,
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

    // ----- BatchWriteItem -----

    fn put_write(pk_name: &str, value: &str) -> WriteRequest {
        let put = PutRequest::builder()
            .item(pk_name, s(value))
            .item("other", s("x"))
            .build()
            .unwrap();
        WriteRequest::builder().put_request(put).build()
    }

    fn put_write_without_pk() -> WriteRequest {
        let put = PutRequest::builder().item("other", s("x")).build().unwrap();
        WriteRequest::builder().put_request(put).build()
    }

    fn delete_write(pk_name: &str, value: &str) -> WriteRequest {
        let delete = DeleteRequest::builder()
            .key(pk_name, s(value))
            .build()
            .unwrap();
        WriteRequest::builder().delete_request(delete).build()
    }

    #[test]
    fn batch_write_applies_only_for_any_write() {
        let r = BatchWriteItemInput::builder()
            .request_items("t", vec![put_write("pk", "k")])
            .build()
            .unwrap();
        let op = DynamoOp::BatchWrite(&r);

        assert!(!op.should_apply(KeyRouteAffinityType::None));
        assert!(!op.should_apply(KeyRouteAffinityType::Rmw));
        assert!(op.should_apply(KeyRouteAffinityType::AnyWrite));
    }

    #[test]
    fn batch_write_delete_applies_only_for_any_write() {
        let r = BatchWriteItemInput::builder()
            .request_items("t", vec![delete_write("pk", "k")])
            .build()
            .unwrap();
        let op = DynamoOp::BatchWrite(&r);

        assert!(!op.should_apply(KeyRouteAffinityType::None));
        assert!(!op.should_apply(KeyRouteAffinityType::Rmw));
        assert!(op.should_apply(KeyRouteAffinityType::AnyWrite));
    }

    #[test]
    fn batch_write_partition_key_candidates_include_single_table_put() {
        let r = BatchWriteItemInput::builder()
            .request_items("t", vec![put_write("pk", "put_key")])
            .build()
            .unwrap();

        let candidates = DynamoOp::BatchWrite(&r).partition_key_candidates();

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].table_name, "t");
        assert_eq!(candidates[0].attributes.get("pk"), Some(&s("put_key")));
    }

    #[test]
    fn batch_write_partition_key_candidates_include_single_table_delete() {
        let r = BatchWriteItemInput::builder()
            .request_items("t", vec![delete_write("pk", "delete_key")])
            .build()
            .unwrap();

        let candidates = DynamoOp::BatchWrite(&r).partition_key_candidates();

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].table_name, "t");
        assert_eq!(candidates[0].attributes.get("pk"), Some(&s("delete_key")));
    }

    #[test]
    fn batch_write_partition_key_candidates_include_requests_without_pk() {
        let r = BatchWriteItemInput::builder()
            .request_items(
                "t",
                vec![
                    put_write_without_pk(),
                    delete_write("pk", "delete_key"),
                    put_write("pk", "put_key"),
                ],
            )
            .build()
            .unwrap();

        let candidates = DynamoOp::BatchWrite(&r).partition_key_candidates();

        assert_eq!(candidates.len(), 3);
        assert!(
            candidates
                .iter()
                .all(|candidate| candidate.table_name == "t")
        );
        assert!(
            candidates
                .iter()
                .any(|candidate| !candidate.attributes.contains_key("pk"))
        );
        assert!(
            candidates
                .iter()
                .any(|candidate| candidate.attributes.get("pk") == Some(&s("delete_key")))
        );
        assert!(
            candidates
                .iter()
                .any(|candidate| candidate.attributes.get("pk") == Some(&s("put_key")))
        );
    }

    #[test]
    fn batch_write_partition_key_candidates_include_multiple_tables() {
        for z_table_first in [true, false] {
            let builder = BatchWriteItemInput::builder();
            let builder = if z_table_first {
                builder
                    .request_items("z_table", vec![put_write("pk", "z_key")])
                    .request_items("a_table", vec![put_write("pk", "a_key")])
            } else {
                builder
                    .request_items("a_table", vec![put_write("pk", "a_key")])
                    .request_items("z_table", vec![put_write("pk", "z_key")])
            };
            let r = builder.build().unwrap();

            let candidates = DynamoOp::BatchWrite(&r).partition_key_candidates();

            assert_eq!(candidates.len(), 2);
            assert!(candidates.iter().any(|candidate| {
                candidate.table_name == "a_table"
                    && candidate.attributes.get("pk") == Some(&s("a_key"))
            }));
            assert!(candidates.iter().any(|candidate| {
                candidate.table_name == "z_table"
                    && candidate.attributes.get("pk") == Some(&s("z_key"))
            }));
        }
    }

    #[test]
    fn batch_write_empty_batch_returns_no_candidates() {
        let r = BatchWriteItemInput::builder()
            .request_items("t", Vec::new())
            .build()
            .unwrap();

        assert!(
            DynamoOp::BatchWrite(&r)
                .partition_key_candidates()
                .is_empty()
        );
    }

    #[test]
    fn batch_write_partition_key_candidates_use_put_items_and_delete_keys() {
        let r = BatchWriteItemInput::builder()
            .request_items(
                "orders",
                vec![put_write("pk", "put_key"), delete_write("pk", "delete_key")],
            )
            .build()
            .unwrap();

        let candidates = DynamoOp::BatchWrite(&r).partition_key_candidates();

        assert_eq!(candidates.len(), 2);
        assert!(candidates.iter().any(|candidate| {
            candidate.attributes.get("pk") == Some(&s("put_key"))
                && candidate.attributes.get("other") == Some(&s("x"))
        }));
        assert!(candidates.iter().any(|candidate| {
            candidate.attributes.get("pk") == Some(&s("delete_key"))
                && !candidate.attributes.contains_key("other")
        }));
    }

    #[test]
    fn batch_write_ignores_empty_write_requests() {
        let r = BatchWriteItemInput::builder()
            .request_items("t", vec![WriteRequest::builder().build()])
            .build()
            .unwrap();

        assert!(
            DynamoOp::BatchWrite(&r)
                .partition_key_candidates()
                .is_empty()
        );
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
    fn from_input_recognizes_put_update_delete_batch_write() {
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

        let bw = BatchWriteItemInput::builder()
            .request_items("t", vec![put_write("pk", "k")])
            .build()
            .unwrap();
        assert!(matches!(
            DynamoOp::from_input(&Input::erase(bw)),
            Some(DynamoOp::BatchWrite(_))
        ));
    }

    #[test]
    fn from_input_returns_none_for_unsupported_op() {
        // Scan, Query, etc. are intentionally not in the enum.
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
        let candidates = DynamoOp::Put(&r).partition_key_candidates();

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].table_name, "t");
        assert_eq!(candidates[0].attributes.get("pk"), Some(&s("alice")));
    }

    #[test]
    fn update_partition_key_from_key() {
        let r = UpdateItemInput::builder()
            .table_name("t")
            .key("pk", s("bob"))
            .build()
            .unwrap();
        let candidates = DynamoOp::Update(&r).partition_key_candidates();

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].table_name, "t");
        assert_eq!(candidates[0].attributes.get("pk"), Some(&s("bob")));
    }

    #[test]
    fn partition_key_missing_attribute_returns_none() {
        let r = PutItemInput::builder()
            .table_name("t")
            .item("other", s("x"))
            .build()
            .unwrap();
        let candidates = DynamoOp::Put(&r).partition_key_candidates();

        assert_eq!(candidates.len(), 1);
        assert!(!candidates[0].attributes.contains_key("pk"));
    }

    #[test]
    fn missing_table_name_returns_no_candidates() {
        let r = PutItemInput::builder().item("pk", s("a")).build().unwrap();

        assert!(DynamoOp::Put(&r).partition_key_candidates().is_empty());
    }
}
