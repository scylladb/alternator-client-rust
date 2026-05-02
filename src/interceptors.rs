use crate::*;

use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::interceptors::Intercept;
use aws_smithy_runtime_api::client::interceptors::context::{
    BeforeSerializationInterceptorContextMut, BeforeTransmitInterceptorContextMut,
};
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::config_bag::ConfigBag;
use aws_smithy_types::config_bag::{Storable, StoreReplace};
use std::sync::Arc;

/// Driver's main interceptor
///
/// Is added by [AlternatorClient] to its inner Dynamodb client on construction.
///
/// Uses [strip_headers] and [compress_request].
///
/// Also checks [ConfigBag] for config overrides that could have been left by [AlternatorOverrideInterceptor].
#[derive(Debug)]
pub(crate) struct AlternatorInterceptor {
    request_compression: RequestCompression,
    enforce_header_whitelist: bool,
}
impl AlternatorInterceptor {
    pub fn new(request_compression: RequestCompression, enforce_header_whitelist: bool) -> Self {
        Self {
            request_compression,
            enforce_header_whitelist,
        }
    }
}
impl Intercept for AlternatorInterceptor {
    fn name(&self) -> &'static str {
        "AlternatorInterceptor"
    }

    fn modify_before_retry_loop(
        &self,
        context: &mut BeforeTransmitInterceptorContextMut,
        _: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        // check for overrides
        let request_compression = cfg
            .interceptor_state()
            .load::<RequestCompressionStore>()
            .map(|store| store.request_compression.clone())
            .unwrap_or(self.request_compression.clone());

        // message must be compressed before signing, but it's more efficient to do it before retry loop
        if let Some((algorithm, level, threshold)) = request_compression.get() {
            compress_request(context.request_mut(), algorithm, level, threshold);
        }

        Ok(())
    }

    fn modify_before_transmit(
        &self,
        context: &mut BeforeTransmitInterceptorContextMut,
        _: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        // check for overrides
        let enforce_header_whitelist = cfg
            .interceptor_state()
            .load::<EnforceHeaderWhitelistStore>()
            .map(|store| store.enforce_header_whitelist)
            .unwrap_or(self.enforce_header_whitelist);

        // enforce header whitelist
        if enforce_header_whitelist {
            strip_headers(context.request_mut());
        }

        Ok(())
    }

    fn modify_before_signing(
        &self,
        context: &mut BeforeTransmitInterceptorContextMut<'_>,
        _: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        // Take the next node from the query plan and override the request URI.
        if let Some(query_plan) = cfg.interceptor_state().load::<QueryPlan>()
            && let Some(next_node) = query_plan.next_node()
        {
            let request = context.request_mut();
            let mut current = url::Url::parse(request.uri())?;
            current
                .set_scheme(next_node.scheme())
                .map_err(|_| "cannot set scheme")?;
            current
                .set_host(next_node.host_str())
                .map_err(|_| "cannot set host")?;
            current
                .set_port(next_node.port())
                .map_err(|_| "cannot set port")?;

            request.set_uri(current.as_str())?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RequestCompressionStore {
    request_compression: RequestCompression,
}
impl Storable for RequestCompressionStore {
    type Storer = StoreReplace<Self>;
}

#[derive(Debug, Clone)]
pub(crate) struct EnforceHeaderWhitelistStore {
    enforce_header_whitelist: bool,
}
impl Storable for EnforceHeaderWhitelistStore {
    type Storer = StoreReplace<Self>;
}

/// An interceptor used to override [AlternatorClient]'s config.
///
/// Adds specified config overrides to [ConfigBag], so that [AlternatorInterceptor] can later look for it.
///
/// Is used by [AlternatorCustomizableOperation] to allow per-operation customization.
#[derive(Debug)]
pub(crate) struct AlternatorOverrideInterceptor<T: Storable<Storer = StoreReplace<T>> + Clone> {
    store: T,
}
impl<T: Storable<Storer = StoreReplace<T>> + Clone> Intercept for AlternatorOverrideInterceptor<T> {
    fn name(&self) -> &'static str {
        "AlternatorOverrideInterceptor"
    }

    fn modify_before_serialization(
        &self,
        _: &mut BeforeSerializationInterceptorContextMut,
        _: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        // update config bag, so that AlternatorInterceptor will later include the override
        cfg.interceptor_state().store_put(self.store.clone());

        Ok(())
    }
}
impl AlternatorOverrideInterceptor<RequestCompressionStore> {
    pub(crate) fn for_request_compression(request_compression: RequestCompression) -> Self {
        AlternatorOverrideInterceptor {
            store: RequestCompressionStore {
                request_compression,
            },
        }
    }
}
impl AlternatorOverrideInterceptor<EnforceHeaderWhitelistStore> {
    pub(crate) fn for_enforce_header_whitelist(enforce_header_whitelist: bool) -> Self {
        AlternatorOverrideInterceptor {
            store: EnforceHeaderWhitelistStore {
                enforce_header_whitelist,
            },
        }
    }
}

/// An interceptor that adds a round-robin [QueryPlan] to the config bag before request serialization,
/// so that [AlternatorInterceptor] can later use it to determine which node to send the request to.
#[derive(Debug)]
pub(crate) struct RoundRobinQueryPlanInterceptor {
    live_nodes: Arc<LiveNodes>,
}

impl RoundRobinQueryPlanInterceptor {
    pub fn new(live_nodes: Arc<LiveNodes>) -> Self {
        Self { live_nodes }
    }
}

impl Intercept for RoundRobinQueryPlanInterceptor {
    fn name(&self) -> &'static str {
        "RoundRobinQueryPlanInterceptor"
    }

    /// This hook is triggered exactly once per request, before the first attempt is serialized.
    /// Query plan, put here, is then used before every attempt by [`AlternatorInterceptor`] in `modify_before_signing`
    /// hook to determine which node the request should be sent to.
    /// This allows for tracking which nodes have already been tried in the current request and implementing a round-robin strategy.
    fn modify_before_serialization(
        &self,
        _: &mut BeforeSerializationInterceptorContextMut,
        _: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let query_plan = QueryPlan::new(self.live_nodes.clone());
        cfg.interceptor_state().store_put(query_plan);

        Ok(())
    }
}
