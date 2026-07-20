use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::http::Request;
use std::fmt;
use std::sync::Arc;

pub const DEFAULT_USER_AGENT: &str = concat!(
    "scylladb-alternator-client-rust/",
    env!("CARGO_PKG_VERSION")
);

type UserAgentTransform = Arc<dyn Fn(&str) -> Option<String> + Send + Sync>;

#[derive(Clone, Default)]
pub enum UserAgent {
    #[default]
    Default,
    Disabled,
    Value(String),
    Transform(UserAgentTransform),
}

impl UserAgent {
    pub fn disabled() -> Self {
        Self::Disabled
    }

    pub fn value(user_agent: impl Into<String>) -> Self {
        Self::Value(user_agent.into())
    }

    pub fn transform<F>(transform: F) -> Self
    where
        F: Fn(&str) -> String + Send + Sync + 'static,
    {
        Self::Transform(Arc::new(move |default_user_agent| {
            Some(transform(default_user_agent))
        }))
    }

    pub fn transform_optional<F>(transform: F) -> Self
    where
        F: Fn(&str) -> Option<String> + Send + Sync + 'static,
    {
        Self::Transform(Arc::new(transform))
    }

    pub(crate) fn resolve(&self) -> Option<String> {
        match self {
            Self::Default => Some(DEFAULT_USER_AGENT.to_string()),
            Self::Disabled => None,
            Self::Value(user_agent) => Some(user_agent.clone()),
            Self::Transform(transform) => transform(DEFAULT_USER_AGENT),
        }
    }
}

impl From<String> for UserAgent {
    fn from(user_agent: String) -> Self {
        Self::Value(user_agent)
    }
}

impl From<&str> for UserAgent {
    fn from(user_agent: &str) -> Self {
        Self::Value(user_agent.to_string())
    }
}

impl fmt::Debug for UserAgent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Default => f.write_str("UserAgent::Default"),
            Self::Disabled => f.write_str("UserAgent::Disabled"),
            Self::Value(value) => f.debug_tuple("UserAgent::Value").field(value).finish(),
            Self::Transform(_) => f.write_str("UserAgent::Transform(<callback>)"),
        }
    }
}

pub(crate) fn apply_user_agent(
    request: &mut Request,
    user_agent: &UserAgent,
) -> Result<(), BoxError> {
    let headers = request.headers_mut();
    headers.remove("user-agent");

    if let Some(user_agent) = user_agent.resolve() {
        headers.try_insert("user-agent", user_agent)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_user_agent_uses_crate_version() {
        assert_eq!(
            DEFAULT_USER_AGENT,
            concat!(
                "scylladb-alternator-client-rust/",
                env!("CARGO_PKG_VERSION")
            )
        );
    }

    #[test]
    fn user_agent_variants_resolve_final_header_value() {
        assert_eq!(
            UserAgent::default().resolve().as_deref(),
            Some(DEFAULT_USER_AGENT)
        );
        assert_eq!(UserAgent::disabled().resolve(), None);
        assert_eq!(
            UserAgent::value("my-client/1.2.3").resolve().as_deref(),
            Some("my-client/1.2.3")
        );
        assert_eq!(
            UserAgent::transform(|default| format!("{default} my-app/4.5.6"))
                .resolve()
                .as_deref(),
            Some(concat!(
                "scylladb-alternator-client-rust/",
                env!("CARGO_PKG_VERSION"),
                " my-app/4.5.6"
            ))
        );
        assert_eq!(UserAgent::transform_optional(|_| None).resolve(), None);
    }

    #[test]
    fn apply_user_agent_replaces_existing_user_agent() {
        let mut request = Request::empty();
        request.headers_mut().insert("user-agent", "aws-sdk-rust/1");

        apply_user_agent(&mut request, &UserAgent::value("custom/1")).unwrap();

        assert_eq!(request.headers().get("user-agent"), Some("custom/1"));
    }

    #[test]
    fn apply_user_agent_removes_existing_user_agent_when_disabled() {
        let mut request = Request::empty();
        request.headers_mut().insert("user-agent", "aws-sdk-rust/1");

        apply_user_agent(&mut request, &UserAgent::disabled()).unwrap();

        assert!(!request.headers().contains_key("user-agent"));
    }
}
