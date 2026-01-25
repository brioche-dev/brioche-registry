use axum::http::header::HeaderMap;
use bstr::ByteSlice as _;
use std::borrow::Cow;

const UNKNOWN_USER_AGENT: &str = "<unknown>";
const INVALID_USER_AGENT: &str = "<invalid>";

/// Extracts the client IP from headers based on proxy configuration.
#[must_use]
pub fn extract_forwarded_ip<'a>(
    headers: &'a HeaderMap,
    proxy_layers: usize,
    fallback_ip: Cow<'a, str>,
) -> Cow<'a, str> {
    headers
        .get_all("X-Forwarded-For")
        .into_iter()
        .flat_map(|header| header.as_bytes().split_str(","))
        .take(proxy_layers)
        .last()
        .map_or(fallback_ip, |value| String::from_utf8_lossy(value.trim()))
}

/// Extracts the user agent from headers.
#[must_use]
pub fn extract_user_agent(headers: &HeaderMap) -> Cow<'_, str> {
    headers
        .get("User-Agent")
        .map_or(Cow::Borrowed(UNKNOWN_USER_AGENT), |user_agent| {
            user_agent
                .to_str()
                .map_or(Cow::Borrowed(INVALID_USER_AGENT), Cow::Borrowed)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    const FALLBACK: &str = "<fallback>";

    fn headers_with_x_forwarded_for(values: &[&str]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for value in values {
            headers.append("X-Forwarded-For", HeaderValue::from_str(value).unwrap());
        }
        headers
    }

    #[test]
    fn extract_forwarded_ip_zero_proxy_layers_returns_fallback() {
        let headers = headers_with_x_forwarded_for(&["client-ip, proxy1-ip"]);

        let result = extract_forwarded_ip(&headers, 0, FALLBACK.into());
        assert_eq!(result.as_ref(), FALLBACK);
    }

    #[test]
    fn extract_forwarded_ip_selects_ip_based_on_proxy_layers() {
        let headers = headers_with_x_forwarded_for(&["client-ip, proxy1-ip, proxy2-ip"]);

        let result = extract_forwarded_ip(&headers, 1, FALLBACK.into());
        assert_eq!(result.as_ref(), "client-ip");

        let result = extract_forwarded_ip(&headers, 2, FALLBACK.into());
        assert_eq!(result.as_ref(), "proxy1-ip");

        let result = extract_forwarded_ip(&headers, 3, FALLBACK.into());
        assert_eq!(result.as_ref(), "proxy2-ip");
    }

    #[test]
    fn extract_forwarded_ip_more_layers_than_ips_returns_last_available() {
        let headers = headers_with_x_forwarded_for(&["client-ip, proxy1-ip"]);

        let result = extract_forwarded_ip(&headers, 5, FALLBACK.into());
        assert_eq!(result.as_ref(), "proxy1-ip");
    }

    #[test]
    fn extract_forwarded_ip_empty_header_returns_empty_string() {
        let headers = headers_with_x_forwarded_for(&[""]);

        let result = extract_forwarded_ip(&headers, 1, FALLBACK.into());
        assert_eq!(result.as_ref(), "");
    }

    #[test]
    fn extract_forwarded_ip_no_headers_returns_fallback() {
        let headers = HeaderMap::new();

        let result = extract_forwarded_ip(&headers, 1, FALLBACK.into());
        assert_eq!(result.as_ref(), FALLBACK);
    }

    #[test]
    fn extract_user_agent_valid() {
        let mut headers = HeaderMap::new();
        headers.insert("User-Agent", HeaderValue::from_static("user-agent"));

        let result = extract_user_agent(&headers);
        assert_eq!(result.as_ref(), "user-agent");
    }

    #[test]
    fn extract_user_agent_missing_returns_unknown() {
        let headers = HeaderMap::new();

        let result = extract_user_agent(&headers);
        assert_eq!(result.as_ref(), UNKNOWN_USER_AGENT);
    }

    #[test]
    fn extract_user_agent_invalid_utf8_returns_invalid() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "User-Agent",
            HeaderValue::from_bytes(&[0x80, 0x81]).unwrap(),
        );

        let result = extract_user_agent(&headers);
        assert_eq!(result.as_ref(), INVALID_USER_AGENT);
    }
}
