//! Dar es Salaam Stock Exchange (DSE) market data provider.
//!
//! Fetches Tanzanian equity data from an external DSE API service.
//! Default base URL: `http://localhost:9090`
//! Auth: API key via `X-API-Key` header (optional).

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use log::{debug, warn};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::errors::MarketDataError;
use crate::models::{
    AssetProfile, Coverage, InstrumentKind, ProviderInstrument, Quote, QuoteContext, SearchResult,
};
use crate::provider::capabilities::{ProviderCapabilities, RateLimit};
use crate::provider::traits::MarketDataProvider;
use crate::resolver::{ResolverChain, SymbolResolver};

const DEFAULT_BASE_URL: &str = "http://localhost:9090";
const PROVIDER_ID: &str = "DSE";

// ── API response types ──────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct LatestQuoteResponse {
    #[allow(dead_code)]
    symbol: Option<String>,
    close: f64,
    open: Option<f64>,
    high: Option<f64>,
    low: Option<f64>,
    volume: Option<f64>,
    currency: Option<String>,
    timestamp: Option<String>,
}

#[derive(Debug, Deserialize)]
struct HistoricalResponse {
    #[allow(dead_code)]
    symbol: Option<String>,
    quotes: Vec<HistoricalQuote>,
    currency: Option<String>,
}

#[derive(Debug, Deserialize)]
struct HistoricalQuote {
    date: String,
    open: Option<f64>,
    high: Option<f64>,
    low: Option<f64>,
    close: f64,
    volume: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct SearchResponse {
    results: Vec<SearchItem>,
}

#[derive(Debug, Deserialize)]
struct SearchItem {
    symbol: String,
    name: String,
    #[serde(rename = "type", default = "default_equity")]
    asset_type: String,
    currency: Option<String>,
}

fn default_equity() -> String {
    "EQUITY".to_string()
}

#[derive(Debug, Deserialize)]
struct ProfileResponse {
    name: Option<String>,
    sector: Option<String>,
    industry: Option<String>,
    country: Option<String>,
    description: Option<String>,
    website: Option<String>,
    market_cap: Option<f64>,
    employees: Option<u64>,
    logo_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ErrorResponse {
    #[serde(alias = "message")]
    error: Option<String>,
}

// ── Provider ────────────────────────────────────────────────────────────────

pub struct DseProvider {
    client: Client,
    base_url: String,
    api_key: String,
}

impl DseProvider {
    pub fn new(api_key: String) -> Self {
        Self::with_base_url(api_key, DEFAULT_BASE_URL.to_string())
    }

    pub fn with_base_url(api_key: String, base_url: String) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap_or_else(|_| Client::new());

        Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key,
        }
    }

    /// Shared HTTP fetch with auth and error handling.
    async fn fetch(&self, path: &str) -> Result<String, MarketDataError> {
        let url = format!("{}{}", self.base_url, path);

        debug!("DSE request: {}", path);

        let mut request = self.client.get(&url);
        if !self.api_key.is_empty() {
            request = request.header("X-API-Key", &self.api_key);
        }
        let response = request
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    MarketDataError::Timeout {
                        provider: PROVIDER_ID.to_string(),
                    }
                } else {
                    MarketDataError::ProviderError {
                        provider: PROVIDER_ID.to_string(),
                        message: format!("Request failed: {}", e),
                    }
                }
            })?;

        let status = response.status();
        debug!("DSE response status: {} for {}", status, path);

        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(MarketDataError::RateLimited {
                provider: PROVIDER_ID.to_string(),
            });
        }

        if status == reqwest::StatusCode::UNAUTHORIZED {
            return Err(MarketDataError::ProviderError {
                provider: PROVIDER_ID.to_string(),
                message: "Invalid or missing API key".to_string(),
            });
        }

        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            if let Ok(err) = serde_json::from_str::<ErrorResponse>(&body) {
                if let Some(msg) = err.error {
                    return Err(MarketDataError::ProviderError {
                        provider: PROVIDER_ID.to_string(),
                        message: msg,
                    });
                }
            }
            return Err(MarketDataError::ProviderError {
                provider: PROVIDER_ID.to_string(),
                message: format!("HTTP {} - {}", status, body),
            });
        }

        response
            .text()
            .await
            .map_err(|e| MarketDataError::ProviderError {
                provider: PROVIDER_ID.to_string(),
                message: format!("Failed to read response: {}", e),
            })
    }

    fn extract_symbol(&self, instrument: &ProviderInstrument) -> Result<String, MarketDataError> {
        match instrument {
            ProviderInstrument::EquitySymbol { symbol } => Ok(symbol.to_string()),
            _ => Err(MarketDataError::UnsupportedAssetType(
                "DSE only supports equities".to_string(),
            )),
        }
    }

    fn get_currency(&self, context: &QuoteContext) -> String {
        let chain = ResolverChain::new();
        chain
            .get_currency(&PROVIDER_ID.into(), context)
            .or_else(|| context.currency_hint.clone())
            .map(|c| c.to_string())
            .unwrap_or_else(|| "TZS".to_string())
    }

    async fn fetch_latest_quote(
        &self,
        symbol: &str,
        currency: &str,
    ) -> Result<Quote, MarketDataError> {
        let path = format!(
            "/api/v1/quotes/{}/latest",
            urlencoding::encode(symbol)
        );
        let text = self.fetch(&path).await?;

        let resp: LatestQuoteResponse =
            serde_json::from_str(&text).map_err(|e| MarketDataError::ProviderError {
                provider: PROVIDER_ID.to_string(),
                message: format!("Failed to parse quote response: {}", e),
            })?;

        if resp.close == 0.0 {
            return Err(MarketDataError::SymbolNotFound(format!(
                "No quote data for symbol: {}",
                symbol
            )));
        }

        let close = Decimal::try_from(resp.close).map_err(|_| {
            MarketDataError::ValidationFailed {
                message: format!("Invalid close price: {}", resp.close),
            }
        })?;

        let timestamp = resp
            .timestamp
            .as_deref()
            .and_then(|ts| DateTime::parse_from_rfc3339(ts).ok())
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(Utc::now);

        let quote_currency = resp.currency.as_deref().unwrap_or(currency);

        Ok(Quote {
            timestamp,
            open: resp.open.and_then(|v| Decimal::try_from(v).ok()),
            high: resp.high.and_then(|v| Decimal::try_from(v).ok()),
            low: resp.low.and_then(|v| Decimal::try_from(v).ok()),
            close,
            volume: resp.volume.and_then(|v| Decimal::try_from(v).ok()),
            currency: quote_currency.to_string(),
            source: PROVIDER_ID.to_string(),
        })
    }

    async fn fetch_historical_quotes(
        &self,
        symbol: &str,
        currency: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<Quote>, MarketDataError> {
        let path = format!(
            "/api/v1/quotes/{}/history?start={}&end={}",
            urlencoding::encode(symbol),
            start.format("%Y-%m-%d"),
            end.format("%Y-%m-%d"),
        );
        let text = self.fetch(&path).await?;

        let resp: HistoricalResponse =
            serde_json::from_str(&text).map_err(|e| MarketDataError::ProviderError {
                provider: PROVIDER_ID.to_string(),
                message: format!("Failed to parse historical response: {}", e),
            })?;

        if resp.quotes.is_empty() {
            return Err(MarketDataError::NoDataForRange);
        }

        let resp_currency = resp.currency.as_deref().unwrap_or(currency);
        let mut quotes = Vec::with_capacity(resp.quotes.len());

        for q in &resp.quotes {
            let date = match NaiveDate::parse_from_str(&q.date, "%Y-%m-%d") {
                Ok(d) => d,
                Err(_) => {
                    warn!("DSE: invalid date format: {}", q.date);
                    continue;
                }
            };

            let timestamp = date
                .and_hms_opt(14, 0, 0)
                .unwrap()
                .and_utc();

            let close = match Decimal::try_from(q.close) {
                Ok(d) => d,
                Err(_) => {
                    warn!("DSE: invalid close price on {}: {}", q.date, q.close);
                    continue;
                }
            };

            quotes.push(Quote {
                timestamp,
                open: q.open.and_then(|v| Decimal::try_from(v).ok()),
                high: q.high.and_then(|v| Decimal::try_from(v).ok()),
                low: q.low.and_then(|v| Decimal::try_from(v).ok()),
                close,
                volume: q.volume.and_then(|v| Decimal::try_from(v).ok()),
                currency: resp_currency.to_string(),
                source: PROVIDER_ID.to_string(),
            });
        }

        quotes.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        Ok(quotes)
    }

    async fn fetch_asset_profile(&self, symbol: &str) -> Result<AssetProfile, MarketDataError> {
        let path = format!(
            "/api/v1/symbols/{}/profile",
            urlencoding::encode(symbol)
        );
        let text = self.fetch(&path).await?;

        if text.trim() == "{}" {
            return Err(MarketDataError::SymbolNotFound(format!(
                "No profile data for symbol: {}",
                symbol
            )));
        }

        let resp: ProfileResponse =
            serde_json::from_str(&text).map_err(|e| MarketDataError::ProviderError {
                provider: PROVIDER_ID.to_string(),
                message: format!("Failed to parse profile response: {}", e),
            })?;

        if resp.name.is_none() {
            return Err(MarketDataError::SymbolNotFound(format!(
                "No profile data for symbol: {}",
                symbol
            )));
        }

        Ok(AssetProfile {
            source: Some(PROVIDER_ID.to_string()),
            name: resp.name,
            quote_type: Some("EQUITY".to_string()),
            sector: resp.sector,
            sectors: None,
            industry: resp.industry,
            website: resp.website,
            description: resp.description,
            country: resp.country,
            employees: resp.employees,
            logo_url: resp.logo_url,
            market_cap: resp.market_cap,
            pe_ratio: None,
            dividend_yield: None,
            week_52_high: None,
            week_52_low: None,
        })
    }

    async fn search_symbols(&self, query: &str) -> Result<Vec<SearchResult>, MarketDataError> {
        let path = format!(
            "/api/v1/symbols/search?query={}",
            urlencoding::encode(query)
        );
        let text = self.fetch(&path).await?;

        let resp: SearchResponse =
            serde_json::from_str(&text).map_err(|e| MarketDataError::ProviderError {
                provider: PROVIDER_ID.to_string(),
                message: format!("Failed to parse search response: {}", e),
            })?;

        Ok(resp
            .results
            .into_iter()
            .map(|item| {
                SearchResult::new(&item.symbol, &item.name, "DSE", &item.asset_type)
                    .with_exchange_mic("XDAR")
                    .with_exchange_name("Dar es Salaam Stock Exchange")
                    .with_currency(item.currency.as_deref().unwrap_or("TZS"))
                    .with_data_source(PROVIDER_ID)
            })
            .collect())
    }
}

// ── Trait impl ──────────────────────────────────────────────────────────────

#[async_trait]
impl MarketDataProvider for DseProvider {
    fn id(&self) -> &'static str {
        PROVIDER_ID
    }

    fn priority(&self) -> u8 {
        5
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            instrument_kinds: &[InstrumentKind::Equity],
            coverage: Coverage {
                equity_mic_allow: Some(&["XDAR"]),
                equity_mic_deny: None,
                allow_unknown_mic: true,
                metal_quote_ccy_allow: None,
            },
            supports_latest: true,
            supports_historical: true,
            supports_search: true,
            supports_profile: true,
        }
    }

    fn rate_limit(&self) -> RateLimit {
        RateLimit {
            requests_per_minute: 120,
            max_concurrency: 5,
            min_delay: Duration::from_millis(100),
        }
    }

    async fn get_latest_quote(
        &self,
        context: &QuoteContext,
        instrument: ProviderInstrument,
    ) -> Result<Quote, MarketDataError> {
        let symbol = self.extract_symbol(&instrument)?;
        let currency = self.get_currency(context);
        debug!("Fetching latest quote for {} from DSE", symbol);
        self.fetch_latest_quote(&symbol, &currency).await
    }

    async fn get_historical_quotes(
        &self,
        context: &QuoteContext,
        instrument: ProviderInstrument,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<Quote>, MarketDataError> {
        let symbol = self.extract_symbol(&instrument)?;
        let currency = self.get_currency(context);
        debug!(
            "Fetching historical quotes for {} from DSE ({} to {})",
            symbol,
            start.format("%Y-%m-%d"),
            end.format("%Y-%m-%d")
        );
        let quotes = self
            .fetch_historical_quotes(&symbol, &currency, start, end)
            .await?;
        if quotes.is_empty() {
            return Err(MarketDataError::NoDataForRange);
        }
        Ok(quotes)
    }

    async fn search(&self, query: &str) -> Result<Vec<SearchResult>, MarketDataError> {
        debug!("Searching DSE for '{}'", query);
        self.search_symbols(query).await
    }

    async fn get_profile(&self, symbol: &str) -> Result<AssetProfile, MarketDataError> {
        debug!("Fetching profile for {} from DSE", symbol);
        self.fetch_asset_profile(symbol).await
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_provider_id() {
        let provider = DseProvider::new("test-key".to_string());
        assert_eq!(provider.id(), "DSE");
    }

    #[test]
    fn test_provider_priority() {
        let provider = DseProvider::new("test-key".to_string());
        assert_eq!(provider.priority(), 5);
    }

    #[test]
    fn test_capabilities() {
        let provider = DseProvider::new("test-key".to_string());
        let caps = provider.capabilities();
        assert!(caps.supports_latest);
        assert!(caps.supports_historical);
        assert!(caps.supports_search);
        assert!(caps.supports_profile);
        assert_eq!(caps.instrument_kinds, &[InstrumentKind::Equity]);
    }

    #[test]
    fn test_extract_symbol_equity() {
        let provider = DseProvider::new("test-key".to_string());
        let instrument = ProviderInstrument::EquitySymbol {
            symbol: "TCC".into(),
        };
        assert_eq!(provider.extract_symbol(&instrument).unwrap(), "TCC");
    }

    #[test]
    fn test_extract_symbol_unsupported() {
        let provider = DseProvider::new("test-key".to_string());
        let instrument = ProviderInstrument::FxPair {
            from: "USD".into(),
            to: "TZS".into(),
        };
        assert!(provider.extract_symbol(&instrument).is_err());
    }

    #[test]
    fn test_parse_latest_quote_response() {
        let json = r#"{
            "symbol": "TCC",
            "close": 3200.0,
            "open": 3150.0,
            "high": 3250.0,
            "low": 3100.0,
            "volume": 15000,
            "currency": "TZS",
            "timestamp": "2026-02-10T14:00:00Z"
        }"#;

        let resp: LatestQuoteResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.close, 3200.0);
        assert_eq!(resp.open, Some(3150.0));
        assert_eq!(resp.currency, Some("TZS".to_string()));
    }

    #[test]
    fn test_parse_historical_response() {
        let json = r#"{
            "symbol": "TCC",
            "quotes": [
                { "date": "2025-01-02", "open": 3000, "high": 3050, "low": 2980, "close": 3020, "volume": 12000 },
                { "date": "2025-01-03", "close": 3040 }
            ],
            "currency": "TZS"
        }"#;

        let resp: HistoricalResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.quotes.len(), 2);
        assert_eq!(resp.quotes[0].close, 3020.0);
        assert!(resp.quotes[1].open.is_none());
    }

    #[test]
    fn test_parse_search_response() {
        let json = r#"{
            "results": [
                { "symbol": "TCC", "name": "Tanzania Cigarette Company", "type": "EQUITY", "currency": "TZS" },
                { "symbol": "CRDB", "name": "CRDB Bank", "currency": "TZS" }
            ]
        }"#;

        let resp: SearchResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.results.len(), 2);
        assert_eq!(resp.results[0].symbol, "TCC");
        assert_eq!(resp.results[1].asset_type, "EQUITY"); // default
    }

    #[test]
    fn test_parse_profile_response() {
        let json = r#"{
            "name": "CRDB Bank Plc",
            "sector": "Financial Services",
            "industry": "Banking",
            "country": "TZ",
            "description": "CRDB Bank Plc is a commercial bank in Tanzania.",
            "website": "https://www.crdbbank.co.tz",
            "market_cap": 1500000000000.0,
            "employees": 3500
        }"#;

        let resp: ProfileResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.name, Some("CRDB Bank Plc".to_string()));
        assert_eq!(resp.sector, Some("Financial Services".to_string()));
        assert_eq!(resp.country, Some("TZ".to_string()));
        assert_eq!(resp.employees, Some(3500));
    }

    #[test]
    fn test_parse_profile_response_minimal() {
        let json = r#"{ "name": "NMB Bank Plc" }"#;

        let resp: ProfileResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.name, Some("NMB Bank Plc".to_string()));
        assert!(resp.sector.is_none());
        assert!(resp.market_cap.is_none());
    }

    #[test]
    fn test_base_url_trailing_slash() {
        let provider = DseProvider::with_base_url(
            "key".to_string(),
            "http://localhost:8080/".to_string(),
        );
        assert_eq!(provider.base_url, "http://localhost:8080");
    }
}
