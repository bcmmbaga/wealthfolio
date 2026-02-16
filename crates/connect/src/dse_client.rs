//! DSE (Dar es Salaam Stock Exchange) broker API client.
//!
//! Implements `BrokerApiClient` by calling the user's local DSE API service.
//! Auth: `X-API-Key` header. Default base URL: `http://localhost:9090`.

use async_trait::async_trait;
use log::{debug, info};
use reqwest::Client;
use serde::Deserialize;
use std::time::Duration;

use crate::broker::{
    BrokerAccount, BrokerBrokerage, BrokerConnection, BrokerConnectionBrokerage,
    BrokerHoldingsResponse, HoldingsBalance, HoldingsCurrency, HoldingsInnerSymbol,
    HoldingsPosition, HoldingsSymbol, PaginatedUniversalActivity, PaginationDetails,
};
use crate::broker::{
    AccountUniversalActivity, AccountUniversalActivityCurrency, AccountUniversalActivitySymbol,
    BrokerApiClient,
};
use wealthfolio_core::errors::{Error, Result};

const DEFAULT_BASE_URL: &str = "http://localhost:9090";
const DSE_CONNECTION_ID: &str = "DSE";

// ── DSE API response types ──────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct DseAccountsResponse {
    #[serde(default)]
    accounts: Vec<DseAccount>,
}

#[derive(Debug, Deserialize)]
struct DseAccount {
    id: String,
    name: Option<String>,
    account_number: Option<String>,
    currency: Option<String>,
    status: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DseActivitiesResponse {
    #[serde(default, deserialize_with = "deserialize_null_as_empty")]
    data: Vec<DseActivity>,
    pagination: Option<DsePagination>,
}

fn deserialize_null_as_empty<'de, D, T>(deserializer: D) -> std::result::Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::de::DeserializeOwned,
{
    Option::<Vec<T>>::deserialize(deserializer).map(|opt| opt.unwrap_or_default())
}

#[derive(Debug, Deserialize)]
struct DsePagination {
    offset: Option<i64>,
    limit: Option<i64>,
    total: Option<i64>,
    has_more: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct DseActivity {
    id: Option<String>,
    activity_type: Option<String>,
    symbol: Option<String>,
    symbol_name: Option<String>,
    quantity: Option<f64>,
    price: Option<f64>,
    amount: Option<f64>,
    fee: Option<f64>,
    currency: Option<String>,
    trade_date: Option<String>,
    settlement_date: Option<String>,
    description: Option<String>,
    external_reference_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DseHoldingsResponse {
    #[serde(default)]
    balances: Vec<DseBalance>,
    #[serde(default)]
    positions: Vec<DsePosition>,
}

#[derive(Debug, Deserialize)]
struct DseBalance {
    currency: Option<String>,
    cash: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct DsePosition {
    symbol: Option<String>,
    name: Option<String>,
    quantity: Option<f64>,
    price: Option<f64>,
    average_cost: Option<f64>,
    currency: Option<String>,
}

// ── Client ──────────────────────────────────────────────────────────────────

pub struct DseBrokerApiClient {
    client: Client,
    base_url: String,
    api_key: String,
}

impl DseBrokerApiClient {
    pub fn new(api_key: String) -> Self {
        let base_url = std::env::var("DSE_API_URL")
            .ok()
            .map(|v| v.trim().trim_end_matches('/').to_string())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| DEFAULT_BASE_URL.to_string());

        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("failed to build HTTP client");

        Self {
            client,
            base_url,
            api_key,
        }
    }

    async fn get<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        let url = format!("{}{}", self.base_url, path);
        debug!("DSE broker request: {}", url);

        let resp: reqwest::Response = self
            .client
            .get(&url)
            .header("X-API-Key", &self.api_key)
            .send()
            .await
            .map_err(|e| Error::Unexpected(format!("DSE broker request failed: {}", e)))?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Unexpected(format!(
                "DSE broker API error ({}): {}",
                status, body
            )));
        }

        resp.json::<T>()
            .await
            .map_err(|e| Error::Unexpected(format!("DSE broker response parse error: {}", e)))
    }
}

#[async_trait]
impl BrokerApiClient for DseBrokerApiClient {
    async fn list_connections(&self) -> Result<Vec<BrokerConnection>> {
        // DSE doesn't use OAuth connections. Return a synthetic connection
        // so the orchestrator creates a "DSE" platform entry.
        Ok(vec![BrokerConnection {
            id: DSE_CONNECTION_ID.to_string(),
            brokerage: Some(BrokerConnectionBrokerage {
                id: Some(DSE_CONNECTION_ID.to_string()),
                slug: Some(DSE_CONNECTION_ID.to_string()),
                name: Some("Dar es Salaam Stock Exchange".to_string()),
                display_name: Some("DSE".to_string()),
                aws_s3_logo_url: None,
                aws_s3_square_logo_url: None,
            }),
            connection_type: Some("read".to_string()),
            status: Some("connected".to_string()),
            disabled: false,
            disabled_date: None,
            updated_at: None,
            name: Some("DSE".to_string()),
        }])
    }

    async fn list_accounts(
        &self,
        _authorization_ids: Option<Vec<String>>,
    ) -> Result<Vec<BrokerAccount>> {
        info!("Fetching accounts from DSE broker API...");
        let resp: DseAccountsResponse = self.get("/api/v1/broker/accounts").await?;

        let accounts = resp
            .accounts
            .into_iter()
            .map(|a| BrokerAccount {
                id: Some(a.id),
                name: a.name,
                account_number: a.account_number,
                currency: a.currency,
                status: a.status,
                institution_name: Some("DSE".to_string()),
                provider: Some("DSE".to_string()),
                sync_enabled: true,
                ..Default::default()
            })
            .collect();

        Ok(accounts)
    }

    async fn list_brokerages(&self) -> Result<Vec<BrokerBrokerage>> {
        Ok(vec![])
    }

    async fn get_account_activities(
        &self,
        account_id: &str,
        start_date: Option<&str>,
        end_date: Option<&str>,
        offset: Option<i64>,
        limit: Option<i64>,
    ) -> Result<PaginatedUniversalActivity> {
        let mut path = format!("/api/v1/broker/accounts/{}/activities?", account_id);
        if let Some(s) = start_date {
            path.push_str(&format!("start_date={}&", s));
        }
        if let Some(e) = end_date {
            path.push_str(&format!("end_date={}&", e));
        }
        if let Some(o) = offset {
            path.push_str(&format!("offset={}&", o));
        }
        if let Some(l) = limit {
            path.push_str(&format!("limit={}&", l));
        }
        // Remove trailing & or ?
        let path = path.trim_end_matches(&['&', '?']).to_string();

        info!("Fetching activities from DSE broker API: {}", path);
        let resp: DseActivitiesResponse = self.get(&path).await?;

        let data = resp
            .data
            .into_iter()
            .map(|a| {
                let currency_code = a.currency.clone();
                AccountUniversalActivity {
                    id: a.id,
                    activity_type: a.activity_type,
                    symbol: a.symbol.as_ref().map(|sym| AccountUniversalActivitySymbol {
                        symbol: Some(sym.clone()),
                        raw_symbol: Some(sym.clone()),
                        description: a.symbol_name.clone(),
                        ..Default::default()
                    }),
                    units: a.quantity,
                    price: a.price,
                    amount: a.amount,
                    fee: a.fee,
                    currency: currency_code.map(|code| AccountUniversalActivityCurrency {
                        code: Some(code),
                        ..Default::default()
                    }),
                    trade_date: a.trade_date.map(|d| format!("{}T00:00:00Z", d)),
                    settlement_date: a.settlement_date.map(|d| format!("{}T00:00:00Z", d)),
                    description: a.description,
                    external_reference_id: a.external_reference_id,
                    institution: Some("DSE".to_string()),
                    source_system: Some("DSE".to_string()),
                    source_record_id: None,
                    ..Default::default()
                }
            })
            .collect();

        let pagination = resp.pagination.map(|p| PaginationDetails {
            offset: p.offset,
            limit: p.limit,
            total: p.total,
            has_more: p.has_more,
        });

        Ok(PaginatedUniversalActivity { data, pagination })
    }

    async fn get_account_holdings(&self, account_id: &str) -> Result<BrokerHoldingsResponse> {
        info!("Fetching holdings from DSE broker API for {}", account_id);
        let resp: DseHoldingsResponse = self
            .get(&format!("/api/v1/broker/accounts/{}/holdings", account_id))
            .await?;

        let balances = resp
            .balances
            .into_iter()
            .map(|b| HoldingsBalance {
                currency: b.currency.map(|c| HoldingsCurrency {
                    code: Some(c),
                    ..Default::default()
                }),
                cash: b.cash,
                buying_power: None,
            })
            .collect();

        let positions = resp
            .positions
            .into_iter()
            .map(|p| HoldingsPosition {
                symbol: Some(HoldingsSymbol {
                    symbol: Some(HoldingsInnerSymbol {
                        symbol: p.symbol.clone(),
                        raw_symbol: p.symbol,
                        name: p.name,
                        currency: p.currency.as_ref().map(|c| HoldingsCurrency {
                            code: Some(c.clone()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                units: p.quantity,
                price: p.price,
                average_purchase_price: p.average_cost,
                currency: p.currency.map(|c| HoldingsCurrency {
                    code: Some(c),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .collect();

        Ok(BrokerHoldingsResponse {
            account: None,
            balances: Some(balances),
            positions: Some(positions),
            option_positions: None,
        })
    }
}
