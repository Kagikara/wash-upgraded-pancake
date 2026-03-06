use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use rust_decimal::Decimal;
use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunMode {
    ReviewOnly,
    Clean,
    Full,
}

impl RunMode {
    fn parse(raw: &str) -> Result<Self, ConfigError> {
        match raw {
            "review-only" => Ok(Self::ReviewOnly),
            "clean" => Ok(Self::Clean),
            "full" => Ok(Self::Full),
            _ => Err(ConfigError::Schema(format!("invalid mode: {raw}"))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputFormat {
    Csv,
    Parquet,
}

impl InputFormat {
    fn parse(raw: &str) -> Result<Self, ConfigError> {
        match raw {
            "csv" => Ok(Self::Csv),
            "parquet" => Ok(Self::Parquet),
            _ => Err(ConfigError::Schema(format!("invalid input.format: {raw}"))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputSchemaMap {
    pub date: String,
    pub ticker: String,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub vwap: String,
    pub volume: String,
    pub turnover: String,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputConfig {
    pub path: PathBuf,
    pub format: InputFormat,
    pub schema: InputSchemaMap,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CalendarConfig {
    pub trading_calendar_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleSourceConfig {
    pub path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleSwitchConfig {
    pub enabled_categories: Vec<String>,
    pub enabled_rules: Vec<String>,
    pub disabled_rules: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PolicyConfig {
    pub rule_name: String,
    pub action: String,
    pub params: serde_yaml::Value,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct HandlingConfig {
    pub policies: Vec<PolicyConfig>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LoadConfig {
    pub mode: RunMode,
    pub input: InputConfig,
    pub calendar: CalendarConfig,
    pub market_rules: RuleSourceConfig,
    pub corporate_actions: Option<PathBuf>,
    pub lifecycle_map: Option<PathBuf>,
    pub rules: RuleSwitchConfig,
    pub handling: HandlingConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TradeStatus {
    Normal,
    Halted,
    Delisted,
    Other(String),
}

impl TradeStatus {
    fn parse(raw: &str) -> Self {
        match raw.to_ascii_uppercase().as_str() {
            "NORMAL" => Self::Normal,
            "HALTED" => Self::Halted,
            "DELISTED" => Self::Delisted,
            _ => Self::Other(raw.to_string()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    pub date: String,
    pub ticker: String,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub vwap: Decimal,
    pub volume: i64,
    pub turnover: i64,
    pub status: TradeStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoadErrorCode {
    ParseFail,
    TypeCastFail,
    MissingField,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadError {
    pub stage: &'static str,
    pub row_number: usize,
    pub raw_row: String,
    pub error_code: LoadErrorCode,
    pub error_detail: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LoadOutput {
    pub records: Vec<Record>,
    pub load_errors: Vec<LoadError>,
}

pub trait RuleRegistry {
    fn all_rules(&self) -> HashSet<String>;
    fn all_categories(&self) -> HashSet<String>;
}

#[derive(Debug, Clone)]
pub struct StaticRuleRegistry {
    rules: HashSet<String>,
    categories: HashSet<String>,
}

impl StaticRuleRegistry {
    pub fn new(rules: Vec<&str>, categories: Vec<&str>) -> Self {
        Self {
            rules: rules.into_iter().map(str::to_string).collect(),
            categories: categories.into_iter().map(str::to_string).collect(),
        }
    }
}

impl RuleRegistry for StaticRuleRegistry {
    fn all_rules(&self) -> HashSet<String> {
        self.rules.clone()
    }

    fn all_categories(&self) -> HashSet<String> {
        self.categories.clone()
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("config file not found: {0}")]
    NotFound(String),
    #[error("invalid yaml syntax: {0}")]
    InvalidYaml(String),
    #[error("schema validation failed: {0}")]
    Schema(String),
    #[error("unknown category: {0}")]
    UnknownCategory(String),
    #[error("unknown rule: {0}")]
    UnknownRule(String),
    #[error("policy references unknown rule: {0}")]
    UnknownPolicyRule(String),
}

#[derive(Debug, Error)]
pub enum LoadStageError {
    #[error("open input failed: {0}")]
    OpenInput(String),
    #[error("unsupported input format: {0}")]
    UnsupportedFormat(String),
    #[error("csv read failed: {0}")]
    CsvRead(String),
}

#[derive(Debug, Deserialize)]
struct RawConfig {
    mode: String,
    input: RawInput,
    calendar: Option<RawCalendar>,
    market_rules: Option<RawPathNode>,
    corporate_actions: Option<RawPathNode>,
    lifecycle_map: Option<RawPathNode>,
    rules: RawRules,
    handling: Option<RawHandling>,
}

#[derive(Debug, Deserialize)]
struct RawInput {
    path: String,
    format: String,
    schema: RawSchema,
}

#[derive(Debug, Deserialize)]
struct RawSchema {
    date: String,
    ticker: String,
    open: String,
    high: String,
    low: String,
    close: String,
    vwap: String,
    volume: String,
    turnover: String,
    status: String,
}

#[derive(Debug, Deserialize)]
struct RawCalendar {
    trading_calendar_path: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawPathNode {
    path: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawRules {
    enabled_categories: Vec<String>,
    #[serde(default)]
    enabled_rules: Vec<String>,
    #[serde(default)]
    disabled_rules: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RawHandling {
    policies: Option<Vec<RawPolicy>>,
}

#[derive(Debug, Deserialize)]
struct RawPolicy {
    rule_name: String,
    action: String,
    #[serde(default)]
    params: serde_yaml::Value,
}

pub fn load_and_validate_config(path: &Path, registry: &dyn RuleRegistry) -> Result<LoadConfig, ConfigError> {
    if !path.exists() {
        return Err(ConfigError::NotFound(path.display().to_string()));
    }

    let content = fs::read_to_string(path).map_err(|e| ConfigError::InvalidYaml(e.to_string()))?;
    let raw: RawConfig = serde_yaml::from_str(&content).map_err(|e| ConfigError::InvalidYaml(e.to_string()))?;

    let mode = RunMode::parse(&raw.mode)?;
    let format = InputFormat::parse(&raw.input.format)?;

    let input_path = PathBuf::from(raw.input.path);
    if input_path.as_os_str().is_empty() {
        return Err(ConfigError::Schema("input.path cannot be empty".to_string()));
    }

    let schema = InputSchemaMap {
        date: raw.input.schema.date,
        ticker: raw.input.schema.ticker,
        open: raw.input.schema.open,
        high: raw.input.schema.high,
        low: raw.input.schema.low,
        close: raw.input.schema.close,
        vwap: raw.input.schema.vwap,
        volume: raw.input.schema.volume,
        turnover: raw.input.schema.turnover,
        status: raw.input.schema.status,
    };

    let calendar_path = raw
        .calendar
        .and_then(|n| n.trading_calendar_path)
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "data/default_trading_calendar.csv".to_string());

    let market_rules_path = raw
        .market_rules
        .and_then(|n| n.path)
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "data/default_market_rules.yaml".to_string());

    let corporate_actions = raw
        .corporate_actions
        .and_then(|n| n.path)
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from);

    let lifecycle_map = raw
        .lifecycle_map
        .and_then(|n| n.path)
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from);

    let all_categories = registry.all_categories();
    for c in &raw.rules.enabled_categories {
        if !all_categories.contains(c) {
            return Err(ConfigError::UnknownCategory(c.clone()));
        }
    }

    let all_rules = registry.all_rules();
    for r in &raw.rules.enabled_rules {
        if !all_rules.contains(r) {
            return Err(ConfigError::UnknownRule(r.clone()));
        }
    }
    for r in &raw.rules.disabled_rules {
        if !all_rules.contains(r) {
            return Err(ConfigError::UnknownRule(r.clone()));
        }
    }

    let mut policies = Vec::new();
    if let Some(handling) = raw.handling {
        if let Some(raw_policies) = handling.policies {
            for p in raw_policies {
                if !all_rules.contains(&p.rule_name) {
                    return Err(ConfigError::UnknownPolicyRule(p.rule_name));
                }
                policies.push(PolicyConfig {
                    rule_name: p.rule_name,
                    action: p.action,
                    params: p.params,
                });
            }
        }
    }

    Ok(LoadConfig {
        mode,
        input: InputConfig {
            path: input_path,
            format,
            schema,
        },
        calendar: CalendarConfig {
            trading_calendar_path: PathBuf::from(calendar_path),
        },
        market_rules: RuleSourceConfig {
            path: PathBuf::from(market_rules_path),
        },
        corporate_actions,
        lifecycle_map,
        rules: RuleSwitchConfig {
            enabled_categories: raw.rules.enabled_categories,
            enabled_rules: raw.rules.enabled_rules,
            disabled_rules: raw.rules.disabled_rules,
        },
        handling: HandlingConfig { policies },
    })
}

pub fn load_data(cfg: &LoadConfig) -> Result<LoadOutput, LoadStageError> {
    match cfg.input.format {
        InputFormat::Csv => load_csv_data(cfg),
        InputFormat::Parquet => Err(LoadStageError::UnsupportedFormat("parquet".to_string())),
    }
}

fn load_csv_data(cfg: &LoadConfig) -> Result<LoadOutput, LoadStageError> {
    let path = &cfg.input.path;
    if !path.exists() {
        return Err(LoadStageError::OpenInput(path.display().to_string()));
    }

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(path)
        .map_err(|e| LoadStageError::CsvRead(e.to_string()))?;

    let headers = reader
        .headers()
        .map_err(|e| LoadStageError::CsvRead(e.to_string()))?
        .clone();

    let mut header_index = HashMap::new();
    for (idx, col) in headers.iter().enumerate() {
        header_index.insert(col.to_string(), idx);
    }

    let mut records = Vec::new();
    let mut load_errors = Vec::new();

    for (idx, row) in reader.records().enumerate() {
        let row_number = idx + 1;
        match row {
            Ok(rec) => match parse_csv_row(&rec, &header_index, &cfg.input.schema, row_number) {
                Ok(parsed) => records.push(parsed),
                Err(err) => load_errors.push(err),
            },
            Err(err) => load_errors.push(LoadError {
                stage: "LOAD",
                row_number,
                raw_row: String::new(),
                error_code: LoadErrorCode::ParseFail,
                error_detail: err.to_string(),
            }),
        }
    }

    Ok(LoadOutput {
        records,
        load_errors,
    })
}

fn parse_csv_row(
    row: &csv::StringRecord,
    header_index: &HashMap<String, usize>,
    schema: &InputSchemaMap,
    row_number: usize,
) -> Result<Record, LoadError> {
    let raw_row = row.iter().collect::<Vec<_>>().join(",");

    let get = |column: &str| -> Result<&str, LoadError> {
        let idx = header_index.get(column).copied().ok_or_else(|| LoadError {
            stage: "LOAD",
            row_number,
            raw_row: raw_row.clone(),
            error_code: LoadErrorCode::MissingField,
            error_detail: format!("column not found in header: {column}"),
        })?;

        row.get(idx).ok_or_else(|| LoadError {
            stage: "LOAD",
            row_number,
            raw_row: raw_row.clone(),
            error_code: LoadErrorCode::MissingField,
            error_detail: format!("missing value for column: {column}"),
        })
    };

    let parse_decimal = |s: &str, field: &str| -> Result<Decimal, LoadError> {
        Decimal::from_str(s).map_err(|_| LoadError {
            stage: "LOAD",
            row_number,
            raw_row: raw_row.clone(),
            error_code: LoadErrorCode::TypeCastFail,
            error_detail: format!("invalid decimal for {field}: {s}"),
        })
    };

    let parse_i64 = |s: &str, field: &str| -> Result<i64, LoadError> {
        s.parse::<i64>().map_err(|_| LoadError {
            stage: "LOAD",
            row_number,
            raw_row: raw_row.clone(),
            error_code: LoadErrorCode::TypeCastFail,
            error_detail: format!("invalid int for {field}: {s}"),
        })
    };

    let date = get(&schema.date)?.to_string();
    let ticker = get(&schema.ticker)?.to_string();

    let open = parse_decimal(get(&schema.open)?, "open")?;
    let high = parse_decimal(get(&schema.high)?, "high")?;
    let low = parse_decimal(get(&schema.low)?, "low")?;
    let close = parse_decimal(get(&schema.close)?, "close")?;
    let vwap = parse_decimal(get(&schema.vwap)?, "vwap")?;

    let volume = parse_i64(get(&schema.volume)?, "volume")?;
    let turnover = parse_i64(get(&schema.turnover)?, "turnover")?;
    let status = TradeStatus::parse(get(&schema.status)?);

    Ok(Record {
        date,
        ticker,
        open,
        high,
        low,
        close,
        vwap,
        volume,
        turnover,
        status,
    })
}
