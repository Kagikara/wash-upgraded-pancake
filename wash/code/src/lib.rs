use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;
use std::time::Instant;

use rust_decimal::Decimal;
use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReviewChartType {
    IssueByDate,
    IssueByCategory,
    IssueByRule,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewChartConfig {
    pub enabled: bool,
    pub types: HashSet<ReviewChartType>,
}

impl Default for ReviewChartConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            types: HashSet::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewPreviewConfig {
    pub enabled: bool,
    pub sample_size: usize,
}

impl Default for ReviewPreviewConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            sample_size: 20,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewConfig {
    pub charts: ReviewChartConfig,
    pub preview: ReviewPreviewConfig,
    pub output_dir: PathBuf,
}

impl Default for ReviewConfig {
    fn default() -> Self {
        Self {
            charts: ReviewChartConfig::default(),
            preview: ReviewPreviewConfig::default(),
            output_dir: PathBuf::from("output/review"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReviewStats {
    pub total_issues: usize,
    pub ticker_count: usize,
    pub issue_by_date: HashMap<String, usize>,
    pub issue_by_category: HashMap<String, usize>,
    pub issue_by_rule: HashMap<String, usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewChart {
    pub chart_type: ReviewChartType,
    pub title: String,
    pub payload: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuggestedFix {
    pub action: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewPreviewItem {
    pub issue: Issue,
    pub suggested_fix: SuggestedFix,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReviewReport {
    pub stats: ReviewStats,
    pub charts: Vec<ReviewChart>,
    pub preview: Vec<ReviewPreviewItem>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewOutput {
    pub approved_issues: Vec<Issue>,
    pub disabled_issues: Vec<Issue>,
    pub review_report: ReviewReport,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DisableIssueRule {
    pub issue_types: HashSet<IssueType>,
    pub categories: HashSet<String>,
    pub rule_names: HashSet<String>,
    pub tickers: HashSet<String>,
    pub dates: HashSet<String>,
    pub fields: HashSet<String>,
}

impl DisableIssueRule {
    pub fn matches(&self, issue: &Issue) -> bool {
        if !self.issue_types.is_empty() && !self.issue_types.contains(&issue.issue_type) {
            return false;
        }
        if !self.categories.is_empty() && !self.categories.contains(&issue.category) {
            return false;
        }
        if !self.rule_names.is_empty() && !self.rule_names.contains(&issue.rule_name) {
            return false;
        }
        if !self.tickers.is_empty() && !self.tickers.contains(&issue.ticker) {
            return false;
        }
        if !self.dates.is_empty() && !self.dates.contains(&issue.date) {
            return false;
        }
        if !self.fields.is_empty() && !self.fields.contains(&issue.field) {
            return false;
        }
        true
    }
}

#[derive(Debug, Error)]
pub enum ReviewError {
    #[error("failed to load disabled issue rules: {0}")]
    DisabledRules(String),
    #[error("chart generation failed: {0}")]
    Chart(String),
    #[error("preview generation failed: {0}")]
    Preview(String),
    #[error("report persistence failed: {0}")]
    Persist(String),
}

pub trait DisabledIssueProvider: Send + Sync {
    fn load_disabled_rules(&self, config: &ReviewConfig) -> Result<Vec<DisableIssueRule>, ReviewError>;
}

pub trait ReviewChartRenderer: Send + Sync {
    fn render(&self, chart_type: ReviewChartType, issues: &[Issue], stats: &ReviewStats)
        -> Result<ReviewChart, ReviewError>;
}

pub trait ReviewPreviewEngine: Send + Sync {
    fn suggest_fix(&self, issue: &Issue) -> Result<SuggestedFix, ReviewError>;
}

pub trait ReviewReportStore: Send + Sync {
    fn save(&self, report: &ReviewReport, config: &ReviewConfig) -> Result<(), ReviewError>;
}

pub trait ReviewStage: Send + Sync {
    fn run(&self, issues: &[Issue], config: &ReviewConfig) -> Result<ReviewOutput, ReviewError>;
}

pub struct DefaultReviewStage<P, C, V, S>
where
    P: DisabledIssueProvider,
    C: ReviewChartRenderer,
    V: ReviewPreviewEngine,
    S: ReviewReportStore,
{
    disabled_provider: P,
    chart_renderer: C,
    preview_engine: V,
    report_store: S,
}

impl<P, C, V, S> DefaultReviewStage<P, C, V, S>
where
    P: DisabledIssueProvider,
    C: ReviewChartRenderer,
    V: ReviewPreviewEngine,
    S: ReviewReportStore,
{
    pub fn new(disabled_provider: P, chart_renderer: C, preview_engine: V, report_store: S) -> Self {
        Self {
            disabled_provider,
            chart_renderer,
            preview_engine,
            report_store,
        }
    }

    fn summarize_issues(issues: &[Issue]) -> ReviewStats {
        let mut issue_by_date = HashMap::new();
        let mut issue_by_category = HashMap::new();
        let mut issue_by_rule = HashMap::new();
        let mut tickers = HashSet::new();

        for issue in issues {
            *issue_by_date.entry(issue.date.clone()).or_insert(0usize) += 1;
            *issue_by_category
                .entry(issue.category.clone())
                .or_insert(0usize) += 1;
            *issue_by_rule.entry(issue.rule_name.clone()).or_insert(0usize) += 1;
            tickers.insert(issue.ticker.clone());
        }

        ReviewStats {
            total_issues: issues.len(),
            ticker_count: tickers.len(),
            issue_by_date,
            issue_by_category,
            issue_by_rule,
        }
    }

    fn preview_items(
        &self,
        issues: &[Issue],
        config: &ReviewConfig,
    ) -> Result<Vec<ReviewPreviewItem>, ReviewError> {
        if !config.preview.enabled {
            return Ok(Vec::new());
        }

        let sample_size = issues.len().min(config.preview.sample_size);
        let mut out = Vec::with_capacity(sample_size);
        for issue in issues.iter().take(sample_size) {
            out.push(ReviewPreviewItem {
                issue: issue.clone(),
                suggested_fix: self.preview_engine.suggest_fix(issue)?,
            });
        }

        Ok(out)
    }
}

impl<P, C, V, S> ReviewStage for DefaultReviewStage<P, C, V, S>
where
    P: DisabledIssueProvider,
    C: ReviewChartRenderer,
    V: ReviewPreviewEngine,
    S: ReviewReportStore,
{
    fn run(&self, issues: &[Issue], config: &ReviewConfig) -> Result<ReviewOutput, ReviewError> {
        let stats = Self::summarize_issues(issues);

        let mut charts = Vec::new();
        if config.charts.enabled {
            for chart_type in &config.charts.types {
                charts.push(self.chart_renderer.render(*chart_type, issues, &stats)?);
            }
        }

        let preview = self.preview_items(issues, config)?;

        let review_report = ReviewReport {
            stats,
            charts,
            preview,
        };

        self.report_store.save(&review_report, config)?;

        let disabled_rules = self.disabled_provider.load_disabled_rules(config)?;
        let mut approved_issues = Vec::new();
        let mut disabled_issues = Vec::new();

        for issue in issues {
            if disabled_rules.iter().any(|rule| rule.matches(issue)) {
                disabled_issues.push(issue.clone());
            } else {
                approved_issues.push(issue.clone());
            }
        }

        Ok(ReviewOutput {
            approved_issues,
            disabled_issues,
            review_report,
        })
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoopDisabledIssueProvider;

impl DisabledIssueProvider for NoopDisabledIssueProvider {
    fn load_disabled_rules(&self, _config: &ReviewConfig) -> Result<Vec<DisableIssueRule>, ReviewError> {
        Ok(Vec::new())
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoopChartRenderer;

impl ReviewChartRenderer for NoopChartRenderer {
    fn render(
        &self,
        chart_type: ReviewChartType,
        _issues: &[Issue],
        _stats: &ReviewStats,
    ) -> Result<ReviewChart, ReviewError> {
        Ok(ReviewChart {
            chart_type,
            title: "placeholder".to_string(),
            payload: "".to_string(),
        })
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoopPreviewEngine;

impl ReviewPreviewEngine for NoopPreviewEngine {
    fn suggest_fix(&self, issue: &Issue) -> Result<SuggestedFix, ReviewError> {
        Ok(SuggestedFix {
            action: "no-op".to_string(),
            reason: format!("preview unavailable for {}", issue.rule_name),
        })
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoopReviewReportStore;

impl ReviewReportStore for NoopReviewReportStore {
    fn save(&self, _report: &ReviewReport, _config: &ReviewConfig) -> Result<(), ReviewError> {
        Ok(())
    }
}

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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IssueType {
    MissingDates,
    DuplicateDate,
    NonTradingDayData,
    HighBelowOthers,
    LowAboveOthers,
    NegativePrice,
    InvalidTickSize,
    VwapOutOfRange,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Issue {
    pub issue_type: IssueType,
    pub category: String,
    pub rule_name: String,
    pub ticker: String,
    pub date: String,
    pub field: String,
    pub value: String,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ValidationContext {
    trading_days_in_order: Vec<String>,
    trading_day_set: HashSet<String>,
    pub tick_size: Decimal,
}

impl ValidationContext {
    pub fn new(trading_days_in_order: Vec<String>, tick_size: Decimal) -> Self {
        let trading_day_set = trading_days_in_order.iter().cloned().collect();
        Self {
            trading_days_in_order,
            trading_day_set,
            tick_size,
        }
    }

    fn is_trading_day(&self, day: &str) -> bool {
        if self.trading_day_set.is_empty() {
            return true;
        }
        self.trading_day_set.contains(day)
    }

    fn missing_days_between(&self, prev: &str, cur: &str) -> Vec<String> {
        if self.trading_days_in_order.is_empty() {
            return Vec::new();
        }

        let mut prev_idx = None;
        let mut cur_idx = None;
        for (idx, day) in self.trading_days_in_order.iter().enumerate() {
            if day == prev {
                prev_idx = Some(idx);
            }
            if day == cur {
                cur_idx = Some(idx);
            }
        }

        match (prev_idx, cur_idx) {
            (Some(i), Some(j)) if j > i + 1 => self.trading_days_in_order[i + 1..j].to_vec(),
            _ => Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationPlan {
    pub enabled_categories: HashSet<String>,
    pub enabled_rules: HashSet<String>,
    pub disabled_rules: HashSet<String>,
}

impl ValidationPlan {
    pub fn from_rule_switch(switch: &RuleSwitchConfig) -> Self {
        Self {
            enabled_categories: switch.enabled_categories.iter().cloned().collect(),
            enabled_rules: switch.enabled_rules.iter().cloned().collect(),
            disabled_rules: switch.disabled_rules.iter().cloned().collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleMetric {
    pub rule_name: String,
    pub category: String,
    pub elapsed: Duration,
    pub issue_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationOutput {
    pub issues: Vec<Issue>,
    pub metrics: Vec<RuleMetric>,
    pub total_issues: usize,
}

pub trait ValidationRule: Send + Sync {
    fn name(&self) -> &'static str;
    fn category(&self) -> &'static str;
    fn validate(&self, records: &[Record], ctx: &ValidationContext) -> Result<Vec<Issue>, ValidationError>;
}

pub struct ValidationRegistry {
    rules: Vec<Box<dyn ValidationRule>>,
}

impl ValidationRegistry {
    pub fn default() -> Self {
        Self {
            rules: vec![
                Box::new(MissingDatesRule),
                Box::new(DuplicateDatesRule),
                Box::new(NonTradingDayRule),
                Box::new(HighLowLogicRule),
                Box::new(NegativePriceRule),
                Box::new(TickSizeRule),
                Box::new(VwapRangeRule),
            ],
        }
    }

    fn all_rule_names(&self) -> HashSet<String> {
        self.rules.iter().map(|r| r.name().to_string()).collect()
    }

    fn all_categories(&self) -> HashSet<String> {
        self.rules.iter().map(|r| r.category().to_string()).collect()
    }

    fn select_rules(&self, plan: &ValidationPlan) -> Result<Vec<&dyn ValidationRule>, ValidationError> {
        let all_categories = self.all_categories();
        for category in &plan.enabled_categories {
            if !all_categories.contains(category) {
                return Err(ValidationError::UnknownCategory(category.clone()));
            }
        }

        let all_rule_names = self.all_rule_names();
        for rule in &plan.enabled_rules {
            if !all_rule_names.contains(rule) {
                return Err(ValidationError::UnknownRule(rule.clone()));
            }
        }

        for rule in &plan.disabled_rules {
            if !all_rule_names.contains(rule) {
                return Err(ValidationError::UnknownRule(rule.clone()));
            }
        }

        let use_enabled_rules = !plan.enabled_rules.is_empty();

        let selected = self
            .rules
            .iter()
            .filter(|rule| {
                if plan.disabled_rules.contains(rule.name()) {
                    return false;
                }

                if use_enabled_rules {
                    return plan.enabled_rules.contains(rule.name());
                }

                if !plan.enabled_categories.is_empty() {
                    return plan.enabled_categories.contains(rule.category());
                }

                true
            })
            .map(|rule| rule.as_ref())
            .collect();

        Ok(selected)
    }
}

#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("unknown category: {0}")]
    UnknownCategory(String),
    #[error("unknown rule: {0}")]
    UnknownRule(String),
    #[error("rule execution failed: {rule_name}: {detail}")]
    RuleExecution { rule_name: String, detail: String },
}

pub fn validate_records(
    records: &[Record],
    ctx: &ValidationContext,
    plan: &ValidationPlan,
    registry: &ValidationRegistry,
) -> Result<ValidationOutput, ValidationError> {
    let selected_rules = registry.select_rules(plan)?;

    let mut issues = Vec::new();
    let mut metrics = Vec::new();

    for rule in selected_rules {
        let start = Instant::now();
        let mut rule_issues = rule.validate(records, ctx)?;
        let elapsed = start.elapsed();
        let issue_count = rule_issues.len();

        issues.append(&mut rule_issues);
        metrics.push(RuleMetric {
            rule_name: rule.name().to_string(),
            category: rule.category().to_string(),
            elapsed,
            issue_count,
        });
    }

    issues.sort_by(|a, b| {
        (&a.ticker, &a.date, &a.rule_name, &a.field, &a.detail).cmp(&(
            &b.ticker,
            &b.date,
            &b.rule_name,
            &b.field,
            &b.detail,
        ))
    });

    Ok(ValidationOutput {
        total_issues: issues.len(),
        issues,
        metrics,
    })
}

struct MissingDatesRule;

impl ValidationRule for MissingDatesRule {
    fn name(&self) -> &'static str {
        "MissingDatesRule"
    }

    fn category(&self) -> &'static str {
        "DataIntegrity"
    }

    fn validate(&self, records: &[Record], ctx: &ValidationContext) -> Result<Vec<Issue>, ValidationError> {
        let mut grouped: HashMap<&str, Vec<&Record>> = HashMap::new();
        for row in records {
            grouped.entry(&row.ticker).or_default().push(row);
        }

        let mut issues = Vec::new();
        for (ticker, mut rows) in grouped {
            rows.sort_by(|a, b| a.date.cmp(&b.date));
            for pair in rows.windows(2) {
                let prev = pair[0];
                let cur = pair[1];
                let missing_days = ctx.missing_days_between(&prev.date, &cur.date);
                if !missing_days.is_empty() {
                    issues.push(Issue {
                        issue_type: IssueType::MissingDates,
                        category: self.category().to_string(),
                        rule_name: self.name().to_string(),
                        ticker: ticker.to_string(),
                        date: missing_days.join("|"),
                        field: "date".to_string(),
                        value: "gap".to_string(),
                        detail: "Trading days missing between records".to_string(),
                    });
                }
            }
        }

        Ok(issues)
    }
}

struct DuplicateDatesRule;

impl ValidationRule for DuplicateDatesRule {
    fn name(&self) -> &'static str {
        "DuplicateDatesRule"
    }

    fn category(&self) -> &'static str {
        "DataIntegrity"
    }

    fn validate(&self, records: &[Record], _ctx: &ValidationContext) -> Result<Vec<Issue>, ValidationError> {
        let mut seen = HashSet::new();
        let mut issues = Vec::new();

        for row in records {
            let key = (row.ticker.as_str(), row.date.as_str());
            if seen.contains(&key) {
                issues.push(Issue {
                    issue_type: IssueType::DuplicateDate,
                    category: self.category().to_string(),
                    rule_name: self.name().to_string(),
                    ticker: row.ticker.clone(),
                    date: row.date.clone(),
                    field: "date".to_string(),
                    value: row.date.clone(),
                    detail: "Multiple rows for same ticker & date".to_string(),
                });
            } else {
                seen.insert(key);
            }
        }

        Ok(issues)
    }
}

struct NonTradingDayRule;

impl ValidationRule for NonTradingDayRule {
    fn name(&self) -> &'static str {
        "NonTradingDayRule"
    }

    fn category(&self) -> &'static str {
        "DataIntegrity"
    }

    fn validate(&self, records: &[Record], ctx: &ValidationContext) -> Result<Vec<Issue>, ValidationError> {
        let mut issues = Vec::new();
        for row in records {
            if !ctx.is_trading_day(&row.date) {
                issues.push(Issue {
                    issue_type: IssueType::NonTradingDayData,
                    category: self.category().to_string(),
                    rule_name: self.name().to_string(),
                    ticker: row.ticker.clone(),
                    date: row.date.clone(),
                    field: "date".to_string(),
                    value: row.date.clone(),
                    detail: "Data exists on non-trading day".to_string(),
                });
            }
        }
        Ok(issues)
    }
}

struct HighLowLogicRule;

impl ValidationRule for HighLowLogicRule {
    fn name(&self) -> &'static str {
        "HighLowLogicRule"
    }

    fn category(&self) -> &'static str {
        "IntraBarLogic"
    }

    fn validate(&self, records: &[Record], _ctx: &ValidationContext) -> Result<Vec<Issue>, ValidationError> {
        let mut issues = Vec::new();

        for row in records {
            let max_other = row.open.max(row.close).max(row.low);
            if row.high < max_other {
                issues.push(Issue {
                    issue_type: IssueType::HighBelowOthers,
                    category: self.category().to_string(),
                    rule_name: self.name().to_string(),
                    ticker: row.ticker.clone(),
                    date: row.date.clone(),
                    field: "high".to_string(),
                    value: row.high.to_string(),
                    detail: "High is below Open/Close/Low".to_string(),
                });
            }

            let min_other = row.open.min(row.close).min(row.high);
            if row.low > min_other {
                issues.push(Issue {
                    issue_type: IssueType::LowAboveOthers,
                    category: self.category().to_string(),
                    rule_name: self.name().to_string(),
                    ticker: row.ticker.clone(),
                    date: row.date.clone(),
                    field: "low".to_string(),
                    value: row.low.to_string(),
                    detail: "Low is above Open/Close/High".to_string(),
                });
            }
        }

        Ok(issues)
    }
}

struct NegativePriceRule;

impl ValidationRule for NegativePriceRule {
    fn name(&self) -> &'static str {
        "NegativePriceRule"
    }

    fn category(&self) -> &'static str {
        "IntraBarLogic"
    }

    fn validate(&self, records: &[Record], _ctx: &ValidationContext) -> Result<Vec<Issue>, ValidationError> {
        let mut issues = Vec::new();

        for row in records {
            if row.open < Decimal::ZERO
                || row.high < Decimal::ZERO
                || row.low < Decimal::ZERO
                || row.close < Decimal::ZERO
                || row.vwap < Decimal::ZERO
            {
                issues.push(Issue {
                    issue_type: IssueType::NegativePrice,
                    category: self.category().to_string(),
                    rule_name: self.name().to_string(),
                    ticker: row.ticker.clone(),
                    date: row.date.clone(),
                    field: "price".to_string(),
                    value: format!(
                        "open={},high={},low={},close={},vwap={}",
                        row.open, row.high, row.low, row.close, row.vwap
                    ),
                    detail: "Negative price not allowed".to_string(),
                });
            }
        }

        Ok(issues)
    }
}

struct TickSizeRule;

impl ValidationRule for TickSizeRule {
    fn name(&self) -> &'static str {
        "TickSizeRule"
    }

    fn category(&self) -> &'static str {
        "IntraBarLogic"
    }

    fn validate(&self, records: &[Record], ctx: &ValidationContext) -> Result<Vec<Issue>, ValidationError> {
        let mut issues = Vec::new();
        if ctx.tick_size <= Decimal::ZERO {
            return Ok(issues);
        }

        for row in records {
            let invalid = !is_valid_tick(row.open, ctx.tick_size)
                || !is_valid_tick(row.high, ctx.tick_size)
                || !is_valid_tick(row.low, ctx.tick_size)
                || !is_valid_tick(row.close, ctx.tick_size)
                || !is_valid_tick(row.vwap, ctx.tick_size);

            if invalid {
                issues.push(Issue {
                    issue_type: IssueType::InvalidTickSize,
                    category: self.category().to_string(),
                    rule_name: self.name().to_string(),
                    ticker: row.ticker.clone(),
                    date: row.date.clone(),
                    field: "price".to_string(),
                    value: format!(
                        "open={},high={},low={},close={},vwap={}",
                        row.open, row.high, row.low, row.close, row.vwap
                    ),
                    detail: "Price not aligned to tick size".to_string(),
                });
            }
        }

        Ok(issues)
    }
}

struct VwapRangeRule;

impl ValidationRule for VwapRangeRule {
    fn name(&self) -> &'static str {
        "VwapRangeRule"
    }

    fn category(&self) -> &'static str {
        "IntraBarLogic"
    }

    fn validate(&self, records: &[Record], _ctx: &ValidationContext) -> Result<Vec<Issue>, ValidationError> {
        let mut issues = Vec::new();

        for row in records {
            if row.vwap < row.low || row.vwap > row.high {
                issues.push(Issue {
                    issue_type: IssueType::VwapOutOfRange,
                    category: self.category().to_string(),
                    rule_name: self.name().to_string(),
                    ticker: row.ticker.clone(),
                    date: row.date.clone(),
                    field: "vwap".to_string(),
                    value: row.vwap.to_string(),
                    detail: "VWAP is outside [Low, High]".to_string(),
                });
            }
        }

        Ok(issues)
    }
}

fn is_valid_tick(value: Decimal, tick_size: Decimal) -> bool {
    let rem = value % tick_size;
    rem == Decimal::ZERO
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
