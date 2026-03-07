use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;
use std::time::Instant;
use std::time::{SystemTime, UNIX_EPOCH};

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

pub const REVIEW_DISABLED_RULES_FILE: &str = "disabled_issues.yaml";
pub const REVIEW_REPORT_FILE: &str = "review_report.txt";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileDisabledIssueProvider {
    pub file_name: String,
}

impl Default for FileDisabledIssueProvider {
    fn default() -> Self {
        Self {
            file_name: REVIEW_DISABLED_RULES_FILE.to_string(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct RawDisabledRulesFile {
    #[serde(default)]
    rules: Vec<RawDisableIssueRule>,
}

#[derive(Debug, Deserialize)]
struct RawDisableIssueRule {
    #[serde(default)]
    issue_types: Vec<String>,
    #[serde(default)]
    categories: Vec<String>,
    #[serde(default)]
    rule_names: Vec<String>,
    #[serde(default)]
    tickers: Vec<String>,
    #[serde(default)]
    dates: Vec<String>,
    #[serde(default)]
    fields: Vec<String>,
}

impl DisabledIssueProvider for FileDisabledIssueProvider {
    fn load_disabled_rules(&self, config: &ReviewConfig) -> Result<Vec<DisableIssueRule>, ReviewError> {
        let path = config.output_dir.join(&self.file_name);
        if !path.exists() {
            return Ok(Vec::new());
        }

        let content = fs::read_to_string(&path)
            .map_err(|e| ReviewError::DisabledRules(format!("{}: {}", path.display(), e)))?;

        let raw: RawDisabledRulesFile = serde_yaml::from_str(&content)
            .map_err(|e| ReviewError::DisabledRules(format!("{}: {}", path.display(), e)))?;

        let mut rules = Vec::with_capacity(raw.rules.len());
        for entry in raw.rules {
            let mut issue_types = HashSet::new();
            for raw_issue_type in entry.issue_types {
                let parsed = parse_issue_type(&raw_issue_type).ok_or_else(|| {
                    ReviewError::DisabledRules(format!(
                        "unknown issue type in {}: {}",
                        path.display(),
                        raw_issue_type
                    ))
                })?;
                issue_types.insert(parsed);
            }

            rules.push(DisableIssueRule {
                issue_types,
                categories: entry.categories.into_iter().collect(),
                rule_names: entry.rule_names.into_iter().collect(),
                tickers: entry.tickers.into_iter().collect(),
                dates: entry.dates.into_iter().collect(),
                fields: entry.fields.into_iter().collect(),
            });
        }

        Ok(rules)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct BasicReviewChartRenderer;

impl ReviewChartRenderer for BasicReviewChartRenderer {
    fn render(
        &self,
        chart_type: ReviewChartType,
        _issues: &[Issue],
        stats: &ReviewStats,
    ) -> Result<ReviewChart, ReviewError> {
        let (title, payload) = match chart_type {
            ReviewChartType::IssueByDate => {
                ("Issue Count By Date".to_string(), format_kv_map(&stats.issue_by_date))
            }
            ReviewChartType::IssueByCategory => (
                "Issue Count By Category".to_string(),
                format_kv_map(&stats.issue_by_category),
            ),
            ReviewChartType::IssueByRule => {
                ("Issue Count By Rule".to_string(), format_kv_map(&stats.issue_by_rule))
            }
        };

        Ok(ReviewChart {
            chart_type,
            title,
            payload,
        })
    }
}

#[derive(Debug, Clone)]
pub struct RuleBasedPreviewEngine {
    suggestions: HashMap<IssueType, SuggestedFix>,
}

impl Default for RuleBasedPreviewEngine {
    fn default() -> Self {
        let mut suggestions = HashMap::new();
        suggestions.insert(
            IssueType::DuplicateDate,
            SuggestedFix {
                action: "drop-duplicate".to_string(),
                reason: "keep first valid row for same ticker/date".to_string(),
            },
        );
        suggestions.insert(
            IssueType::NegativePrice,
            SuggestedFix {
                action: "set-null".to_string(),
                reason: "negative prices are invalid market values".to_string(),
            },
        );
        suggestions.insert(
            IssueType::VwapOutOfRange,
            SuggestedFix {
                action: "clamp".to_string(),
                reason: "limit VWAP to [low, high] interval".to_string(),
            },
        );

        Self { suggestions }
    }
}

impl ReviewPreviewEngine for RuleBasedPreviewEngine {
    fn suggest_fix(&self, issue: &Issue) -> Result<SuggestedFix, ReviewError> {
        Ok(self
            .suggestions
            .get(&issue.issue_type)
            .cloned()
            .unwrap_or(SuggestedFix {
                action: "manual-review".to_string(),
                reason: format!("no built-in simulation for {}", issue.rule_name),
            }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileReviewReportStore {
    pub file_name: String,
}

impl Default for FileReviewReportStore {
    fn default() -> Self {
        Self {
            file_name: REVIEW_REPORT_FILE.to_string(),
        }
    }
}

impl ReviewReportStore for FileReviewReportStore {
    fn save(&self, report: &ReviewReport, config: &ReviewConfig) -> Result<(), ReviewError> {
        fs::create_dir_all(&config.output_dir)
            .map_err(|e| ReviewError::Persist(format!("{}", e)))?;

        let path = config.output_dir.join(&self.file_name);
        let mut out = String::new();
        out.push_str("[stats]\n");
        out.push_str(&format!("total_issues: {}\n", report.stats.total_issues));
        out.push_str(&format!("ticker_count: {}\n", report.stats.ticker_count));
        out.push_str("issue_by_date:\n");
        out.push_str(&format_kv_map(&report.stats.issue_by_date));
        out.push_str("\nissue_by_category:\n");
        out.push_str(&format_kv_map(&report.stats.issue_by_category));
        out.push_str("\nissue_by_rule:\n");
        out.push_str(&format_kv_map(&report.stats.issue_by_rule));
        out.push_str("\n[charts]\n");
        for chart in &report.charts {
            out.push_str(&format!("- {}\n", chart.title));
            if !chart.payload.is_empty() {
                out.push_str(&chart.payload);
                out.push('\n');
            }
        }
        out.push_str("[preview]\n");
        for item in &report.preview {
            out.push_str(&format!(
                "- rule={}, ticker={}, date={}, action={}, reason={}\n",
                item.issue.rule_name,
                item.issue.ticker,
                item.issue.date,
                item.suggested_fix.action,
                item.suggested_fix.reason
            ));
        }

        fs::write(&path, out).map_err(|e| ReviewError::Persist(format!("{}: {}", path.display(), e)))
    }
}

fn format_kv_map(map: &HashMap<String, usize>) -> String {
    let mut pairs = map.iter().collect::<Vec<_>>();
    pairs.sort_by(|a, b| a.0.cmp(b.0));

    let mut out = String::new();
    for (key, value) in pairs {
        out.push_str(&format!("{}: {}\n", key, value));
    }
    out
}

fn parse_issue_type(raw: &str) -> Option<IssueType> {
    let normalized = raw
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_lowercase();

    match normalized.as_str() {
        "missingdates" => Some(IssueType::MissingDates),
        "duplicatedate" | "duplicatedates" => Some(IssueType::DuplicateDate),
        "nontradingdaydata" => Some(IssueType::NonTradingDayData),
        "highbelowothers" => Some(IssueType::HighBelowOthers),
        "lowaboveothers" => Some(IssueType::LowAboveOthers),
        "negativeprice" => Some(IssueType::NegativePrice),
        "invalidticksize" => Some(IssueType::InvalidTickSize),
        "vwapoutofrange" => Some(IssueType::VwapOutOfRange),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditActionSource {
    Auto,
    Manual,
    Disabled,
    Loader,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditStage {
    Load,
    Validate,
    Review,
    Clean,
    Write,
}

impl AuditStage {
    fn as_str(self) -> &'static str {
        match self {
            Self::Load => "LOAD",
            Self::Validate => "VALIDATE",
            Self::Review => "REVIEW",
            Self::Clean => "CLEAN",
            Self::Write => "WRITE",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditEntry {
    pub timestamp: String,
    pub stage: AuditStage,
    pub ticker: String,
    pub date: String,
    pub issue_type: String,
    pub category: String,
    pub rule_name: String,
    pub field: String,
    pub old_value: String,
    pub new_value: String,
    pub action: String,
    pub action_source: AuditActionSource,
    pub comment: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PerformanceSummary {
    pub total_rows: usize,
    pub total_issues: usize,
    pub processed_issues: usize,
    pub unresolved_issues: usize,
    pub disabled_issues: usize,
    pub load_error_count: usize,
    pub total_time_ms: u128,
    pub throughput_rows_per_sec: u64,
    pub rule_time_breakdown: HashMap<String, u128>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CleanerOutput {
    pub cleaned_records: Vec<Record>,
    pub audit_entries: Vec<AuditEntry>,
    pub processed_issues: usize,
    pub unresolved_issues: usize,
}

#[derive(Debug, Error)]
pub enum AuditError {
    #[error("failed to persist audit output: {0}")]
    Persist(String),
}

#[derive(Debug, Clone)]
pub struct PerformanceSummaryInput<'a> {
    pub total_rows: usize,
    pub total_issues: usize,
    pub disabled_issues: usize,
    pub load_error_count: usize,
    pub cleaner_output: &'a CleanerOutput,
    pub total_time_ms: u128,
    pub rule_time_breakdown: HashMap<String, u128>,
}

pub trait PerformanceSummaryBuilder: Send + Sync {
    fn build(&self, input: PerformanceSummaryInput<'_>) -> PerformanceSummary;
}

pub trait AuditLogWriter: Send + Sync {
    fn write(
        &self,
        audit_entries: &[AuditEntry],
        performance_summary: &PerformanceSummary,
        output_json: &Path,
        output_csv: &Path,
    ) -> Result<(), AuditError>;
}

pub trait AuditService: Send + Sync {
    fn publish(
        &self,
        audit_entries: &[AuditEntry],
        summary_input: PerformanceSummaryInput<'_>,
        output_json: &Path,
        output_csv: &Path,
    ) -> Result<PerformanceSummary, AuditError>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultPerformanceSummaryBuilder;

impl PerformanceSummaryBuilder for DefaultPerformanceSummaryBuilder {
    fn build(&self, input: PerformanceSummaryInput<'_>) -> PerformanceSummary {
        build_performance_summary(
            input.total_rows,
            input.total_issues,
            input.disabled_issues,
            input.load_error_count,
            input.cleaner_output,
            input.total_time_ms,
            input.rule_time_breakdown,
        )
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct FileAuditLogWriter;

impl AuditLogWriter for FileAuditLogWriter {
    fn write(
        &self,
        audit_entries: &[AuditEntry],
        performance_summary: &PerformanceSummary,
        output_json: &Path,
        output_csv: &Path,
    ) -> Result<(), AuditError> {
        if let Some(parent) = output_json.parent() {
            fs::create_dir_all(parent).map_err(|e| AuditError::Persist(format!("{}", e)))?;
        }
        if let Some(parent) = output_csv.parent() {
            fs::create_dir_all(parent).map_err(|e| AuditError::Persist(format!("{}", e)))?;
        }

        fs::write(output_json, render_audit_json(audit_entries, performance_summary)).map_err(|e| {
            AuditError::Persist(format!("{}: {}", output_json.display(), e))
        })?;

        let mut csv_out = String::from(
            "timestamp,stage,ticker,date,issue_type,category,rule_name,field,old_value,new_value,action,action_source,comment\n",
        );
        for entry in audit_entries {
            csv_out.push_str(&format!(
                "{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
                csv_escape(&entry.timestamp),
                csv_escape(entry.stage.as_str()),
                csv_escape(&entry.ticker),
                csv_escape(&entry.date),
                csv_escape(&entry.issue_type),
                csv_escape(&entry.category),
                csv_escape(&entry.rule_name),
                csv_escape(&entry.field),
                csv_escape(&entry.old_value),
                csv_escape(&entry.new_value),
                csv_escape(&entry.action),
                csv_escape(audit_action_source_name(entry.action_source)),
                csv_escape(&entry.comment)
            ));
        }

        fs::write(output_csv, csv_out)
            .map_err(|e| AuditError::Persist(format!("{}: {}", output_csv.display(), e)))
    }
}

pub struct DefaultAuditService<B, W>
where
    B: PerformanceSummaryBuilder,
    W: AuditLogWriter,
{
    summary_builder: B,
    writer: W,
}

impl<B, W> DefaultAuditService<B, W>
where
    B: PerformanceSummaryBuilder,
    W: AuditLogWriter,
{
    pub fn new(summary_builder: B, writer: W) -> Self {
        Self {
            summary_builder,
            writer,
        }
    }
}

impl<B, W> AuditService for DefaultAuditService<B, W>
where
    B: PerformanceSummaryBuilder,
    W: AuditLogWriter,
{
    fn publish(
        &self,
        audit_entries: &[AuditEntry],
        summary_input: PerformanceSummaryInput<'_>,
        output_json: &Path,
        output_csv: &Path,
    ) -> Result<PerformanceSummary, AuditError> {
        let performance_summary = self.summary_builder.build(summary_input);
        self.writer
            .write(audit_entries, &performance_summary, output_json, output_csv)?;
        Ok(performance_summary)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PolicyApplyResult {
    pub action: String,
    pub old_value: String,
    pub new_value: String,
    pub action_source: AuditActionSource,
    pub comment: String,
}

#[derive(Debug, Error)]
pub enum CleanerError {
    #[error("invalid issue field: {0}")]
    UnknownField(String),
    #[error("policy execution failed for {rule_name}: {detail}")]
    PolicyExecution { rule_name: String, detail: String },
}

pub trait PolicyResolver: Send + Sync {
    fn resolve_policy(&self, issue: &Issue, handling: &HandlingConfig) -> Option<PolicyConfig>;
}

pub trait PolicyExecutor: Send + Sync {
    fn apply_policy(
        &self,
        record: &mut Record,
        issue: &Issue,
        policy: &PolicyConfig,
    ) -> Result<PolicyApplyResult, CleanerError>;
}

pub trait LoadErrorAuditMapper: Send + Sync {
    fn map(&self, load_errors: &[LoadError]) -> Vec<AuditEntry>;
}

pub trait CleanerStage: Send + Sync {
    fn run(
        &self,
        records: &[Record],
        approved_issues: &[Issue],
        load_errors: &[LoadError],
        handling: &HandlingConfig,
    ) -> Result<CleanerOutput, CleanerError>;
}

pub struct DefaultCleanerStage<R, E, M>
where
    R: PolicyResolver,
    E: PolicyExecutor,
    M: LoadErrorAuditMapper,
{
    resolver: R,
    executor: E,
    load_error_mapper: M,
}

impl<R, E, M> DefaultCleanerStage<R, E, M>
where
    R: PolicyResolver,
    E: PolicyExecutor,
    M: LoadErrorAuditMapper,
{
    pub fn new(resolver: R, executor: E, load_error_mapper: M) -> Self {
        Self {
            resolver,
            executor,
            load_error_mapper,
        }
    }

    fn issue_index(issues: &[Issue]) -> HashMap<(String, String), Vec<Issue>> {
        let mut out: HashMap<(String, String), Vec<Issue>> = HashMap::new();
        for issue in issues {
            out.entry((issue.ticker.clone(), issue.date.clone()))
                .or_default()
                .push(issue.clone());
        }
        out
    }
}

impl<R, E, M> CleanerStage for DefaultCleanerStage<R, E, M>
where
    R: PolicyResolver,
    E: PolicyExecutor,
    M: LoadErrorAuditMapper,
{
    fn run(
        &self,
        records: &[Record],
        approved_issues: &[Issue],
        load_errors: &[LoadError],
        handling: &HandlingConfig,
    ) -> Result<CleanerOutput, CleanerError> {
        let mut cleaned_records = records.to_vec();
        let mut audit_entries = self.load_error_mapper.map(load_errors);
        let issue_index = Self::issue_index(approved_issues);

        let mut processed_issues = 0usize;
        let mut unresolved_issues = 0usize;

        for record in &mut cleaned_records {
            let key = (record.ticker.clone(), record.date.clone());
            let Some(issues) = issue_index.get(&key) else {
                continue;
            };

            for issue in issues {
                let old_value = record_field_value(record, &issue.field)?;

                let Some(policy) = self.resolver.resolve_policy(issue, handling) else {
                    unresolved_issues += 1;
                    audit_entries.push(new_audit_entry(
                        issue,
                        old_value.clone(),
                        old_value,
                        "UNRESOLVED".to_string(),
                        AuditActionSource::Disabled,
                        "No policy configured for this issue".to_string(),
                    ));
                    continue;
                };

                let applied = self.executor.apply_policy(record, issue, &policy)?;
                processed_issues += 1;
                audit_entries.push(new_audit_entry(
                    issue,
                    applied.old_value,
                    applied.new_value,
                    applied.action,
                    applied.action_source,
                    applied.comment,
                ));
            }
        }

        Ok(CleanerOutput {
            cleaned_records,
            audit_entries,
            processed_issues,
            unresolved_issues,
        })
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RuleNamePolicyResolver;

impl PolicyResolver for RuleNamePolicyResolver {
    fn resolve_policy(&self, issue: &Issue, handling: &HandlingConfig) -> Option<PolicyConfig> {
        handling
            .policies
            .iter()
            .find(|p| p.rule_name == issue.rule_name)
            .cloned()
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoopPolicyExecutor;

impl PolicyExecutor for NoopPolicyExecutor {
    fn apply_policy(
        &self,
        record: &mut Record,
        issue: &Issue,
        policy: &PolicyConfig,
    ) -> Result<PolicyApplyResult, CleanerError> {
        let old_value = record_field_value(record, &issue.field)?;
        Ok(PolicyApplyResult {
            action: policy.action.clone(),
            old_value: old_value.clone(),
            new_value: old_value,
            action_source: AuditActionSource::Auto,
            comment: "Noop executor did not change record".to_string(),
        })
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct BuiltinPolicyExecutor;

impl PolicyExecutor for BuiltinPolicyExecutor {
    fn apply_policy(
        &self,
        record: &mut Record,
        issue: &Issue,
        policy: &PolicyConfig,
    ) -> Result<PolicyApplyResult, CleanerError> {
        let old_value = record_field_value(record, &issue.field)?;

        match policy.action.as_str() {
            "set_literal" => {
                let value_raw = required_param_str(policy, "value")?;
                set_record_field(record, &issue.field, value_raw)?;

                Ok(PolicyApplyResult {
                    action: policy.action.clone(),
                    old_value,
                    new_value: record_field_value(record, &issue.field)?,
                    action_source: AuditActionSource::Auto,
                    comment: format!("set {} with literal value", issue.field),
                })
            }
            "clamp_field" => {
                let min_field = required_param_str(policy, "min_field")?;
                let max_field = required_param_str(policy, "max_field")?;

                let min = parse_decimal_field(record, min_field)?;
                let max = parse_decimal_field(record, max_field)?;
                let value = parse_decimal_field(record, &issue.field)?;

                let clamped = if value < min {
                    min
                } else if value > max {
                    max
                } else {
                    value
                };

                set_record_field(record, &issue.field, &clamped.to_string())?;

                Ok(PolicyApplyResult {
                    action: policy.action.clone(),
                    old_value,
                    new_value: record_field_value(record, &issue.field)?,
                    action_source: AuditActionSource::Auto,
                    comment: format!("clamped {} to [{}, {}]", issue.field, min_field, max_field),
                })
            }
            other => Err(CleanerError::PolicyExecution {
                rule_name: issue.rule_name.clone(),
                detail: format!("unsupported action: {other}"),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultLoadErrorAuditMapper;

impl LoadErrorAuditMapper for DefaultLoadErrorAuditMapper {
    fn map(&self, load_errors: &[LoadError]) -> Vec<AuditEntry> {
        let mut out = Vec::with_capacity(load_errors.len());
        for load_error in load_errors {
            out.push(AuditEntry {
                timestamp: now_epoch_millis(),
                stage: AuditStage::Load,
                ticker: String::new(),
                date: String::new(),
                issue_type: "LOAD_ERROR".to_string(),
                category: "Loader".to_string(),
                rule_name: "Loader::parse_csv_row".to_string(),
                field: "raw_row".to_string(),
                old_value: load_error.raw_row.clone(),
                new_value: load_error.raw_row.clone(),
                action: "LOAD_ERROR".to_string(),
                action_source: AuditActionSource::Loader,
                comment: format!(
                    "row={}, code={:?}, detail={}",
                    load_error.row_number, load_error.error_code, load_error.error_detail
                ),
            });
        }
        out
    }
}

pub fn build_performance_summary(
    total_rows: usize,
    total_issues: usize,
    disabled_issues: usize,
    load_error_count: usize,
    cleaner_output: &CleanerOutput,
    total_time_ms: u128,
    rule_time_breakdown: HashMap<String, u128>,
) -> PerformanceSummary {
    let throughput_rows_per_sec = if total_time_ms == 0 {
        0
    } else {
        ((total_rows as u128 * 1000u128) / total_time_ms) as u64
    };

    PerformanceSummary {
        total_rows,
        total_issues,
        processed_issues: cleaner_output.processed_issues,
        unresolved_issues: cleaner_output.unresolved_issues,
        disabled_issues,
        load_error_count,
        total_time_ms,
        throughput_rows_per_sec,
        rule_time_breakdown,
    }
}

fn record_field_value(record: &Record, field: &str) -> Result<String, CleanerError> {
    match field {
        "date" => Ok(record.date.clone()),
        "ticker" => Ok(record.ticker.clone()),
        "open" => Ok(record.open.to_string()),
        "high" => Ok(record.high.to_string()),
        "low" => Ok(record.low.to_string()),
        "close" => Ok(record.close.to_string()),
        "vwap" => Ok(record.vwap.to_string()),
        "volume" => Ok(record.volume.to_string()),
        "turnover" => Ok(record.turnover.to_string()),
        "status" => Ok(format!("{:?}", record.status)),
        other => Err(CleanerError::UnknownField(other.to_string())),
    }
}

fn set_record_field(record: &mut Record, field: &str, value: &str) -> Result<(), CleanerError> {
    match field {
        "date" => {
            record.date = value.to_string();
            Ok(())
        }
        "ticker" => {
            record.ticker = value.to_string();
            Ok(())
        }
        "open" => {
            record.open = parse_decimal_literal(value, field)?;
            Ok(())
        }
        "high" => {
            record.high = parse_decimal_literal(value, field)?;
            Ok(())
        }
        "low" => {
            record.low = parse_decimal_literal(value, field)?;
            Ok(())
        }
        "close" => {
            record.close = parse_decimal_literal(value, field)?;
            Ok(())
        }
        "vwap" => {
            record.vwap = parse_decimal_literal(value, field)?;
            Ok(())
        }
        "volume" => {
            record.volume = value.parse::<i64>().map_err(|_| CleanerError::PolicyExecution {
                rule_name: "PolicyParam".to_string(),
                detail: format!("invalid int literal for volume: {value}"),
            })?;
            Ok(())
        }
        "turnover" => {
            record.turnover = value.parse::<i64>().map_err(|_| CleanerError::PolicyExecution {
                rule_name: "PolicyParam".to_string(),
                detail: format!("invalid int literal for turnover: {value}"),
            })?;
            Ok(())
        }
        "status" => {
            record.status = TradeStatus::parse(value);
            Ok(())
        }
        other => Err(CleanerError::UnknownField(other.to_string())),
    }
}

fn parse_decimal_literal(raw: &str, field: &str) -> Result<Decimal, CleanerError> {
    Decimal::from_str(raw).map_err(|_| CleanerError::PolicyExecution {
        rule_name: "PolicyParam".to_string(),
        detail: format!("invalid decimal literal for {field}: {raw}"),
    })
}

fn required_param_str<'a>(policy: &'a PolicyConfig, key: &str) -> Result<&'a str, CleanerError> {
    policy
        .params
        .get(key)
        .and_then(|v| v.as_str())
        .ok_or_else(|| CleanerError::PolicyExecution {
            rule_name: policy.rule_name.clone(),
            detail: format!("missing or invalid string param: {key}"),
        })
}

fn parse_decimal_field(record: &Record, field: &str) -> Result<Decimal, CleanerError> {
    let raw = record_field_value(record, field)?;
    Decimal::from_str(&raw).map_err(|_| CleanerError::PolicyExecution {
        rule_name: "PolicyParam".to_string(),
        detail: format!("field is not decimal-compatible: {field}"),
    })
}

fn new_audit_entry(
    issue: &Issue,
    old_value: String,
    new_value: String,
    action: String,
    action_source: AuditActionSource,
    comment: String,
) -> AuditEntry {
    AuditEntry {
        timestamp: now_epoch_millis(),
        stage: AuditStage::Clean,
        ticker: issue.ticker.clone(),
        date: issue.date.clone(),
        issue_type: format!("{:?}", issue.issue_type),
        category: issue.category.clone(),
        rule_name: issue.rule_name.clone(),
        field: issue.field.clone(),
        old_value,
        new_value,
        action,
        action_source,
        comment,
    }
}

fn now_epoch_millis() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis().to_string(),
        Err(_) => "0".to_string(),
    }
}

fn audit_action_source_name(source: AuditActionSource) -> &'static str {
    match source {
        AuditActionSource::Auto => "AUTO",
        AuditActionSource::Manual => "MANUAL",
        AuditActionSource::Disabled => "DISABLED",
        AuditActionSource::Loader => "LOADER",
    }
}

fn render_audit_json(audit_entries: &[AuditEntry], performance_summary: &PerformanceSummary) -> String {
    let mut out = String::new();
    out.push_str("{\n  \"audit_entries\": [\n");

    for (idx, entry) in audit_entries.iter().enumerate() {
        if idx > 0 {
            out.push_str(",\n");
        }
        out.push_str("    {");
        out.push_str(&format!(
            "\"timestamp\":\"{}\",\"stage\":\"{}\",\"ticker\":\"{}\",\"date\":\"{}\",\"issue_type\":\"{}\",\"category\":\"{}\",\"rule_name\":\"{}\",\"field\":\"{}\",\"old_value\":\"{}\",\"new_value\":\"{}\",\"action\":\"{}\",\"action_source\":\"{}\",\"comment\":\"{}\"",
            json_escape(&entry.timestamp),
            json_escape(entry.stage.as_str()),
            json_escape(&entry.ticker),
            json_escape(&entry.date),
            json_escape(&entry.issue_type),
            json_escape(&entry.category),
            json_escape(&entry.rule_name),
            json_escape(&entry.field),
            json_escape(&entry.old_value),
            json_escape(&entry.new_value),
            json_escape(&entry.action),
            json_escape(audit_action_source_name(entry.action_source)),
            json_escape(&entry.comment)
        ));
        out.push('}');
    }

    out.push_str("\n  ],\n");
    out.push_str("  \"performance\": {\n");
    out.push_str(&format!(
        "    \"total_rows\": {},\n    \"load_error_count\": {},\n    \"total_issues\": {},\n    \"processed_issues\": {},\n    \"unresolved_issues\": {},\n    \"disabled_issues\": {},\n    \"total_time_ms\": {},\n    \"throughput_rows_per_sec\": {},\n    \"rule_time_breakdown\": {}\n",
        performance_summary.total_rows,
        performance_summary.load_error_count,
        performance_summary.total_issues,
        performance_summary.processed_issues,
        performance_summary.unresolved_issues,
        performance_summary.disabled_issues,
        performance_summary.total_time_ms,
        performance_summary.throughput_rows_per_sec,
        render_rule_time_breakdown_json(&performance_summary.rule_time_breakdown)
    ));
    out.push_str("  }\n}\n");

    out
}

fn render_rule_time_breakdown_json(rule_time_breakdown: &HashMap<String, u128>) -> String {
    let mut pairs = rule_time_breakdown.iter().collect::<Vec<_>>();
    pairs.sort_by(|a, b| a.0.cmp(b.0));

    let mut out = String::from("{");
    for (idx, (rule_name, elapsed_ms)) in pairs.into_iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        out.push_str(&format!(
            "\"{}\":{}",
            json_escape(rule_name),
            elapsed_ms
        ));
    }
    out.push('}');
    out
}

fn json_escape(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(ch),
        }
    }
    out
}

fn csv_escape(raw: &str) -> String {
    if raw.contains(',') || raw.contains('"') || raw.contains('\n') || raw.contains('\r') {
        format!("\"{}\"", raw.replace('"', "\"\""))
    } else {
        raw.to_string()
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
