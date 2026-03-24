use super::*;

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

/// Cleaner stage applies policies on approved issues and emits audit entries.
///
/// Note that load errors are converted into audit entries and merged into the
/// same output stream for downstream reporting consistency.
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
        // Group by (ticker, date) to avoid O(records * issues) scans.
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
        let record_keys = cleaned_records
            .iter()
            .map(|r| (r.ticker.clone(), r.date.clone()))
            .collect::<HashSet<_>>();

        let mut processed_issues = 0usize;
        let mut unresolved_issues = 0usize;

        for (idx, record) in cleaned_records.iter_mut().enumerate() {
            let key = (record.ticker.clone(), record.date.clone());
            if let Some(issues) = issue_index.get(&key) {
                for issue in issues {
                    let old_value = record_field_value(record, &issue.field)?;

                    let Some(policy) = self.resolver.resolve_policy(issue, handling) else {
                        // Missing policy is tracked as unresolved instead of failing
                        // hard, so the pipeline can still produce auditable output.
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

            ValidationModule::validate_row(&records[idx], record)?;
        }

        // Some issues (for example MissingDatesRule) may reference synthetic
        // dates with no physical record row. Keep them auditable as unresolved.
        for issue in approved_issues {
            let key = (issue.ticker.clone(), issue.date.clone());
            if record_keys.contains(&key) {
                continue;
            }

            unresolved_issues += 1;
            audit_entries.push(new_audit_entry(
                issue,
                issue.value.clone(),
                issue.value.clone(),
                "UNRESOLVED".to_string(),
                AuditActionSource::Disabled,
                "No matching record found for this issue key".to_string(),
            ));
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
            action: policy.action.label().to_string(),
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

        match &policy.action {
            PolicyAction::SetLiteral { value } => {
                set_record_field(record, &issue.field, value)?;

                Ok(PolicyApplyResult {
                    action: policy.action.label().to_string(),
                    old_value,
                    new_value: record_field_value(record, &issue.field)?,
                    action_source: AuditActionSource::Auto,
                    comment: format!("set {} with literal value", issue.field),
                })
            }
            PolicyAction::ClampField {
                min_field,
                max_field,
            } => {

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
                    action: policy.action.label().to_string(),
                    old_value,
                    new_value: record_field_value(record, &issue.field)?,
                    action_source: AuditActionSource::Auto,
                    comment: format!("clamped {} to [{}, {}]", issue.field, min_field, max_field),
                })
            }
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
        "price" => Ok(format!(
            "open={},high={},low={},close={},vwap={}",
            record.open, record.high, record.low, record.close, record.vwap
        )),
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
            record.volume = parse_decimal_literal(value, field)?;
            Ok(())
        }
        "turnover" => {
            record.turnover = parse_decimal_literal(value, field)?;
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

pub(crate) fn now_epoch_millis() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis().to_string(),
        Err(_) => "0".to_string(),
    }
}

pub(crate) fn audit_action_source_name(source: AuditActionSource) -> &'static str {
    match source {
        AuditActionSource::Auto => "AUTO",
        AuditActionSource::Manual => "MANUAL",
        AuditActionSource::Disabled => "DISABLED",
        AuditActionSource::Loader => "LOADER",
    }
}

pub(crate) fn render_audit_json(
    audit_entries: &[AuditEntry],
    performance_summary: &PerformanceSummary,
) -> String {
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

pub(crate) fn json_escape(raw: &str) -> String {
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

pub(crate) fn csv_escape(raw: &str) -> String {
    if raw.contains(',') || raw.contains('"') || raw.contains('\n') || raw.contains('\r') {
        format!("\"{}\"", raw.replace('"', "\"\""))
    } else {
        raw.to_string()
    }
}

