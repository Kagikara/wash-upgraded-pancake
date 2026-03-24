use super::*;

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
