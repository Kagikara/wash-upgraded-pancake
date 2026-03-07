use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;

use tempfile::tempdir;
use wash_load::{
    load_and_validate_config, load_data, validate_records, AuditService, BasicReviewChartRenderer,
    BuiltinPolicyExecutor, CheckpointStore, CleanerStage, CommitArtifacts, DefaultAuditService,
    DefaultCleanerStage, DefaultLlmReportService, DefaultLoadErrorAuditMapper,
    DefaultPerformanceSummaryBuilder, DefaultReviewStage, DefaultVersioningService,
    FileAuditCsvSampler, FileAuditLogWriter, FileDisabledIssueProvider, FileHistoryStore,
    FileLlmReportStore, FileReviewReportStore, HandlingConfig, IssueType, LlmClient,
    LlmGenerateRequest, LlmGenerateResponse, LlmReportConfig, LlmReportError, LlmReportService,
    RecoveryService, ReviewChartConfig, ReviewChartType, ReviewConfig, ReviewPreviewConfig,
    ReviewStage, RuleNamePolicyResolver, RunMode, StaticRuleRegistry, TopKSummaryBuilder,
    ValidationContext, ValidationPlan, ValidationRegistry, VersionCommitInput, VersioningConfig,
    VersioningService,
};

fn write_file(path: &Path, content: &str) {
    fs::write(path, content).expect("write file");
}

fn write_cleaned_csv(path: &Path, records: &[wash_load::Record]) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create output dir");
    }

    let mut out = String::from("date,ticker,open,high,low,close,vwap,volume,turnover,status\n");
    for row in records {
        out.push_str(&format!(
            "{},{},{},{},{},{},{},{},{},{}\n",
            row.date,
            row.ticker,
            row.open,
            row.high,
            row.low,
            row.close,
            row.vwap,
            row.volume,
            row.turnover,
            match &row.status {
                wash_load::TradeStatus::Normal => "NORMAL".to_string(),
                wash_load::TradeStatus::Halted => "HALTED".to_string(),
                wash_load::TradeStatus::Delisted => "DELISTED".to_string(),
                wash_load::TradeStatus::Other(v) => v.clone(),
            }
        ));
    }

    fs::write(path, out).expect("write cleaned csv");
}

fn write_summary_json(path: &Path, summary: &wash_load::PerformanceSummary) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create summary dir");
    }

    let payload = format!(
        "{{\n  \"total_rows\": {},\n  \"total_issues\": {},\n  \"processed_issues\": {},\n  \"unresolved_issues\": {},\n  \"disabled_issues\": {},\n  \"load_error_count\": {},\n  \"total_time_ms\": {},\n  \"throughput_rows_per_sec\": {}\n}}\n",
        summary.total_rows,
        summary.total_issues,
        summary.processed_issues,
        summary.unresolved_issues,
        summary.disabled_issues,
        summary.load_error_count,
        summary.total_time_ms,
        summary.throughput_rows_per_sec,
    );

    fs::write(path, payload).expect("write summary json");
}

fn pipeline_registry() -> StaticRuleRegistry {
    StaticRuleRegistry::new(
        vec![
            "MissingDatesRule",
            "DuplicateDatesRule",
            "NonTradingDayRule",
            "HighLowLogicRule",
            "NegativePriceRule",
            "TickSizeRule",
            "VwapRangeRule",
        ],
        vec!["DataIntegrity", "IntraBarLogic"],
    )
}

fn validation_ctx() -> ValidationContext {
    ValidationContext::new(
        vec![
            "2026-03-03".to_string(),
            "2026-03-04".to_string(),
            "2026-03-05".to_string(),
            "2026-03-06".to_string(),
            "2026-03-07".to_string(),
        ],
        "0.01".parse().expect("decimal"),
    )
}

#[derive(Debug, Clone, Copy)]
struct MockLlmClient;

impl LlmClient for MockLlmClient {
    fn generate(&self, req: &LlmGenerateRequest) -> Result<LlmGenerateResponse, LlmReportError> {
        assert_eq!(req.model, "mock-gpt");
        assert!(req.prompt.contains("top_issue_types"));
        Ok(LlmGenerateResponse {
            text: "# Mock LLM Report\n\n- no network call\n".to_string(),
            usage_prompt_tokens: Some(64),
            usage_completion_tokens: Some(32),
            latency_ms: Some(3),
        })
    }
}

#[test]
fn user_journey_review_then_full_pipeline_with_isolated_outputs() {
    let dir = tempdir().expect("tempdir");

    // Simulate a real user preparing source data and config.
    let raw_csv = dir.path().join("input/l1_raw.csv");
    fs::create_dir_all(raw_csv.parent().expect("parent")).expect("mkdir input");
    write_file(
        &raw_csv,
        "date,ticker,open,high,low,close,vwap,volume,turnover,status\n\
2026-03-03,000001.SZ,10.00,10.30,9.90,10.10,10.05,1000,10000,NORMAL\n\
2026-03-03,000001.SZ,10.10,10.20,9.90,-1.00,12.00,800,8000,NORMAL\n\
2026-03-06,000001.SZ,10.00,9.00,9.50,9.80,8.50,900,9000,NORMAL\n\
2026-03-08,000001.SZ,10.20,10.40,10.10,10.30,10.20,1100,11000,NORMAL\n\
2026-03-07,000002.SZ,bad,10.10,9.90,10.00,10.00,500,5000,NORMAL\n",
    );

    let review_dir = dir.path().join("output/review");
    fs::create_dir_all(&review_dir).expect("mkdir review");
    write_file(
        &review_dir.join("disabled_issues.yaml"),
        "rules:\n  - rule_names: [\"DuplicateDatesRule\", \"NegativePriceRule\", \"TickSizeRule\"]\n    tickers: [\"000001.SZ\"]\n",
    );

    let review_cfg_path = dir.path().join("config_review_only.yaml");
    let review_cfg = vec![
        "mode: review-only".to_string(),
        "input:".to_string(),
        format!("  path: '{}'", raw_csv.display()),
        "  format: csv".to_string(),
        "  schema:".to_string(),
        "    date: date".to_string(),
        "    ticker: ticker".to_string(),
        "    open: open".to_string(),
        "    high: high".to_string(),
        "    low: low".to_string(),
        "    close: close".to_string(),
        "    vwap: vwap".to_string(),
        "    volume: volume".to_string(),
        "    turnover: turnover".to_string(),
        "    status: status".to_string(),
        "rules:".to_string(),
        "  enabled_categories: ['DataIntegrity', 'IntraBarLogic']".to_string(),
        "  enabled_rules: []".to_string(),
        "  disabled_rules: []".to_string(),
        "handling:".to_string(),
        "  policies:".to_string(),
        "    - rule_name: VwapRangeRule".to_string(),
        "      action: clamp_field".to_string(),
        "      params:".to_string(),
        "        min_field: low".to_string(),
        "        max_field: high".to_string(),
        "    - rule_name: HighLowLogicRule".to_string(),
        "      action: set_literal".to_string(),
        "      params:".to_string(),
        "        value: '10.20'".to_string(),
    ]
    .join("\n");
    write_file(&review_cfg_path, &review_cfg);

    // User first runs in review-only mode.
    let parsed_review_cfg =
        load_and_validate_config(&review_cfg_path, &pipeline_registry()).expect("review cfg valid");
    assert_eq!(parsed_review_cfg.mode, RunMode::ReviewOnly);

    let load_out = load_data(&parsed_review_cfg).expect("load success");
    assert_eq!(load_out.records.len(), 4);
    assert_eq!(load_out.load_errors.len(), 1);

    let validation_plan = ValidationPlan::from_rule_switch(&parsed_review_cfg.rules);
    let validation_out = validate_records(
        &load_out.records,
        &validation_ctx(),
        &validation_plan,
        &ValidationRegistry::default(),
    )
    .expect("validate success");
    assert!(validation_out.total_issues > 0);

    let review_stage = DefaultReviewStage::new(
        FileDisabledIssueProvider::default(),
        BasicReviewChartRenderer,
        wash_load::RuleBasedPreviewEngine::default(),
        FileReviewReportStore::default(),
    );
    let review_out = review_stage
        .run(
            &validation_out.issues,
            &ReviewConfig {
                charts: ReviewChartConfig {
                    enabled: true,
                    types: HashSet::from([
                        ReviewChartType::IssueByDate,
                        ReviewChartType::IssueByCategory,
                        ReviewChartType::IssueByRule,
                    ]),
                },
                preview: ReviewPreviewConfig {
                    enabled: true,
                    sample_size: 20,
                },
                output_dir: review_dir.clone(),
            },
        )
        .expect("review success");

    assert_eq!(
        review_out.approved_issues.len() + review_out.disabled_issues.len(),
        validation_out.issues.len()
    );
    assert!(review_out
        .disabled_issues
        .iter()
        .any(|i| i.issue_type == IssueType::DuplicateDate));
    assert!(review_dir.join("review_report.txt").exists());

    // Then user decides to run full mode with the reviewed disable rules kept.
    let full_cfg_path = dir.path().join("config_full.yaml");
    write_file(&full_cfg_path, &review_cfg.replace("mode: review-only", "mode: full"));
    let parsed_full_cfg =
        load_and_validate_config(&full_cfg_path, &pipeline_registry()).expect("full cfg valid");
    assert_eq!(parsed_full_cfg.mode, RunMode::Full);

    let cleaner = DefaultCleanerStage::new(
        RuleNamePolicyResolver,
        BuiltinPolicyExecutor,
        DefaultLoadErrorAuditMapper,
    );
    let cleaner_out = cleaner
        .run(
            &load_out.records,
            &review_out.approved_issues,
            &load_out.load_errors,
            &HandlingConfig {
                policies: parsed_full_cfg.handling.policies.clone(),
            },
        )
        .expect("clean success");

    // Disabled issues should never enter cleaning/audit actions.
    assert!(cleaner_out
        .audit_entries
        .iter()
        .all(|e| e.rule_name != "DuplicateDatesRule"));
    assert!(cleaner_out
        .audit_entries
        .iter()
        .all(|e| e.rule_name != "NegativePriceRule"));
    assert!(cleaner_out
        .audit_entries
        .iter()
        .all(|e| e.rule_name != "TickSizeRule"));
    assert!(cleaner_out.audit_entries.iter().any(|e| e.action == "LOAD_ERROR"));

    let repaired = cleaner_out
        .cleaned_records
        .iter()
        .find(|r| r.ticker == "000001.SZ" && r.date == "2026-03-06")
        .expect("repaired row exists");
    assert_eq!(repaired.high.to_string(), "10.20");
    assert_eq!(repaired.low.to_string(), "10.20");
    assert_eq!(repaired.vwap.to_string(), "10.20");

    let output_dir = dir.path().join("output/final");
    let cleaned_csv_path = output_dir.join("cleaned.csv");
    let audit_json_path = output_dir.join("audit_log.json");
    let audit_csv_path = output_dir.join("audit_log.csv");
    let report_path = output_dir.join("report.md");
    let summary_path = output_dir.join("summary.json");

    write_cleaned_csv(&cleaned_csv_path, &cleaner_out.cleaned_records);

    let rule_time_breakdown = validation_out
        .metrics
        .iter()
        .map(|m| (m.rule_name.clone(), m.elapsed.as_millis()))
        .collect::<HashMap<String, u128>>();

    let audit_service = DefaultAuditService::new(DefaultPerformanceSummaryBuilder, FileAuditLogWriter);
    let perf = audit_service
        .publish(
            &cleaner_out.audit_entries,
            wash_load::PerformanceSummaryInput {
                total_rows: load_out.records.len(),
                total_issues: validation_out.total_issues,
                disabled_issues: review_out.disabled_issues.len(),
                load_error_count: load_out.load_errors.len(),
                cleaner_output: &cleaner_out,
                total_time_ms: 333,
                rule_time_breakdown,
            },
            &audit_json_path,
            &audit_csv_path,
        )
        .expect("audit publish");

    assert!(audit_json_path.exists());
    assert!(audit_csv_path.exists());

    // Network isolation: LLM generation is mocked and writes only to tempdir.
    let llm_service = DefaultLlmReportService::new(
        TopKSummaryBuilder,
        FileAuditCsvSampler,
        wash_load::SimplePromptBuilder,
        MockLlmClient,
        FileLlmReportStore,
    );

    let llm_out = llm_service
        .generate(
            &cleaner_out.audit_entries,
            &perf,
            &LlmReportConfig {
                enabled: true,
                model: "mock-gpt".to_string(),
                output_path: report_path.clone(),
                audit_csv_path: audit_csv_path.clone(),
                ..LlmReportConfig::default()
            },
        )
        .expect("llm success")
        .expect("llm output");

    assert_eq!(llm_out.report_path, report_path);
    assert!(llm_out.report_text.contains("Mock LLM Report"));

    write_summary_json(&summary_path, &perf);

    let versioning_cfg = VersioningConfig {
        history_dir: dir.path().join(".history"),
        head_file: "HEAD".to_string(),
        commits_dir: "commits".to_string(),
        checkpoint_dir: dir.path().join(".checkpoint"),
    };
    let versioning = DefaultVersioningService::new(FileHistoryStore, wash_load::EpochCommitIdStrategy);
    let commit_id = versioning
        .commit(
            &versioning_cfg,
            VersionCommitInput {
                author: "test-user".to_string(),
                message: "review then full run".to_string(),
                run_mode: "full".to_string(),
                artifacts: CommitArtifacts {
                    config_yaml: full_cfg_path.clone(),
                    cleaned_csv: Some(cleaned_csv_path.clone()),
                    audit_log_json: Some(audit_json_path.clone()),
                    audit_log_csv: Some(audit_csv_path.clone()),
                    report_md: Some(report_path.clone()),
                    summary_json: summary_path.clone(),
                },
            },
        )
        .expect("version commit");

    let head = versioning
        .current_head(&versioning_cfg)
        .expect("read head")
        .expect("head exists");
    assert_eq!(head, commit_id);

    let commit_dir = versioning_cfg.history_dir.join("commits").join(commit_id);
    assert!(commit_dir.join("config.yaml").exists());
    assert!(commit_dir.join("cleaned.csv").exists());
    assert!(commit_dir.join("audit_log.json").exists());
    assert!(commit_dir.join("audit_log.csv").exists());
    assert!(commit_dir.join("report.md").exists());
    assert!(commit_dir.join("summary.json").exists());

    // Optional crash-recovery checkpoint chain validation.
    let checkpoint_store = wash_load::FileCheckpointStore;
    checkpoint_store
        .save(
            &versioning_cfg,
            "journey-run",
            wash_load::PipelineStage::Load,
            b"ok-load",
            None,
        )
        .expect("save load checkpoint");
    checkpoint_store
        .save(
            &versioning_cfg,
            "journey-run",
            wash_load::PipelineStage::Validate,
            b"ok-validate",
            None,
        )
        .expect("save validate checkpoint");

    let recovery = wash_load::DefaultRecoveryService::new(checkpoint_store);
    let plan = recovery
        .plan_resume(&versioning_cfg, "journey-run")
        .expect("plan recovery")
        .expect("plan exists");
    assert_eq!(plan.resume_from, wash_load::PipelineStage::Review);
}
