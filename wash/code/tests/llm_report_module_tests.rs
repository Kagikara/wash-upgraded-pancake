use std::collections::HashMap;

use tempfile::tempdir;
use wash_load::{
    AuditActionSource, AuditCsvSampler, AuditEntry, AuditStage, DefaultLlmReportService,
    FileAuditCsvSampler, FileLlmReportStore, LlmClient, LlmGenerateRequest, LlmGenerateResponse,
    LlmReportConfig, LlmReportError, LlmReportService, LlmReportStore, PerformanceSummary,
    ReportSummaryBuilder, SimplePromptBuilder, TopKSummaryBuilder,
};

fn sample_summary() -> PerformanceSummary {
    PerformanceSummary {
        total_rows: 10,
        total_issues: 5,
        processed_issues: 3,
        unresolved_issues: 2,
        disabled_issues: 1,
        load_error_count: 1,
        total_time_ms: 100,
        throughput_rows_per_sec: 100,
        rule_time_breakdown: HashMap::from([
            ("DuplicateDatesRule".to_string(), 11u128),
            ("NegativePriceRule".to_string(), 19u128),
        ]),
    }
}

fn sample_entries() -> Vec<AuditEntry> {
    vec![
        AuditEntry {
            timestamp: "1700000000000".to_string(),
            stage: AuditStage::Clean,
            ticker: "000001.SZ".to_string(),
            date: "2026-03-06".to_string(),
            issue_type: "NegativePrice".to_string(),
            category: "IntraBarLogic".to_string(),
            rule_name: "NegativePriceRule".to_string(),
            field: "close".to_string(),
            old_value: "-1.0".to_string(),
            new_value: "10.1".to_string(),
            action: "set_literal".to_string(),
            action_source: AuditActionSource::Auto,
            comment: "normal fix".to_string(),
        },
        AuditEntry {
            timestamp: "1700000000001".to_string(),
            stage: AuditStage::Clean,
            ticker: "000001.SZ".to_string(),
            date: "2026-03-07".to_string(),
            issue_type: "NegativePrice".to_string(),
            category: "IntraBarLogic".to_string(),
            rule_name: "NegativePriceRule".to_string(),
            field: "open".to_string(),
            old_value: "-2.0".to_string(),
            new_value: "10.2".to_string(),
            action: "set_literal".to_string(),
            action_source: AuditActionSource::Auto,
            comment: "normal fix".to_string(),
        },
        AuditEntry {
            timestamp: "1700000000002".to_string(),
            stage: AuditStage::Clean,
            ticker: "000002.SZ".to_string(),
            date: "2026-03-06".to_string(),
            issue_type: "DuplicateDate".to_string(),
            category: "DataIntegrity".to_string(),
            rule_name: "DuplicateDatesRule".to_string(),
            field: "date".to_string(),
            old_value: "2026-03-06".to_string(),
            new_value: "2026-03-06".to_string(),
            action: "UNRESOLVED".to_string(),
            action_source: AuditActionSource::Disabled,
            comment: "manual review".to_string(),
        },
    ]
}

#[derive(Debug, Clone, Copy)]
struct MockLlmClientOk;

impl LlmClient for MockLlmClientOk {
    fn generate(&self, req: &LlmGenerateRequest) -> Result<LlmGenerateResponse, LlmReportError> {
        assert_eq!(req.model, "test-model");
        assert!(req.prompt.contains("total_rows"));
        Ok(LlmGenerateResponse {
            text: "# Cleaning Report\nAll good.".to_string(),
            usage_prompt_tokens: Some(100),
            usage_completion_tokens: Some(50),
            latency_ms: Some(20),
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct MockLlmClientFail;

impl LlmClient for MockLlmClientFail {
    fn generate(&self, _req: &LlmGenerateRequest) -> Result<LlmGenerateResponse, LlmReportError> {
        Err(LlmReportError::Llm("upstream unavailable".to_string()))
    }
}

#[test]
fn topk_summary_builder_extracts_expected_top_items() {
    let builder = TopKSummaryBuilder;
    let config = LlmReportConfig {
        top_k_issue_types: 1,
        top_k_rules: 1,
        ..LlmReportConfig::default()
    };

    let out = builder
        .build(&sample_entries(), &sample_summary(), &config)
        .expect("summary build must pass");

    assert_eq!(out.total_rows, 10);
    assert_eq!(out.top_issue_types.len(), 1);
    assert_eq!(out.top_issue_types[0], ("NegativePrice".to_string(), 2));
    assert_eq!(out.top_rules.len(), 1);
    assert_eq!(out.top_rules[0], ("NegativePriceRule".to_string(), 2));
}

#[test]
fn file_audit_csv_sampler_reads_limited_rows() {
    let dir = tempdir().expect("tempdir");
    let csv_path = dir.path().join("audit.csv");
    std::fs::write(
        &csv_path,
        "h1,h2\na,b\nc,d\ne,f\n",
    )
    .expect("write csv");

    let sampler = FileAuditCsvSampler;
    let sample = sampler.sample_csv(&csv_path, 2).expect("sample ok");

    assert!(sample.contains("h1,h2"));
    assert!(sample.contains("a,b"));
    assert!(!sample.contains("e,f"));
}

#[test]
fn file_audit_csv_sampler_returns_sample_error_when_missing() {
    let sampler = FileAuditCsvSampler;
    let err = sampler
        .sample_csv(std::path::Path::new("/not/exist/audit.csv"), 10)
        .expect_err("must fail");

    match err {
        LlmReportError::Sample(msg) => assert!(!msg.is_empty()),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn default_llm_report_service_returns_none_when_disabled() {
    let service = DefaultLlmReportService::new(
        TopKSummaryBuilder,
        FileAuditCsvSampler,
        SimplePromptBuilder,
        MockLlmClientOk,
        FileLlmReportStore,
    );

    let config = LlmReportConfig {
        enabled: false,
        ..LlmReportConfig::default()
    };

    let out = service
        .generate(&sample_entries(), &sample_summary(), &config)
        .expect("disabled should not fail");

    assert!(out.is_none());
}

#[test]
fn default_llm_report_service_success_writes_report() {
    let dir = tempdir().expect("tempdir");
    let audit_csv_path = dir.path().join("audit").join("audit_log.csv");
    std::fs::create_dir_all(audit_csv_path.parent().expect("parent")).expect("mkdir");
    std::fs::write(&audit_csv_path, "h1,h2\na,b\n").expect("write csv");

    let report_path = dir.path().join("report").join("report.md");

    let config = LlmReportConfig {
        enabled: true,
        model: "test-model".to_string(),
        max_tokens: 512,
        output_path: report_path.clone(),
        audit_csv_path,
        ..LlmReportConfig::default()
    };

    let service = DefaultLlmReportService::new(
        TopKSummaryBuilder,
        FileAuditCsvSampler,
        SimplePromptBuilder,
        MockLlmClientOk,
        FileLlmReportStore,
    );

    let out = service
        .generate(&sample_entries(), &sample_summary(), &config)
        .expect("service should succeed")
        .expect("enabled should return report");

    assert_eq!(out.report_path, report_path);
    assert!(out.report_text.contains("Cleaning Report"));

    let content = std::fs::read_to_string(&out.report_path).expect("report file exists");
    assert!(content.contains("Cleaning Report"));
}

#[test]
fn default_llm_report_service_propagates_llm_error_when_fail_open_false() {
    let dir = tempdir().expect("tempdir");
    let audit_csv_path = dir.path().join("audit.csv");
    std::fs::write(&audit_csv_path, "h1,h2\na,b\n").expect("write csv");

    let config = LlmReportConfig {
        enabled: true,
        model: "test-model".to_string(),
        output_path: dir.path().join("report.md"),
        audit_csv_path,
        fail_open: false,
        ..LlmReportConfig::default()
    };

    let service = DefaultLlmReportService::new(
        TopKSummaryBuilder,
        FileAuditCsvSampler,
        SimplePromptBuilder,
        MockLlmClientFail,
        FileLlmReportStore,
    );

    let err = service
        .generate(&sample_entries(), &sample_summary(), &config)
        .expect_err("must propagate llm error");

    match err {
        LlmReportError::Llm(msg) => assert!(msg.contains("upstream unavailable")),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn default_llm_report_service_swallow_llm_error_when_fail_open_true() {
    let dir = tempdir().expect("tempdir");
    let audit_csv_path = dir.path().join("audit.csv");
    std::fs::write(&audit_csv_path, "h1,h2\na,b\n").expect("write csv");

    let config = LlmReportConfig {
        enabled: true,
        model: "test-model".to_string(),
        output_path: dir.path().join("report.md"),
        audit_csv_path,
        fail_open: true,
        ..LlmReportConfig::default()
    };

    let service = DefaultLlmReportService::new(
        TopKSummaryBuilder,
        FileAuditCsvSampler,
        SimplePromptBuilder,
        MockLlmClientFail,
        FileLlmReportStore,
    );

    let out = service
        .generate(&sample_entries(), &sample_summary(), &config)
        .expect("fail-open should return ok");

    assert!(out.is_none());
}

#[test]
fn file_llm_report_store_returns_persist_error_on_invalid_parent_path() {
    let dir = tempdir().expect("tempdir");
    let blocked = dir.path().join("blocked");
    std::fs::write(&blocked, "not dir").expect("write blocker");

    let store = FileLlmReportStore;
    let err = store
        .save(&blocked.join("report.md"), "x")
        .expect_err("must fail");

    match err {
        LlmReportError::Persist(msg) => assert!(!msg.is_empty()),
        other => panic!("unexpected error: {other:?}"),
    }
}
