use std::collections::HashMap;

use tempfile::tempdir;
use wash_load::{
    AuditActionSource, AuditEntry, AuditError, AuditLogWriter, AuditService, AuditStage, CleanerOutput,
    DefaultAuditService, DefaultLoadErrorAuditMapper, DefaultPerformanceSummaryBuilder,
    FileAuditLogWriter, LoadError, LoadErrorAuditMapper, LoadErrorCode, PerformanceSummary,
    PerformanceSummaryBuilder, PerformanceSummaryInput,
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

fn sample_audit_entry() -> AuditEntry {
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
    }
}

#[test]
fn performance_summary_builder_computes_expected_fields() {
    let cleaner_output = CleanerOutput {
        cleaned_records: vec![],
        audit_entries: vec![],
        processed_issues: 7,
        unresolved_issues: 2,
    };

    let input = PerformanceSummaryInput {
        total_rows: 200,
        total_issues: 12,
        disabled_issues: 3,
        load_error_count: 4,
        cleaner_output: &cleaner_output,
        total_time_ms: 500,
        rule_time_breakdown: HashMap::from([("R1".to_string(), 15u128)]),
    };

    let builder = DefaultPerformanceSummaryBuilder;
    let out = builder.build(input);

    assert_eq!(out.total_rows, 200);
    assert_eq!(out.total_issues, 12);
    assert_eq!(out.processed_issues, 7);
    assert_eq!(out.unresolved_issues, 2);
    assert_eq!(out.disabled_issues, 3);
    assert_eq!(out.load_error_count, 4);
    assert_eq!(out.total_time_ms, 500);
    assert_eq!(out.throughput_rows_per_sec, 400);
    assert_eq!(out.rule_time_breakdown.get("R1"), Some(&15u128));
}

#[test]
fn performance_summary_builder_handles_zero_total_time() {
    let cleaner_output = CleanerOutput {
        cleaned_records: vec![],
        audit_entries: vec![],
        processed_issues: 0,
        unresolved_issues: 0,
    };

    let input = PerformanceSummaryInput {
        total_rows: 200,
        total_issues: 0,
        disabled_issues: 0,
        load_error_count: 0,
        cleaner_output: &cleaner_output,
        total_time_ms: 0,
        rule_time_breakdown: HashMap::new(),
    };

    let builder = DefaultPerformanceSummaryBuilder;
    let out = builder.build(input);

    assert_eq!(out.throughput_rows_per_sec, 0);
}

#[test]
fn file_audit_log_writer_writes_json_and_csv() {
    let dir = tempdir().expect("tempdir");
    let output_json = dir.path().join("audit").join("audit_log.json");
    let output_csv = dir.path().join("audit").join("audit_log.csv");

    let mut entry = sample_audit_entry();
    entry.comment = "contains , comma and \"quote\" and\nnewline".to_string();

    let writer = FileAuditLogWriter;
    writer
        .write(&[entry], &sample_summary(), &output_json, &output_csv)
        .expect("write succeeds");

    let json = std::fs::read_to_string(&output_json).expect("json exists");
    assert!(json.contains("\"audit_entries\""));
    assert!(json.contains("\"stage\":\"CLEAN\""));
    assert!(json.contains("\"action_source\":\"AUTO\""));

    let csv = std::fs::read_to_string(&output_csv).expect("csv exists");
    assert!(csv.contains("timestamp,stage,ticker,date"));
    assert!(csv.contains("CLEAN"));
    assert!(csv.contains("\"contains , comma and \"\"quote\"\" and"));
}

#[test]
fn file_audit_log_writer_returns_persist_error_on_invalid_parent_path() {
    let dir = tempdir().expect("tempdir");
    let blocked = dir.path().join("blocked");
    std::fs::write(&blocked, "not a directory").expect("create blocker file");

    let output_json = blocked.join("audit_log.json");
    let output_csv = blocked.join("audit_log.csv");

    let writer = FileAuditLogWriter;
    let err = writer
        .write(&[sample_audit_entry()], &sample_summary(), &output_json, &output_csv)
        .expect_err("must fail");

    match err {
        AuditError::Persist(msg) => assert!(!msg.is_empty()),
    }
}

#[test]
fn default_audit_service_publish_success() {
    let dir = tempdir().expect("tempdir");
    let output_json = dir.path().join("audit").join("audit_log.json");
    let output_csv = dir.path().join("audit").join("audit_log.csv");

    let cleaner_output = CleanerOutput {
        cleaned_records: vec![],
        audit_entries: vec![sample_audit_entry()],
        processed_issues: 5,
        unresolved_issues: 1,
    };

    let input = PerformanceSummaryInput {
        total_rows: 100,
        total_issues: 8,
        disabled_issues: 2,
        load_error_count: 1,
        cleaner_output: &cleaner_output,
        total_time_ms: 250,
        rule_time_breakdown: HashMap::from([("R1".to_string(), 20u128)]),
    };

    let service = DefaultAuditService::new(DefaultPerformanceSummaryBuilder, FileAuditLogWriter);
    let summary = service
        .publish(
            &cleaner_output.audit_entries,
            input,
            &output_json,
            &output_csv,
        )
        .expect("publish succeeds");

    assert_eq!(summary.total_rows, 100);
    assert_eq!(summary.processed_issues, 5);
    assert!(output_json.exists());
    assert!(output_csv.exists());
}

#[test]
fn default_audit_service_propagates_writer_error() {
    let dir = tempdir().expect("tempdir");
    let blocked = dir.path().join("blocked");
    std::fs::write(&blocked, "not a directory").expect("create blocker file");

    let output_json = blocked.join("audit_log.json");
    let output_csv = blocked.join("audit_log.csv");

    let cleaner_output = CleanerOutput {
        cleaned_records: vec![],
        audit_entries: vec![sample_audit_entry()],
        processed_issues: 1,
        unresolved_issues: 0,
    };

    let input = PerformanceSummaryInput {
        total_rows: 1,
        total_issues: 1,
        disabled_issues: 0,
        load_error_count: 0,
        cleaner_output: &cleaner_output,
        total_time_ms: 1,
        rule_time_breakdown: HashMap::new(),
    };

    let service = DefaultAuditService::new(DefaultPerformanceSummaryBuilder, FileAuditLogWriter);
    let err = service
        .publish(
            &cleaner_output.audit_entries,
            input,
            &output_json,
            &output_csv,
        )
        .expect_err("must fail");

    match err {
        AuditError::Persist(msg) => assert!(!msg.is_empty()),
    }
}

#[test]
fn default_load_error_audit_mapper_maps_stage_and_raw_row() {
    let mapper = DefaultLoadErrorAuditMapper;
    let load_errors = vec![LoadError {
        stage: "LOAD",
        row_number: 9,
        raw_row: "2026-03-06,000001.SZ,bad".to_string(),
        error_code: LoadErrorCode::TypeCastFail,
        error_detail: "bad decimal".to_string(),
    }];

    let out = mapper.map(&load_errors);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].stage, AuditStage::Load);
    assert_eq!(out[0].field, "raw_row");
    assert_eq!(out[0].old_value, load_errors[0].raw_row);
    assert_eq!(out[0].new_value, load_errors[0].raw_row);
    assert_eq!(out[0].action_source, AuditActionSource::Loader);
}
