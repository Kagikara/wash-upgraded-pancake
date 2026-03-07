use std::collections::HashSet;
use std::path::PathBuf;

use tempfile::tempdir;
use wash_load::{
    BasicReviewChartRenderer, DefaultReviewStage, FileDisabledIssueProvider, FileReviewReportStore,
    Issue, IssueType, ReviewChartConfig, ReviewChartRenderer, ReviewChartType, ReviewConfig,
    ReviewError, ReviewOutput, ReviewPreviewConfig, ReviewPreviewEngine, ReviewReport,
    ReviewReportStore, ReviewStage, SuggestedFix,
};

fn make_issue(
    issue_type: IssueType,
    category: &str,
    rule_name: &str,
    ticker: &str,
    date: &str,
    field: &str,
    value: &str,
    detail: &str,
) -> Issue {
    Issue {
        issue_type,
        category: category.to_string(),
        rule_name: rule_name.to_string(),
        ticker: ticker.to_string(),
        date: date.to_string(),
        field: field.to_string(),
        value: value.to_string(),
        detail: detail.to_string(),
    }
}

fn config_with_output(output_dir: PathBuf) -> ReviewConfig {
    let mut chart_types = HashSet::new();
    chart_types.insert(ReviewChartType::IssueByDate);

    ReviewConfig {
        charts: ReviewChartConfig {
            enabled: true,
            types: chart_types,
        },
        preview: ReviewPreviewConfig {
            enabled: true,
            sample_size: 10,
        },
        output_dir,
    }
}

#[derive(Debug, Clone, Copy)]
struct FixedPreviewEngine;

impl ReviewPreviewEngine for FixedPreviewEngine {
    fn suggest_fix(&self, issue: &Issue) -> Result<SuggestedFix, ReviewError> {
        Ok(SuggestedFix {
            action: "simulate".to_string(),
            reason: format!("preview for {}", issue.rule_name),
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct FailingChartRenderer;

impl ReviewChartRenderer for FailingChartRenderer {
    fn render(
        &self,
        _chart_type: ReviewChartType,
        _issues: &[Issue],
        _stats: &wash_load::ReviewStats,
    ) -> Result<wash_load::ReviewChart, ReviewError> {
        Err(ReviewError::Chart("chart failed".to_string()))
    }
}

#[derive(Debug, Clone, Copy)]
struct FailingPreviewEngine;

impl ReviewPreviewEngine for FailingPreviewEngine {
    fn suggest_fix(&self, _issue: &Issue) -> Result<SuggestedFix, ReviewError> {
        Err(ReviewError::Preview("preview failed".to_string()))
    }
}

#[derive(Debug, Clone, Copy)]
struct FailingReportStore;

impl ReviewReportStore for FailingReportStore {
    fn save(&self, _report: &ReviewReport, _config: &ReviewConfig) -> Result<(), ReviewError> {
        Err(ReviewError::Persist("persist failed".to_string()))
    }
}

#[test]
fn review_stage_success_with_file_provider_and_store() {
    let dir = tempdir().expect("tempdir");
    let output_dir = dir.path().join("review_out");
    std::fs::create_dir_all(&output_dir).expect("create review_out");

    let disabled_yaml = r#"
rules:
  - rule_names: ["DuplicateDatesRule"]
    tickers: ["000001.SZ"]
"#;
    std::fs::write(output_dir.join("disabled_issues.yaml"), disabled_yaml)
        .expect("write disabled rules");

    let issues = vec![
        make_issue(
            IssueType::DuplicateDate,
            "DataIntegrity",
            "DuplicateDatesRule",
            "000001.SZ",
            "2026-03-03",
            "date",
            "2026-03-03",
            "dup",
        ),
        make_issue(
            IssueType::NegativePrice,
            "IntraBarLogic",
            "NegativePriceRule",
            "000002.SZ",
            "2026-03-03",
            "price",
            "open=-1",
            "neg",
        ),
    ];

    let stage = DefaultReviewStage::new(
        FileDisabledIssueProvider::default(),
        BasicReviewChartRenderer,
        FixedPreviewEngine,
        FileReviewReportStore::default(),
    );

    let out = stage
        .run(&issues, &config_with_output(output_dir.clone()))
        .expect("review stage should succeed");

    assert_eq!(out.disabled_issues.len(), 1);
    assert_eq!(out.approved_issues.len(), 1);
    assert_eq!(out.review_report.stats.total_issues, 2);

    let report_path = output_dir.join("review_report.txt");
    let report_content = std::fs::read_to_string(report_path).expect("report file must exist");
    assert!(report_content.contains("total_issues: 2"));
    assert!(report_content.contains("DuplicateDatesRule"));
}

#[test]
fn review_stage_invalid_disabled_issue_type_returns_error() {
    let dir = tempdir().expect("tempdir");
    let output_dir = dir.path().join("review_out");
    std::fs::create_dir_all(&output_dir).expect("create review_out");

    let disabled_yaml = r#"
rules:
  - issue_types: ["NoSuchIssueType"]
"#;
    std::fs::write(output_dir.join("disabled_issues.yaml"), disabled_yaml)
        .expect("write disabled rules");

    let mut config = config_with_output(output_dir);
    config.preview.enabled = false;

    let issues = vec![make_issue(
        IssueType::DuplicateDate,
        "DataIntegrity",
        "DuplicateDatesRule",
        "000001.SZ",
        "2026-03-03",
        "date",
        "2026-03-03",
        "dup",
    )];

    let stage = DefaultReviewStage::new(
        FileDisabledIssueProvider::default(),
        BasicReviewChartRenderer,
        FixedPreviewEngine,
        FileReviewReportStore::default(),
    );

    let err = stage.run(&issues, &config).expect_err("must fail");
    match err {
        ReviewError::DisabledRules(msg) => {
            assert!(msg.contains("NoSuchIssueType"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn review_stage_chart_error_is_propagated() {
    let dir = tempdir().expect("tempdir");
    let output_dir = dir.path().join("review_out");
    let config = config_with_output(output_dir);

    let issues = vec![make_issue(
        IssueType::DuplicateDate,
        "DataIntegrity",
        "DuplicateDatesRule",
        "000001.SZ",
        "2026-03-03",
        "date",
        "2026-03-03",
        "dup",
    )];

    let stage = DefaultReviewStage::new(
        FileDisabledIssueProvider::default(),
        FailingChartRenderer,
        FixedPreviewEngine,
        FileReviewReportStore::default(),
    );

    let err = stage.run(&issues, &config).expect_err("must fail");
    match err {
        ReviewError::Chart(msg) => assert_eq!(msg, "chart failed"),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn review_stage_preview_error_is_propagated() {
    let dir = tempdir().expect("tempdir");
    let output_dir = dir.path().join("review_out");
    let config = config_with_output(output_dir);

    let issues = vec![make_issue(
        IssueType::DuplicateDate,
        "DataIntegrity",
        "DuplicateDatesRule",
        "000001.SZ",
        "2026-03-03",
        "date",
        "2026-03-03",
        "dup",
    )];

    let stage = DefaultReviewStage::new(
        FileDisabledIssueProvider::default(),
        BasicReviewChartRenderer,
        FailingPreviewEngine,
        FileReviewReportStore::default(),
    );

    let err = stage.run(&issues, &config).expect_err("must fail");
    match err {
        ReviewError::Preview(msg) => assert_eq!(msg, "preview failed"),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn review_stage_persist_error_is_propagated() {
    let dir = tempdir().expect("tempdir");
    let output_dir = dir.path().join("review_out");
    let config = config_with_output(output_dir);

    let issues = vec![make_issue(
        IssueType::DuplicateDate,
        "DataIntegrity",
        "DuplicateDatesRule",
        "000001.SZ",
        "2026-03-03",
        "date",
        "2026-03-03",
        "dup",
    )];

    let stage = DefaultReviewStage::new(
        FileDisabledIssueProvider::default(),
        BasicReviewChartRenderer,
        FixedPreviewEngine,
        FailingReportStore,
    );

    let err = stage.run(&issues, &config).expect_err("must fail");
    match err {
        ReviewError::Persist(msg) => assert_eq!(msg, "persist failed"),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn review_stage_disables_only_matching_issues() {
    let dir = tempdir().expect("tempdir");
    let output_dir = dir.path().join("review_out");
    std::fs::create_dir_all(&output_dir).expect("create review_out");

    let disabled_yaml = r#"
rules:
  - issue_types: ["NegativePrice"]
    tickers: ["000001.SZ"]
"#;
    std::fs::write(output_dir.join("disabled_issues.yaml"), disabled_yaml)
        .expect("write disabled rules");

    let issues = vec![
        make_issue(
            IssueType::NegativePrice,
            "IntraBarLogic",
            "NegativePriceRule",
            "000001.SZ",
            "2026-03-03",
            "price",
            "open=-1",
            "neg",
        ),
        make_issue(
            IssueType::NegativePrice,
            "IntraBarLogic",
            "NegativePriceRule",
            "000002.SZ",
            "2026-03-03",
            "price",
            "open=-1",
            "neg",
        ),
    ];

    let stage = DefaultReviewStage::new(
        FileDisabledIssueProvider::default(),
        BasicReviewChartRenderer,
        FixedPreviewEngine,
        FileReviewReportStore::default(),
    );

    let out: ReviewOutput = stage
        .run(&issues, &config_with_output(output_dir))
        .expect("review stage should succeed");

    assert_eq!(out.disabled_issues.len(), 1);
    assert_eq!(out.approved_issues.len(), 1);
    assert_eq!(out.disabled_issues[0].ticker, "000001.SZ");
    assert_eq!(out.approved_issues[0].ticker, "000002.SZ");
}
