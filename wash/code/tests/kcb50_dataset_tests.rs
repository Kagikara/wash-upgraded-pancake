use std::path::{Path, PathBuf};

use tempfile::tempdir;
use wash_load::{load_and_validate_config, load_data, LoadErrorCode, RunMode, StaticRuleRegistry};

fn registry() -> StaticRuleRegistry {
    StaticRuleRegistry::new(
        vec!["MissingDatesRule", "DuplicateDatesRule", "NonTradingDayRule"],
        vec!["DataIntegrity", "IntraBarLogic"],
    )
}

fn kcb50_csv_path() -> PathBuf {
    let p = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../data/kcb50_data.csv")
        .canonicalize()
        .expect("kcb50_data.csv should exist in repo data directory");
    assert!(p.exists(), "kcb50_data.csv missing: {}", p.display());
    p
}

#[test]
fn kcb50_load_reports_single_decimal_parse_error_and_total_rows() {
    let dir = tempdir().expect("tmp dir");
    let csv_path = kcb50_csv_path();

    let cfg_text = format!(
        "mode: review-only
input:
  path: \"{}\"
  format: csv
  schema:
    date: trade_date
    ticker: ts_code
    open: open
    high: high
    low: low
    close: close
    vwap: close
    volume: vol
    turnover: amount
    status: ts_code
rules:
  enabled_categories: [\"DataIntegrity\"]
  enabled_rules: []
  disabled_rules: []
handling:
  policies: []
",
        csv_path.display()
    );

    let cfg_path = dir.path().join("kcb50_loader_config.yaml");
    std::fs::write(&cfg_path, cfg_text).expect("write temp cfg");

    let cfg = load_and_validate_config(&cfg_path, &registry()).expect("config should be valid");
    assert_eq!(cfg.mode, RunMode::ReviewOnly);

    let output = load_data(&cfg).expect("loader should finish and keep row-level errors");

    assert_eq!(output.total_rows, 1019);
    assert_eq!(output.records.len(), 1018);
    assert_eq!(output.load_errors.len(), 1);

    let err = &output.load_errors[0];
    assert_eq!(err.error_code, LoadErrorCode::TypeCastFail);
    assert!(err.error_detail.contains("invalid decimal for open"));
    assert!(err.raw_row.contains("20191231"));
    assert_eq!(err.row_number, 1019);
}
