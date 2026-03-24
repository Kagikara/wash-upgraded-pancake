use std::fs;
use std::path::{Path, PathBuf};

use rust_decimal::Decimal;
use tempfile::tempdir;
use wash_load::{
    load_and_validate_config, load_data, validate_records, IssueType, LoadErrorCode, Record,
    RunMode, StaticRuleRegistry, TradeStatus, ValidationContext, ValidationPlan,
    ValidationRegistry,
};

fn registry() -> StaticRuleRegistry {
    StaticRuleRegistry::new(
        vec!["MissingDatesRule", "DuplicateDatesRule", "NonTradingDayRule"],
        vec!["DataIntegrity", "IntraBarLogic"],
    )
}

fn write_file(path: &Path, content: &str) {
    fs::write(path, content).expect("write file");
}

fn zz500_csv_path() -> PathBuf {
    let p = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../data/zz500_data.csv")
        .canonicalize()
        .expect("zz500_data.csv should exist in repo data directory");
    assert!(p.exists(), "zz500_data.csv missing: {}", p.display());
    p
}

fn sse_calendar_path() -> PathBuf {
    let p = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../data/trading_date_SSE_20050101-20251231.csv")
        .canonicalize()
        .expect("SSE trading calendar should exist in repo data directory");
    assert!(
        p.exists(),
        "trading calendar missing: {}",
        p.display()
    );
    p
}

fn parse_decimal(raw: &str, field: &str) -> Decimal {
    raw.parse::<Decimal>()
        .unwrap_or_else(|_| panic!("invalid decimal in {field}: {raw}"))
}

fn parse_decimal_metric(raw: &str, field: &str) -> Decimal {
    parse_decimal(raw, field)
}

fn load_zz500_records_for_validation() -> Vec<Record> {
    let path = zz500_csv_path();
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(path)
        .expect("open zz500 csv");

    let mut out = Vec::new();
    for row in reader.records() {
        let rec = row.expect("csv row");
        let date = rec.get(1).expect("trade_date").to_string();
        let ticker = rec.get(0).expect("ts_code").to_string();
        let close = parse_decimal(rec.get(2).expect("close"), "close");

        out.push(Record {
            date,
            ticker,
            open: parse_decimal(rec.get(3).expect("open"), "open"),
            high: parse_decimal(rec.get(4).expect("high"), "high"),
            low: parse_decimal(rec.get(5).expect("low"), "low"),
            close,
            // zz500 raw file has no VWAP/status columns; for calendar-only checks, reuse close and NORMAL.
            vwap: parse_decimal(rec.get(2).expect("close"), "close"),
            volume: parse_decimal_metric(rec.get(9).expect("vol"), "vol"),
            turnover: parse_decimal_metric(rec.get(10).expect("amount"), "amount"),
            status: TradeStatus::Normal,
        });
    }
    out
}

fn load_sse_open_trading_days_asc() -> Vec<String> {
    let path = sse_calendar_path();
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(path)
        .expect("open trading calendar csv");

    let mut days = Vec::new();
    for row in reader.records() {
        let rec = row.expect("calendar row");
        let exchange = rec.get(0).expect("exchange");
        let cal_date = rec.get(1).expect("cal_date");
        let is_open = rec.get(2).expect("is_open");
        if exchange == "SSE" && is_open == "1" {
            days.push(cal_date.to_string());
        }
    }

    days.sort();
    days
}

#[test]
fn zz500_strict_schema_reports_missing_required_columns() {
    let dir = tempdir().expect("tmp dir");
    let cfg_path = dir.path().join("zz500_strict_schema.yaml");
    let csv_path = zz500_csv_path();

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
    vwap: vwap
    volume: vol
    turnover: amount
    status: status
rules:
  enabled_categories: [\"DataIntegrity\"]
  enabled_rules: []
  disabled_rules: []
handling:
  policies: []
",
        csv_path.display()
    );
    write_file(&cfg_path, &cfg_text);

    let cfg = load_and_validate_config(&cfg_path, &registry()).expect("config should be valid");
    assert_eq!(cfg.mode, RunMode::ReviewOnly);

    let output = load_data(&cfg).expect("loader should complete and collect row errors");
    assert_eq!(output.records.len(), 0);
    assert_eq!(output.load_errors.len(), 48);
    assert!(output
        .load_errors
        .iter()
        .all(|e| e.error_code == LoadErrorCode::MissingField));
    assert!(output
        .load_errors
        .iter()
        .all(|e| e.error_detail.contains("column not found in header: vwap")));
}

#[test]
fn zz500_numeric_contract_accepts_decimal_text_for_metrics() {
    let dir = tempdir().expect("tmp dir");
    let cfg_path = dir.path().join("zz500_numeric_contract.yaml");
    let csv_path = zz500_csv_path();

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
    write_file(&cfg_path, &cfg_text);

    let cfg = load_and_validate_config(&cfg_path, &registry()).expect("config should be valid");
    let output = load_data(&cfg).expect("loader should complete and collect row errors");

    assert_eq!(output.records.len(), 48);
    assert_eq!(output.load_errors.len(), 0);
}

#[test]
fn zz500_calendar_checks_have_no_non_trading_or_missing_day_issues() {
    let records = load_zz500_records_for_validation();
    assert_eq!(records.len(), 48);

    let trading_days = load_sse_open_trading_days_asc();
    let ctx = ValidationContext::new(
        trading_days,
        "0.0001".parse::<Decimal>().expect("decimal tick size"),
    );

    let plan = ValidationPlan {
        enabled_categories: std::collections::HashSet::new(),
        enabled_rules: ["MissingDatesRule", "NonTradingDayRule"]
            .into_iter()
            .map(str::to_string)
            .collect(),
        disabled_rules: std::collections::HashSet::new(),
        params: std::collections::HashMap::new(),
        thresholds: std::collections::HashMap::new(),
    };

    let out = validate_records(&records, &ctx, &plan, &ValidationRegistry::default())
        .expect("validation should succeed");

    assert_eq!(out.total_issues, 0);
    assert!(!out
        .issues
        .iter()
        .any(|i| i.issue_type == IssueType::NonTradingDayData));
    assert!(!out
        .issues
        .iter()
        .any(|i| i.issue_type == IssueType::MissingDates));
}