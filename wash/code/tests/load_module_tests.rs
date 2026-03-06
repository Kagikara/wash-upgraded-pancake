use std::fs;
use std::path::Path;

use tempfile::tempdir;
use wash_load::{
    load_and_validate_config, load_data, ConfigError, InputFormat, LoadErrorCode, LoadStageError,
    RunMode, StaticRuleRegistry, TradeStatus,
};

fn registry() -> StaticRuleRegistry {
    StaticRuleRegistry::new(
        vec!["PriceBounds", "VolumeNonNegative", "IntraBarOrder"],
        vec!["DataIntegrity", "IntraBarLogic"],
    )
}

fn write_file(path: &Path, content: &str) {
    fs::write(path, content).expect("write file");
}

#[test]
fn load_config_and_csv_success_with_defaults() {
    let dir = tempdir().expect("tmp dir");
    let csv_path = dir.path().join("raw.csv");
    write_file(
        &csv_path,
        "date,ticker,open,high,low,close,vwap,volume,turnover,status\n2026-03-06,000001.SZ,10.1,10.5,9.9,10.2,10.15,1000,10000,NORMAL\n",
    );

    let cfg_path = dir.path().join("config.yaml");
    let cfg_text = format!(
        "mode: review-only
input:
  path: \"{}\"
  format: csv
  schema:
    date: date
    ticker: ticker
    open: open
    high: high
    low: low
    close: close
    vwap: vwap
    volume: volume
    turnover: turnover
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

    let cfg = load_and_validate_config(&cfg_path, &registry()).expect("valid config");
    assert_eq!(cfg.mode, RunMode::ReviewOnly);
    assert_eq!(cfg.input.format, InputFormat::Csv);
    assert_eq!(
        cfg.calendar.trading_calendar_path.to_string_lossy(),
        "data/default_trading_calendar.csv"
    );
    assert_eq!(
        cfg.market_rules.path.to_string_lossy(),
        "data/default_market_rules.yaml"
    );

    let output = load_data(&cfg).expect("load success");
    assert_eq!(output.records.len(), 1);
    assert_eq!(output.load_errors.len(), 0);

    let rec = &output.records[0];
    assert_eq!(rec.date, "2026-03-06");
    assert_eq!(rec.ticker, "000001.SZ");
    assert_eq!(rec.status, TradeStatus::Normal);
}

#[test]
fn missing_config_file_returns_not_found() {
    let dir = tempdir().expect("tmp dir");
    let cfg_path = dir.path().join("missing.yaml");

    let err = load_and_validate_config(&cfg_path, &registry()).expect_err("must fail");
    match err {
        ConfigError::NotFound(_) => {}
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn invalid_yaml_returns_error() {
    let dir = tempdir().expect("tmp dir");
    let cfg_path = dir.path().join("bad.yaml");
    write_file(&cfg_path, "mode: review-only\ninput: [\n");

    let err = load_and_validate_config(&cfg_path, &registry()).expect_err("must fail");
    match err {
        ConfigError::InvalidYaml(_) => {}
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn unknown_category_returns_error() {
    let dir = tempdir().expect("tmp dir");
    let cfg_path = dir.path().join("bad_category.yaml");
    write_file(
        &cfg_path,
        "mode: review-only
input:
  path: data/raw.csv
  format: csv
  schema:
    date: date
    ticker: ticker
    open: open
    high: high
    low: low
    close: close
    vwap: vwap
    volume: volume
    turnover: turnover
    status: status
rules:
  enabled_categories: [\"NoSuchCategory\"]
  enabled_rules: []
  disabled_rules: []
",
    );

    let err = load_and_validate_config(&cfg_path, &registry()).expect_err("must fail");
    match err {
        ConfigError::UnknownCategory(v) => assert_eq!(v, "NoSuchCategory"),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn unknown_rule_in_policy_returns_error() {
    let dir = tempdir().expect("tmp dir");
    let cfg_path = dir.path().join("bad_policy.yaml");
    write_file(
        &cfg_path,
        "mode: review-only
input:
  path: data/raw.csv
  format: csv
  schema:
    date: date
    ticker: ticker
    open: open
    high: high
    low: low
    close: close
    vwap: vwap
    volume: volume
    turnover: turnover
    status: status
rules:
  enabled_categories: [\"DataIntegrity\"]
  enabled_rules: []
  disabled_rules: []
handling:
  policies:
    - rule_name: NoSuchRule
      action: fix
      params: {}
",
    );

    let err = load_and_validate_config(&cfg_path, &registry()).expect_err("must fail");
    match err {
        ConfigError::UnknownPolicyRule(v) => assert_eq!(v, "NoSuchRule"),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn parquet_format_returns_unsupported_error() {
    let dir = tempdir().expect("tmp dir");
    let cfg_path = dir.path().join("parquet.yaml");
    write_file(
        &cfg_path,
        "mode: review-only
input:
  path: data/raw.parquet
  format: parquet
  schema:
    date: date
    ticker: ticker
    open: open
    high: high
    low: low
    close: close
    vwap: vwap
    volume: volume
    turnover: turnover
    status: status
rules:
  enabled_categories: [\"DataIntegrity\"]
  enabled_rules: []
  disabled_rules: []
handling:
  policies: []
",
    );

    let cfg = load_and_validate_config(&cfg_path, &registry()).expect("config ok");
    let err = load_data(&cfg).expect_err("must fail");
    match err {
        LoadStageError::UnsupportedFormat(v) => assert_eq!(v, "parquet"),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn csv_parse_fail_collects_load_error_without_crash() {
    let dir = tempdir().expect("tmp dir");
    let csv_path = dir.path().join("raw.csv");
    write_file(
        &csv_path,
        "date,ticker,open,high,low,close,vwap,volume,turnover,status\n2026-03-06,000001.SZ,10.1,10.5,9.9,10.2,10.15,1000,10000,NORMAL\n2026-03-07,000002.SZ,not_decimal,10.8,10.0,10.4,10.3,2000,20000,HALTED\n",
    );

    let cfg_path = dir.path().join("config.yaml");
    let cfg_text = format!(
        "mode: review-only
input:
  path: \"{}\"
  format: csv
  schema:
    date: date
    ticker: ticker
    open: open
    high: high
    low: low
    close: close
    vwap: vwap
    volume: volume
    turnover: turnover
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

    let cfg = load_and_validate_config(&cfg_path, &registry()).expect("config ok");
    let output = load_data(&cfg).expect("load should finish");

    assert_eq!(output.records.len(), 1);
    assert_eq!(output.load_errors.len(), 1);
    assert_eq!(output.load_errors[0].error_code, LoadErrorCode::TypeCastFail);
}

#[test]
fn missing_input_file_returns_open_error() {
    let dir = tempdir().expect("tmp dir");
    let missing_csv = dir.path().join("missing.csv");
    let cfg_path = dir.path().join("config.yaml");

    let cfg_text = format!(
        "mode: review-only
input:
  path: \"{}\"
  format: csv
  schema:
    date: date
    ticker: ticker
    open: open
    high: high
    low: low
    close: close
    vwap: vwap
    volume: volume
    turnover: turnover
    status: status
rules:
  enabled_categories: [\"DataIntegrity\"]
  enabled_rules: []
  disabled_rules: []
handling:
  policies: []
",
        missing_csv.display()
    );
    write_file(&cfg_path, &cfg_text);

    let cfg = load_and_validate_config(&cfg_path, &registry()).expect("config ok");
    let err = load_data(&cfg).expect_err("must fail");

    match err {
        LoadStageError::OpenInput(v) => assert_eq!(v, missing_csv.display().to_string()),
        other => panic!("unexpected error: {other:?}"),
    }
}
