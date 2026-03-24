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
    assert_eq!(cfg.rules.version, 1);
    assert!(cfg.rules.params.is_empty());
    assert_eq!(
      cfg.calendar.trading_calendar_path,
      dir.path().join("data/default_trading_calendar.csv")
    );
    assert_eq!(
      cfg.market_rules.path,
      dir.path().join("data/default_market_rules.yaml")
    );

    let output = load_data(&cfg).expect("load success");
    assert_eq!(output.total_rows, 1);
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
fn tagged_policy_action_parses_into_strong_type() {
    let dir = tempdir().expect("tmp dir");
    let cfg_path = dir.path().join("tagged_policy.yaml");
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
  enabled_categories: [\"IntraBarLogic\"]
  enabled_rules: []
  disabled_rules: []
handling:
  policies:
    - rule_name: PriceBounds
      action:
        type: clamp_field
        min_field: low
        max_field: high
",
    );

    let cfg = load_and_validate_config(&cfg_path, &registry()).expect("valid config");
    assert_eq!(cfg.handling.policies.len(), 1);
}

#[test]
fn unsupported_policy_action_fails_at_load_time() {
    let dir = tempdir().expect("tmp dir");
    let cfg_path = dir.path().join("bad_action.yaml");
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
    - rule_name: PriceBounds
      action: unsupported_action
      params: {}
",
    );

    let err = load_and_validate_config(&cfg_path, &registry()).expect_err("must fail");
    match err {
        ConfigError::Schema(v) => assert!(v.contains("unsupported policy action")),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn unknown_threshold_key_fails_at_load_time() {
    let dir = tempdir().expect("tmp dir");
    let cfg_path = dir.path().join("bad_threshold_key.yaml");
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
  enabled_categories: [\"IntraBarLogic\"]
  enabled_rules: []
  disabled_rules: []
  thresholds:
    HighLowLogicRule:
      unknown_key: 0.1
handling:
  policies: []
",
    );

    let threshold_registry = StaticRuleRegistry::new(
        vec!["HighLowLogicRule", "NegativePriceRule", "TickSizeRule", "VwapRangeRule"],
        vec!["IntraBarLogic"],
    );
    let err = load_and_validate_config(&cfg_path, &threshold_registry).expect_err("must fail");
    match err {
        ConfigError::Schema(v) => assert!(v.contains("unknown threshold key")),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn thresholds_parse_from_yaml_string_or_number() {
    let dir = tempdir().expect("tmp dir");
    let cfg_path = dir.path().join("thresholds.yaml");
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
  enabled_categories: [\"IntraBarLogic\"]
  enabled_rules: []
  disabled_rules: []
  thresholds:
    HighLowLogicRule:
      epsilon: 0.02
handling:
  policies: []
",
    );

    let threshold_registry = StaticRuleRegistry::new(
        vec!["HighLowLogicRule", "NegativePriceRule", "TickSizeRule", "VwapRangeRule"],
        vec!["IntraBarLogic"],
    );
    let cfg = load_and_validate_config(&cfg_path, &threshold_registry).expect("config should parse");
    let epsilon = cfg
        .rules
        .thresholds
        .get("HighLowLogicRule")
        .and_then(|m| m.get("epsilon"))
        .expect("epsilon configured");

    assert_eq!(*epsilon, "0.02".parse().expect("decimal"));
}

#[test]
fn unsupported_rules_version_fails_at_load_time() {
    let dir = tempdir().expect("tmp dir");
    let cfg_path = dir.path().join("bad_rules_version.yaml");
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
  version: 99
  enabled_categories: [\"IntraBarLogic\"]
  enabled_rules: []
  disabled_rules: []
handling:
  policies: []
",
    );

    let threshold_registry = StaticRuleRegistry::new(
        vec!["HighLowLogicRule", "NegativePriceRule", "TickSizeRule", "VwapRangeRule"],
        vec!["IntraBarLogic"],
    );
    let err = load_and_validate_config(&cfg_path, &threshold_registry).expect_err("must fail");
    match err {
        ConfigError::Schema(v) => assert!(v.contains("unsupported rules.version")),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn rule_params_accept_extension_keys_and_override_known_thresholds() {
    let dir = tempdir().expect("tmp dir");
    let cfg_path = dir.path().join("rule_params.yaml");
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
  version: 1
  enabled_categories: [\"IntraBarLogic\"]
  enabled_rules: []
  disabled_rules: []
  thresholds:
    HighLowLogicRule:
      epsilon: 0.02
  params:
    HighLowLogicRule:
      epsilon: 0.03
      future_toggle: enabled
handling:
  policies: []
",
    );

    let threshold_registry = StaticRuleRegistry::new(
        vec!["HighLowLogicRule", "NegativePriceRule", "TickSizeRule", "VwapRangeRule"],
        vec!["IntraBarLogic"],
    );
    let cfg = load_and_validate_config(&cfg_path, &threshold_registry).expect("config should parse");

    assert_eq!(cfg.rules.version, 1);
    let epsilon = cfg
        .rules
        .thresholds
        .get("HighLowLogicRule")
        .and_then(|m| m.get("epsilon"))
        .expect("epsilon configured");
    assert_eq!(*epsilon, "0.03".parse().expect("decimal"));

    let future_toggle = cfg
        .rules
        .params
        .get("HighLowLogicRule")
        .and_then(|m| m.get("future_toggle"))
        .expect("future toggle preserved");
    assert_eq!(future_toggle, "enabled");
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

    assert_eq!(output.total_rows, 2);
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

#[test]
fn missing_status_column_defaults_to_normal() {
    let dir = tempdir().expect("tmp dir");
    let csv_path = dir.path().join("raw.csv");
    write_file(
        &csv_path,
        "date,ticker,open,high,low,close,vwap,volume,turnover\n2026-03-06,000001.SZ,10.1,10.5,9.9,10.2,10.15,1000,10000\n",
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

    assert_eq!(output.total_rows, 1);
    assert_eq!(output.records.len(), 1);
    assert_eq!(output.load_errors.len(), 0);
    assert_eq!(output.records[0].status, TradeStatus::Normal);
}

#[test]
fn blank_status_value_defaults_to_normal() {
    let dir = tempdir().expect("tmp dir");
    let csv_path = dir.path().join("raw.csv");
    write_file(
        &csv_path,
        "date,ticker,open,high,low,close,vwap,volume,turnover,status\n2026-03-06,000001.SZ,10.1,10.5,9.9,10.2,10.15,1000,10000,\n",
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

    assert_eq!(output.total_rows, 1);
    assert_eq!(output.records.len(), 1);
    assert_eq!(output.load_errors.len(), 0);
    assert_eq!(output.records[0].status, TradeStatus::Normal);
}
