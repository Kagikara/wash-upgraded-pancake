use std::str::FromStr;

use rust_decimal::Decimal;
use tempfile::tempdir;
use wash_load::{
    load_and_validate_config, validate_records, Issue, IssueType, Record, RuleMetadata, RuleSeverity,
    StaticRuleRegistry, TradeStatus, ValidationContext, ValidationError, ValidationPlan,
    ValidationRegistry, ValidationRule, ValidationRulePack,
};

#[macro_use]
#[path = "common/rule_extension_template.rs"]
mod rule_extension_template;

fn d(v: &str) -> Decimal {
    Decimal::from_str(v).expect("valid decimal")
}

struct ExtensionPriceSpikeRule;

impl ValidationRule for ExtensionPriceSpikeRule {
    fn name(&self) -> &'static str {
        "ExtensionPriceSpikeRule"
    }

    fn category(&self) -> &'static str {
        "Extension"
    }

    fn metadata(&self) -> RuleMetadata {
        RuleMetadata {
            name: self.name().to_string(),
            category: self.category().to_string(),
            required_fields: vec!["open".to_string(), "close".to_string()],
            default_severity: RuleSeverity::Medium,
            configurable_thresholds: Vec::new(),
        }
    }

    fn validate(
        &self,
        records: &[Record],
        _ctx: &ValidationContext,
        plan: &ValidationPlan,
    ) -> Result<Vec<Issue>, ValidationError> {
        let threshold = plan
            .params
            .get(self.name())
            .and_then(|m| m.get("max_change_ratio"))
            .map(|v| {
                Decimal::from_str(v).map_err(|_| ValidationError::RuleExecution {
                    rule_name: self.name().to_string(),
                    detail: format!("invalid max_change_ratio: {v}"),
                })
            })
            .transpose()?
            .unwrap_or_else(|| d("0.05"));

        let mut issues = Vec::new();
        for row in records {
            if row.open.is_zero() {
                continue;
            }

            let ratio = (row.close - row.open).abs() / row.open.abs();
            if ratio > threshold {
                issues.push(Issue {
                    issue_type: IssueType::VwapOutOfRange,
                    category: self.category().to_string(),
                    rule_name: self.name().to_string(),
                    ticker: row.ticker.clone(),
                    date: row.date.clone(),
                    field: "close".to_string(),
                    value: row.close.to_string(),
                    detail: format!("absolute close/open change ratio {} > {}", ratio, threshold),
                });
            }
        }

        Ok(issues)
    }
}

struct ExtensionRulePack;

impl ValidationRulePack for ExtensionRulePack {
    fn pack_name(&self) -> &'static str {
        "extension-template-pack"
    }

    fn rules(&self) -> Vec<Box<dyn ValidationRule>> {
        vec![Box::new(ExtensionPriceSpikeRule)]
    }
}

fn sample_record(open: &str, close: &str) -> Record {
    Record {
        date: "2026-03-03".to_string(),
        ticker: "000001.SZ".to_string(),
        open: d(open),
        high: d("10.50"),
        low: d("9.50"),
        close: d(close),
        vwap: d("10.00"),
        volume: d("1000"),
        turnover: d("10000"),
        status: TradeStatus::Normal,
    }
}

fn extension_plan(max_change_ratio: &str) -> ValidationPlan {
    ValidationPlan {
        enabled_categories: std::collections::HashSet::new(),
        enabled_rules: ["ExtensionPriceSpikeRule"]
            .into_iter()
            .map(str::to_string)
            .collect(),
        disabled_rules: std::collections::HashSet::new(),
        params: std::collections::HashMap::from([(
            "ExtensionPriceSpikeRule".to_string(),
            std::collections::HashMap::from([(
                "max_change_ratio".to_string(),
                max_change_ratio.to_string(),
            )]),
        )]),
        thresholds: std::collections::HashMap::new(),
    }
}

fn extension_registry() -> ValidationRegistry {
    ValidationRegistry::builder()
        .add_pack(Box::new(ExtensionRulePack))
        .build()
        .expect("extension registry")
}

fn param_validation_case() {
    let dir = tempdir().expect("tmp dir");
    let cfg_path = dir.path().join("ok.yaml");
    std::fs::write(
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
  enabled_categories: [\"Extension\"]
  enabled_rules: [\"ExtensionPriceSpikeRule\"]
  disabled_rules: []
  params:
    ExtensionPriceSpikeRule:
      max_change_ratio: 0.07
handling:
  policies: []
",
    )
    .expect("write config");

    let registry = StaticRuleRegistry::new(vec!["ExtensionPriceSpikeRule"], vec!["Extension"]);
    let cfg = load_and_validate_config(&cfg_path, &registry).expect("config should parse");
    assert_eq!(cfg.rules.version, 1);
    assert_eq!(
        cfg.rules
            .params
            .get("ExtensionPriceSpikeRule")
            .and_then(|m| m.get("max_change_ratio"))
            .map(String::as_str),
        Some("0.07")
    );

    let bad_cfg_path = dir.path().join("bad.yaml");
    std::fs::write(
        &bad_cfg_path,
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
  enabled_categories: [\"Extension\"]
  enabled_rules: [\"ExtensionPriceSpikeRule\"]
  disabled_rules: []
  params:
    ExtensionPriceSpikeRule:
      max_change_ratio: [0.07]
handling:
  policies: []
",
    )
    .expect("write bad config");

    let err = load_and_validate_config(&bad_cfg_path, &registry).expect_err("must fail");
    let msg = err.to_string();
    assert!(msg.contains("rules.params value must be scalar"));
}

fn hit_case() {
    let records = vec![sample_record("10.00", "11.20")];
    let ctx = ValidationContext::new(vec!["2026-03-03".to_string()], d("0.01"));
    let plan = extension_plan("0.05");

    let out = validate_records(&records, &ctx, &plan, &extension_registry())
        .expect("validation should succeed");

    assert!(out
        .issues
        .iter()
        .any(|issue| issue.rule_name == "ExtensionPriceSpikeRule"));
}

fn false_positive_case() {
    let records = vec![sample_record("10.00", "10.20")];
    let ctx = ValidationContext::new(vec!["2026-03-03".to_string()], d("0.01"));
    let plan = extension_plan("0.05");

    let out = validate_records(&records, &ctx, &plan, &extension_registry())
        .expect("validation should succeed");

    assert_eq!(
        out.issues
            .iter()
            .filter(|issue| issue.rule_name == "ExtensionPriceSpikeRule")
            .count(),
        0
    );
}

fn performance_benchmark_case() {
    let records = (0..5000)
        .map(|i| {
            if i % 7 == 0 {
                sample_record("10.00", "11.00")
            } else {
                sample_record("10.00", "10.02")
            }
        })
        .collect::<Vec<_>>();

    let ctx = ValidationContext::new(vec!["2026-03-03".to_string()], d("0.01"));
    let plan = extension_plan("0.05");
    let registry = extension_registry();

    let elapsed_ms = rule_extension_template::benchmark_runs(20, || {
        let _ = validate_records(&records, &ctx, &plan, &registry).expect("validation should succeed");
    });

    assert!(elapsed_ms < 5000);
}

define_rule_extension_test_suite!(
    extension_price_spike_rule,
    param_validation_case,
    hit_case,
    false_positive_case,
    performance_benchmark_case
);
