use std::collections::HashSet;

use rust_decimal::Decimal;
use wash_load::{
    validate_records, IssueType, Record, RuleSwitchConfig, TradeStatus, ValidationContext,
    ValidationError, ValidationPlan, ValidationRegistry,
};

fn d(v: &str) -> Decimal {
    v.parse::<Decimal>().expect("valid decimal")
}

fn make_record(
    date: &str,
    ticker: &str,
    open: &str,
    high: &str,
    low: &str,
    close: &str,
    vwap: &str,
    volume: i64,
) -> Record {
    Record {
        date: date.to_string(),
        ticker: ticker.to_string(),
        open: d(open),
        high: d(high),
        low: d(low),
        close: d(close),
        vwap: d(vwap),
        volume,
        turnover: 1,
        status: TradeStatus::Normal,
    }
}

fn base_plan() -> ValidationPlan {
    ValidationPlan {
        enabled_categories: ["DataIntegrity", "IntraBarLogic"]
            .into_iter()
            .map(str::to_string)
            .collect::<HashSet<_>>(),
        enabled_rules: HashSet::new(),
        disabled_rules: HashSet::new(),
    }
}

#[test]
fn validate_records_success_and_find_multiple_issues() {
    let records = vec![
        make_record("2026-03-03", "000001.SZ", "10.00", "10.20", "9.80", "10.10", "10.00", 100),
        make_record("2026-03-03", "000001.SZ", "10.05", "10.25", "9.85", "10.15", "10.10", 100),
        make_record("2026-03-06", "000001.SZ", "10.00", "9.00", "9.50", "9.80", "12.00", 100),
        make_record("2026-03-08", "000001.SZ", "10.00", "10.20", "9.90", "10.10", "10.05", 100),
        make_record("2026-03-07", "000002.SZ", "-1.00", "1.00", "0.50", "0.80", "0.70", 10),
    ];

    let ctx = ValidationContext::new(
        vec![
            "2026-03-03".to_string(),
            "2026-03-04".to_string(),
            "2026-03-05".to_string(),
            "2026-03-06".to_string(),
            "2026-03-07".to_string(),
        ],
        d("0.01"),
    );

    let out = validate_records(&records, &ctx, &base_plan(), &ValidationRegistry::default())
        .expect("validation success");

    assert!(out.total_issues >= 7);
    assert!(out
        .issues
        .iter()
        .any(|i| i.issue_type == IssueType::DuplicateDate));
    assert!(out
        .issues
        .iter()
        .any(|i| i.issue_type == IssueType::MissingDates));
    assert!(out
        .issues
        .iter()
        .any(|i| i.issue_type == IssueType::NonTradingDayData));
    assert!(out
        .issues
        .iter()
        .any(|i| i.issue_type == IssueType::HighBelowOthers));
    assert!(out
        .issues
        .iter()
        .any(|i| i.issue_type == IssueType::VwapOutOfRange));
    assert!(out
        .issues
        .iter()
        .any(|i| i.issue_type == IssueType::NegativePrice));
}

#[test]
fn enabled_rules_and_disabled_rules_filter_correctly() {
    let records = vec![
        make_record("2026-03-03", "000001.SZ", "10.00", "10.20", "9.80", "10.10", "10.00", 100),
        make_record("2026-03-03", "000001.SZ", "10.05", "10.25", "9.85", "10.15", "10.10", 100),
    ];
    let ctx = ValidationContext::new(vec!["2026-03-03".to_string()], d("0.01"));

    let plan = ValidationPlan {
        enabled_categories: HashSet::new(),
        enabled_rules: ["DuplicateDatesRule"].into_iter().map(str::to_string).collect(),
        disabled_rules: HashSet::new(),
    };

    let out = validate_records(&records, &ctx, &plan, &ValidationRegistry::default())
        .expect("validation success");

    assert_eq!(out.total_issues, 1);
    assert_eq!(out.issues[0].rule_name, "DuplicateDatesRule");

    let plan_with_disable = ValidationPlan {
        enabled_categories: HashSet::new(),
        enabled_rules: ["DuplicateDatesRule"].into_iter().map(str::to_string).collect(),
        disabled_rules: ["DuplicateDatesRule"].into_iter().map(str::to_string).collect(),
    };

    let out = validate_records(
        &records,
        &ctx,
        &plan_with_disable,
        &ValidationRegistry::default(),
    )
    .expect("validation success");

    assert_eq!(out.total_issues, 0);
}

#[test]
fn unknown_category_returns_error() {
    let records = vec![make_record(
        "2026-03-03",
        "000001.SZ",
        "10.00",
        "10.20",
        "9.80",
        "10.10",
        "10.00",
        100,
    )];
    let ctx = ValidationContext::new(vec!["2026-03-03".to_string()], d("0.01"));

    let plan = ValidationPlan {
        enabled_categories: ["NoSuchCategory"]
            .into_iter()
            .map(str::to_string)
            .collect(),
        enabled_rules: HashSet::new(),
        disabled_rules: HashSet::new(),
    };

    let err = validate_records(&records, &ctx, &plan, &ValidationRegistry::default())
        .expect_err("should fail");

    match err {
        ValidationError::UnknownCategory(v) => assert_eq!(v, "NoSuchCategory"),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn unknown_rule_returns_error() {
    let records = vec![make_record(
        "2026-03-03",
        "000001.SZ",
        "10.00",
        "10.20",
        "9.80",
        "10.10",
        "10.00",
        100,
    )];
    let ctx = ValidationContext::new(vec!["2026-03-03".to_string()], d("0.01"));

    let plan = ValidationPlan {
        enabled_categories: HashSet::new(),
        enabled_rules: ["NoSuchRule"].into_iter().map(str::to_string).collect(),
        disabled_rules: HashSet::new(),
    };

    let err = validate_records(&records, &ctx, &plan, &ValidationRegistry::default())
        .expect_err("should fail");

    match err {
        ValidationError::UnknownRule(v) => assert_eq!(v, "NoSuchRule"),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn from_rule_switch_config_builds_plan() {
    let switch = RuleSwitchConfig {
        enabled_categories: vec!["DataIntegrity".to_string()],
        enabled_rules: vec!["DuplicateDatesRule".to_string()],
        disabled_rules: vec!["NonTradingDayRule".to_string()],
    };

    let plan = ValidationPlan::from_rule_switch(&switch);

    assert!(plan.enabled_categories.contains("DataIntegrity"));
    assert!(plan.enabled_rules.contains("DuplicateDatesRule"));
    assert!(plan.disabled_rules.contains("NonTradingDayRule"));
}
