use std::collections::{HashMap, HashSet};

use rust_decimal::Decimal;
use wash_load::{
    validate_records, Issue, IssueType, Record, RegistryComposeError, RuleMetadata, RuleSeverity,
    RuleSwitchConfig, TradeStatus, ValidationContext, ValidationError, ValidationPlan,
    ValidationRegistry, ValidationRule, ValidationRulePack,
};

struct AlwaysIssueExtensionRule;

impl ValidationRule for AlwaysIssueExtensionRule {
    fn name(&self) -> &'static str {
        "AlwaysIssueExtensionRule"
    }

    fn category(&self) -> &'static str {
        "Extension"
    }

    fn metadata(&self) -> RuleMetadata {
        RuleMetadata {
            name: self.name().to_string(),
            category: self.category().to_string(),
            required_fields: vec!["ticker".to_string(), "date".to_string()],
            default_severity: RuleSeverity::Low,
            configurable_thresholds: Vec::new(),
        }
    }

    fn validate(
        &self,
        records: &[Record],
        _ctx: &ValidationContext,
        _plan: &ValidationPlan,
    ) -> Result<Vec<Issue>, ValidationError> {
        let mut out = Vec::new();
        for row in records {
            out.push(Issue {
                issue_type: IssueType::DuplicateDate,
                category: self.category().to_string(),
                rule_name: self.name().to_string(),
                ticker: row.ticker.clone(),
                date: row.date.clone(),
                field: "ticker".to_string(),
                value: row.ticker.clone(),
                detail: "extension probe".to_string(),
            });
        }
        Ok(out)
    }
}

struct ExtensionRulePack;

impl ValidationRulePack for ExtensionRulePack {
    fn pack_name(&self) -> &'static str {
        "extension-pack"
    }

    fn rules(&self) -> Vec<Box<dyn ValidationRule>> {
        vec![Box::new(AlwaysIssueExtensionRule)]
    }
}

struct DuplicateNamePack;

impl ValidationRulePack for DuplicateNamePack {
    fn pack_name(&self) -> &'static str {
        "duplicate-name-pack"
    }

    fn rules(&self) -> Vec<Box<dyn ValidationRule>> {
        vec![Box::new(AlwaysIssueExtensionRule), Box::new(AlwaysIssueExtensionRule)]
    }
}

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
    volume: Decimal,
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
        turnover: d("1"),
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
        params: HashMap::new(),
        thresholds: HashMap::new(),
    }
}

#[test]
fn validate_records_success_and_find_multiple_issues() {
    let records = vec![
        make_record("2026-03-03", "000001.SZ", "10.00", "10.20", "9.80", "10.10", "10.00", d("100")),
        make_record("2026-03-03", "000001.SZ", "10.05", "10.25", "9.85", "10.15", "10.10", d("100")),
        make_record("2026-03-06", "000001.SZ", "10.00", "9.00", "9.50", "9.80", "12.00", d("100")),
        make_record("2026-03-08", "000001.SZ", "10.00", "10.20", "9.90", "10.10", "10.05", d("100")),
        make_record("2026-03-07", "000002.SZ", "-1.00", "1.00", "0.50", "0.80", "0.70", d("10")),
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
        make_record("2026-03-03", "000001.SZ", "10.00", "10.20", "9.80", "10.10", "10.00", d("100")),
        make_record("2026-03-03", "000001.SZ", "10.05", "10.25", "9.85", "10.15", "10.10", d("100")),
    ];
    let ctx = ValidationContext::new(vec!["2026-03-03".to_string()], d("0.01"));

    let plan = ValidationPlan {
        enabled_categories: HashSet::new(),
        enabled_rules: ["DuplicateDatesRule"].into_iter().map(str::to_string).collect(),
        disabled_rules: HashSet::new(),
        params: HashMap::new(),
        thresholds: HashMap::new(),
    };

    let out = validate_records(&records, &ctx, &plan, &ValidationRegistry::default())
        .expect("validation success");

    assert_eq!(out.total_issues, 1);
    assert_eq!(out.issues[0].rule_name, "DuplicateDatesRule");

    let plan_with_disable = ValidationPlan {
        enabled_categories: HashSet::new(),
        enabled_rules: ["DuplicateDatesRule"].into_iter().map(str::to_string).collect(),
        disabled_rules: ["DuplicateDatesRule"].into_iter().map(str::to_string).collect(),
        params: HashMap::new(),
        thresholds: HashMap::new(),
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
        d("100"),
    )];
    let ctx = ValidationContext::new(vec!["2026-03-03".to_string()], d("0.01"));

    let plan = ValidationPlan {
        enabled_categories: ["NoSuchCategory"]
            .into_iter()
            .map(str::to_string)
            .collect(),
        enabled_rules: HashSet::new(),
        disabled_rules: HashSet::new(),
        params: HashMap::new(),
        thresholds: HashMap::new(),
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
        d("100"),
    )];
    let ctx = ValidationContext::new(vec!["2026-03-03".to_string()], d("0.01"));

    let plan = ValidationPlan {
        enabled_categories: HashSet::new(),
        enabled_rules: ["NoSuchRule"].into_iter().map(str::to_string).collect(),
        disabled_rules: HashSet::new(),
        params: HashMap::new(),
        thresholds: HashMap::new(),
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
        version: 1,
        enabled_categories: vec!["DataIntegrity".to_string()],
        enabled_rules: vec!["DuplicateDatesRule".to_string()],
        disabled_rules: vec!["NonTradingDayRule".to_string()],
        params: HashMap::new(),
        thresholds: HashMap::new(),
    };

    let plan = ValidationPlan::from_rule_switch(&switch);

    assert!(plan.enabled_categories.contains("DataIntegrity"));
    assert!(plan.enabled_rules.contains("DuplicateDatesRule"));
    assert!(plan.disabled_rules.contains("NonTradingDayRule"));
}

#[test]
fn high_low_logic_threshold_can_suppress_small_deviation() {
    let records = vec![make_record(
        "2026-03-03",
        "000001.SZ",
        "10.00",
        "9.995",
        "9.90",
        "9.99",
        "9.97",
        d("100"),
    )];
    let ctx = ValidationContext::new(vec!["2026-03-03".to_string()], d("0.01"));

    let strict_plan = ValidationPlan {
        enabled_categories: HashSet::new(),
        enabled_rules: ["HighLowLogicRule"].into_iter().map(str::to_string).collect(),
        disabled_rules: HashSet::new(),
        params: HashMap::new(),
        thresholds: HashMap::new(),
    };

    let strict_out = validate_records(&records, &ctx, &strict_plan, &ValidationRegistry::default())
        .expect("validation should succeed");
    assert!(strict_out.total_issues >= 1);

    let tolerant_plan = ValidationPlan {
        enabled_categories: HashSet::new(),
        enabled_rules: ["HighLowLogicRule"].into_iter().map(str::to_string).collect(),
        disabled_rules: HashSet::new(),
        params: HashMap::new(),
        thresholds: HashMap::from([(
            "HighLowLogicRule".to_string(),
            HashMap::from([("epsilon".to_string(), d("0.01"))]),
        )]),
    };

    let tolerant_out = validate_records(
        &records,
        &ctx,
        &tolerant_plan,
        &ValidationRegistry::default(),
    )
    .expect("validation should succeed");
    assert_eq!(tolerant_out.total_issues, 0);
}

#[test]
fn registry_metadata_catalog_contains_threshold_definitions() {
    let catalog = ValidationRegistry::default().metadata_catalog();
    let high_low = catalog
        .iter()
        .find(|m| m.name == "HighLowLogicRule")
        .expect("high-low metadata exists");

    assert_eq!(high_low.category, "IntraBarLogic");
    assert!(high_low.required_fields.contains(&"high".to_string()));
    assert!(high_low
        .configurable_thresholds
        .iter()
        .any(|t| t.key == "epsilon"));
}

#[test]
fn registry_builder_can_compose_core_and_extension_pack() {
    let registry = ValidationRegistry::builder()
        .with_core_rules()
        .add_pack(Box::new(ExtensionRulePack))
        .build()
        .expect("composed registry");

    let plan = ValidationPlan {
        enabled_categories: HashSet::new(),
        enabled_rules: ["AlwaysIssueExtensionRule"]
            .into_iter()
            .map(str::to_string)
            .collect(),
        disabled_rules: HashSet::new(),
        params: HashMap::new(),
        thresholds: HashMap::new(),
    };

    let records = vec![make_record(
        "2026-03-03",
        "000001.SZ",
        "10.00",
        "10.20",
        "9.80",
        "10.10",
        "10.00",
        d("100"),
    )];
    let ctx = ValidationContext::new(vec!["2026-03-03".to_string()], d("0.01"));

    let out = validate_records(&records, &ctx, &plan, &registry).expect("validate success");
    assert_eq!(out.total_issues, 1);
    assert_eq!(out.issues[0].rule_name, "AlwaysIssueExtensionRule");
}

#[test]
fn registry_builder_rejects_duplicate_rule_names() {
    match ValidationRegistry::builder()
        .add_pack(Box::new(DuplicateNamePack))
        .build()
    {
        Ok(_) => panic!("must fail on duplicate rule names"),
        Err(RegistryComposeError::DuplicateRuleName(name)) => {
            assert_eq!(name, "AlwaysIssueExtensionRule")
        }
    }
}
