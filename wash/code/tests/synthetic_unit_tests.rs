use std::collections::HashSet;
use std::path::PathBuf;

use rust_decimal::Decimal;
use wash_load::{
    load_data, validate_records, BuiltinPolicyExecutor, CleanerStage, DefaultCleanerStage,
    DefaultLoadErrorAuditMapper, HandlingConfig, InputConfig, InputFormat, InputSchemaMap,
    IssueType, LoadConfig, RuleNamePolicyResolver, RuleSourceConfig, RuleSwitchConfig, RunMode,
    ValidationContext, ValidationPlan, ValidationRegistry, CleanerError,
};

fn d(v: &str) -> Decimal {
    v.parse::<Decimal>().expect("valid decimal")
}

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("synthetic")
        .join(name)
}

fn synthetic_load_config(csv_name: &str) -> LoadConfig {
    LoadConfig {
        mode: RunMode::Clean,
        input: InputConfig {
            path: fixture_path(csv_name),
            format: InputFormat::Csv,
            schema: InputSchemaMap {
                date: "date".to_string(),
                ticker: "ticker".to_string(),
                open: "open".to_string(),
                high: "high".to_string(),
                low: "low".to_string(),
                close: "close".to_string(),
                vwap: "vwap".to_string(),
                volume: "volume".to_string(),
                turnover: "turnover".to_string(),
                status: "status".to_string(),
            },
        },
        calendar: wash_load::CalendarConfig {
            trading_calendar_path: PathBuf::from("unused_trading_calendar.csv"),
        },
        market_rules: RuleSourceConfig {
            path: PathBuf::from("unused_market_rules.yaml"),
        },
        corporate_actions: None,
        lifecycle_map: None,
        rules: RuleSwitchConfig {
            version: 1,
            enabled_categories: vec!["DataIntegrity".to_string(), "IntraBarLogic".to_string()],
            enabled_rules: vec![],
            disabled_rules: vec![],
            params: std::collections::HashMap::new(),
            thresholds: std::collections::HashMap::new(),
        },
        handling: HandlingConfig { policies: vec![] },
    }
}

fn default_plan() -> ValidationPlan {
    ValidationPlan {
        enabled_categories: ["DataIntegrity", "IntraBarLogic"]
            .into_iter()
            .map(str::to_string)
            .collect::<HashSet<_>>(),
        enabled_rules: HashSet::new(),
        disabled_rules: HashSet::new(),
        params: std::collections::HashMap::new(),
        thresholds: std::collections::HashMap::new(),
    }
}

#[test]
fn synthetic_case_a_extreme_error_is_detected_and_fails_fast() {
    let cfg = synthetic_load_config("case_a_extreme_error.csv");
    let load_out = load_data(&cfg).expect("csv load success");
    assert_eq!(load_out.records.len(), 10);

    let ctx = ValidationContext::new(
        vec![
            "2026-03-01".to_string(),
            "2026-03-02".to_string(),
            "2026-03-03".to_string(),
            "2026-03-04".to_string(),
            "2026-03-05".to_string(),
            "2026-03-06".to_string(),
            "2026-03-07".to_string(),
            "2026-03-08".to_string(),
            "2026-03-09".to_string(),
            "2026-03-10".to_string(),
        ],
        d("0.01"),
    );

    let validation_out = validate_records(
        &load_out.records,
        &ctx,
        &default_plan(),
        &ValidationRegistry::default(),
    )
    .expect("validation success");

    let extreme_issues = validation_out
        .issues
        .iter()
        .filter(|i| {
            i.date == "2026-03-04"
                && i.rule_name == "HighLowLogicRule"
                && (i.issue_type == IssueType::HighBelowOthers
                    || i.issue_type == IssueType::LowAboveOthers)
        })
        .count();
    assert!(extreme_issues >= 2);

    let cleaner = DefaultCleanerStage::new(
        RuleNamePolicyResolver,
        BuiltinPolicyExecutor,
        DefaultLoadErrorAuditMapper,
    );
    let err = cleaner
        .run(
            &load_out.records,
            &validation_out.issues,
            &load_out.load_errors,
            &HandlingConfig { policies: vec![] },
        )
        .expect_err("clean stage should fail by invariant violation");

    match err {
        CleanerError::InvariantViolation {
            rule_name,
            detail,
            original_row,
            ..
        } => {
            assert_eq!(rule_name, "PriceInvariant::HighGteOpen");
            assert!(detail.contains("high must be >= open"));
            assert!(original_row.contains("date=2026-03-04"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn synthetic_case_b_duplicate_rows_detected_but_not_deduplicated_yet() {
    let cfg = synthetic_load_config("case_b_duplicate_records.csv");
    let load_out = load_data(&cfg).expect("csv load success");
    assert_eq!(load_out.records.len(), 10);

    let ctx = ValidationContext::new(
        vec![
            "2026-03-01".to_string(),
            "2026-03-02".to_string(),
            "2026-03-03".to_string(),
            "2026-03-04".to_string(),
            "2026-03-05".to_string(),
            "2026-03-06".to_string(),
            "2026-03-07".to_string(),
            "2026-03-08".to_string(),
        ],
        d("0.01"),
    );

    let validation_out = validate_records(
        &load_out.records,
        &ctx,
        &default_plan(),
        &ValidationRegistry::default(),
    )
    .expect("validation success");

    let duplicate_issues = validation_out
        .issues
        .iter()
        .filter(|i| i.issue_type == IssueType::DuplicateDate && i.date == "2026-03-03")
        .count();
    assert_eq!(duplicate_issues, 2);

    let cleaner = DefaultCleanerStage::new(
        RuleNamePolicyResolver,
        BuiltinPolicyExecutor,
        DefaultLoadErrorAuditMapper,
    );
    let cleaner_out = cleaner
        .run(
            &load_out.records,
            &validation_out.issues,
            &load_out.load_errors,
            &HandlingConfig { policies: vec![] },
        )
        .expect("clean stage success");

    let same_day_rows = cleaner_out
        .cleaned_records
        .iter()
        .filter(|r| r.date == "2026-03-03")
        .collect::<Vec<_>>();

    // Current behavior: duplicates are flagged but still retained in cleaned output.
    assert_eq!(same_day_rows.len(), 3);
    assert_eq!(same_day_rows[2].close, d("10.35"));
}

#[test]
fn synthetic_case_c_missing_day_is_reported_but_not_auto_filled_yet() {
    let cfg = synthetic_load_config("case_c_halt_fill_gap.csv");
    let load_out = load_data(&cfg).expect("csv load success");
    assert_eq!(load_out.records.len(), 10);

    let ctx = ValidationContext::new(
        vec![
            "2026-03-08".to_string(),
            "2026-03-09".to_string(),
            "2026-03-10".to_string(),
            "2026-03-11".to_string(),
            "2026-03-12".to_string(),
            "2026-03-13".to_string(),
            "2026-03-14".to_string(),
            "2026-03-15".to_string(),
            "2026-03-16".to_string(),
            "2026-03-17".to_string(),
            "2026-03-18".to_string(),
        ],
        d("0.01"),
    );

    let validation_out = validate_records(
        &load_out.records,
        &ctx,
        &default_plan(),
        &ValidationRegistry::default(),
    )
    .expect("validation success");

    assert!(validation_out.issues.iter().any(|i| {
        i.issue_type == IssueType::MissingDates
            && i.rule_name == "MissingDatesRule"
            && i.date.contains("2026-03-11")
    }));

    let cleaner = DefaultCleanerStage::new(
        RuleNamePolicyResolver,
        BuiltinPolicyExecutor,
        DefaultLoadErrorAuditMapper,
    );
    let cleaner_out = cleaner
        .run(
            &load_out.records,
            &validation_out.issues,
            &load_out.load_errors,
            &HandlingConfig { policies: vec![] },
        )
        .expect("clean stage success");

    // Current behavior: missing trading days are audited unresolved, no synthetic halt row is inserted.
    assert!(cleaner_out.unresolved_issues >= 1);
    assert!(cleaner_out
        .cleaned_records
        .iter()
        .all(|r| r.date != "2026-03-11"));
}

#[test]
fn synthetic_case_d_ex_right_gap_has_no_adjustment_factor_pipeline_yet() {
    let cfg = synthetic_load_config("case_d_ex_right_gap.csv");
    let load_out = load_data(&cfg).expect("csv load success");
    assert_eq!(load_out.records.len(), 10);

    let ctx = ValidationContext::new(
        vec![
            "2026-02-25".to_string(),
            "2026-02-26".to_string(),
            "2026-02-27".to_string(),
            "2026-02-28".to_string(),
            "2026-03-01".to_string(),
            "2026-03-02".to_string(),
            "2026-03-03".to_string(),
            "2026-03-04".to_string(),
            "2026-03-05".to_string(),
            "2026-03-06".to_string(),
        ],
        d("0.01"),
    );

    let validation_out = validate_records(
        &load_out.records,
        &ctx,
        &default_plan(),
        &ValidationRegistry::default(),
    )
    .expect("validation success");

    // Current validator set does not include a corporate-action / adjustment-factor rule.
    assert!(validation_out
        .issues
        .iter()
        .all(|i| !i.rule_name.to_ascii_lowercase().contains("adjust")));

    let cleaner = DefaultCleanerStage::new(
        RuleNamePolicyResolver,
        BuiltinPolicyExecutor,
        DefaultLoadErrorAuditMapper,
    );
    let cleaner_out = cleaner
        .run(
            &load_out.records,
            &validation_out.issues,
            &load_out.load_errors,
            &HandlingConfig { policies: vec![] },
        )
        .expect("clean stage success");

    let event_day = cleaner_out
        .cleaned_records
        .iter()
        .find(|r| r.date == "2026-03-02")
        .expect("event day row exists");

    // If a 10-for-10 adjustment factor were applied backward, this close would not stay as raw 10.40.
    assert_eq!(event_day.close, d("10.40"));
}
