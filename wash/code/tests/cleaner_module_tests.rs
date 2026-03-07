use rust_decimal::Decimal;
use wash_load::{
    AuditActionSource, BuiltinPolicyExecutor, CleanerError, CleanerStage, DefaultCleanerStage,
    DefaultLoadErrorAuditMapper, HandlingConfig, Issue, IssueType, LoadError, LoadErrorCode,
    NoopPolicyExecutor, PolicyConfig, Record, RuleNamePolicyResolver, TradeStatus,
};

fn d(v: &str) -> Decimal {
    v.parse::<Decimal>().expect("valid decimal")
}

fn make_record(close: &str, vwap: &str) -> Record {
    Record {
        date: "2026-03-06".to_string(),
        ticker: "000001.SZ".to_string(),
        open: d("10.00"),
        high: d("10.20"),
        low: d("9.80"),
        close: d(close),
        vwap: d(vwap),
        volume: 100,
        turnover: 1000,
        status: TradeStatus::Normal,
    }
}

fn make_issue() -> Issue {
    Issue {
        issue_type: IssueType::NegativePrice,
        category: "IntraBarLogic".to_string(),
        rule_name: "NegativePriceRule".to_string(),
        ticker: "000001.SZ".to_string(),
        date: "2026-03-06".to_string(),
        field: "close".to_string(),
        value: "-1.00".to_string(),
        detail: "Negative price not allowed".to_string(),
    }
}

#[test]
fn cleaner_clones_records_and_merges_load_errors_into_audit() {
    let input_records = vec![make_record("-1.00", "10.00")];
    let issue = make_issue();
    let load_errors = vec![LoadError {
        stage: "LOAD",
        row_number: 2,
        raw_row: "raw".to_string(),
        error_code: LoadErrorCode::TypeCastFail,
        error_detail: "bad decimal".to_string(),
    }];

    let handling = HandlingConfig {
        policies: vec![PolicyConfig {
            rule_name: "NegativePriceRule".to_string(),
            action: "set_literal".to_string(),
            params: serde_yaml::from_str("value: '10.10'").expect("yaml"),
        }],
    };

    let cleaner = DefaultCleanerStage::new(
        RuleNamePolicyResolver,
        BuiltinPolicyExecutor,
        DefaultLoadErrorAuditMapper,
    );

    let out = cleaner
        .run(&input_records, &[issue], &load_errors, &handling)
        .expect("cleaning succeeds");

    assert_eq!(input_records[0].close, d("-1.00"));
    assert_eq!(out.cleaned_records[0].close, d("10.10"));
    assert_eq!(out.processed_issues, 1);

    assert_eq!(out.audit_entries.len(), 2);
    assert!(out
        .audit_entries
        .iter()
        .any(|a| a.action == "LOAD_ERROR" && a.action_source == AuditActionSource::Loader));
    assert!(out
        .audit_entries
        .iter()
        .any(|a| a.action == "set_literal" && a.field == "close"));
}

#[test]
fn cleaner_marks_issue_unresolved_when_policy_missing() {
    let input_records = vec![make_record("-1.00", "10.00")];
    let issue = make_issue();

    let handling = HandlingConfig { policies: vec![] };
    let cleaner = DefaultCleanerStage::new(
        RuleNamePolicyResolver,
        NoopPolicyExecutor,
        DefaultLoadErrorAuditMapper,
    );

    let out = cleaner
        .run(&input_records, &[issue], &[], &handling)
        .expect("cleaning succeeds");

    assert_eq!(out.processed_issues, 0);
    assert_eq!(out.unresolved_issues, 1);
    assert_eq!(out.cleaned_records[0].close, d("-1.00"));

    let unresolved = out
        .audit_entries
        .iter()
        .find(|a| a.action == "UNRESOLVED")
        .expect("has unresolved audit entry");
    assert_eq!(unresolved.action_source, AuditActionSource::Disabled);
}

#[test]
fn cleaner_supports_clamp_field_action() {
    let input_records = vec![make_record("10.00", "12.00")];
    let issue = Issue {
        issue_type: IssueType::VwapOutOfRange,
        category: "IntraBarLogic".to_string(),
        rule_name: "VwapRangeRule".to_string(),
        ticker: "000001.SZ".to_string(),
        date: "2026-03-06".to_string(),
        field: "vwap".to_string(),
        value: "12.00".to_string(),
        detail: "VWAP is outside [Low, High]".to_string(),
    };

    let handling = HandlingConfig {
        policies: vec![PolicyConfig {
            rule_name: "VwapRangeRule".to_string(),
            action: "clamp_field".to_string(),
            params: serde_yaml::from_str(
                "min_field: low
max_field: high",
            )
            .expect("yaml"),
        }],
    };

    let cleaner = DefaultCleanerStage::new(
        RuleNamePolicyResolver,
        BuiltinPolicyExecutor,
        DefaultLoadErrorAuditMapper,
    );

    let out = cleaner
        .run(&input_records, &[issue], &[], &handling)
        .expect("cleaning succeeds");

    assert_eq!(out.cleaned_records[0].vwap, d("10.20"));
    assert_eq!(out.processed_issues, 1);
}

#[test]
fn cleaner_returns_unknown_field_error() {
    let input_records = vec![make_record("-1.00", "10.00")];
    let issue = Issue {
        field: "unknown_field".to_string(),
        ..make_issue()
    };

    let handling = HandlingConfig {
        policies: vec![PolicyConfig {
            rule_name: "NegativePriceRule".to_string(),
            action: "set_literal".to_string(),
            params: serde_yaml::from_str("value: '10.10'").expect("yaml"),
        }],
    };

    let cleaner = DefaultCleanerStage::new(
        RuleNamePolicyResolver,
        BuiltinPolicyExecutor,
        DefaultLoadErrorAuditMapper,
    );

    let err = cleaner
        .run(&input_records, &[issue], &[], &handling)
        .expect_err("should fail with unknown field");

    match err {
        CleanerError::UnknownField(field) => assert_eq!(field, "unknown_field"),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn cleaner_returns_policy_error_on_unknown_action() {
    let input_records = vec![make_record("-1.00", "10.00")];
    let issue = make_issue();

    let handling = HandlingConfig {
        policies: vec![PolicyConfig {
            rule_name: "NegativePriceRule".to_string(),
            action: "unknown_action".to_string(),
            params: serde_yaml::from_str("{}").expect("yaml"),
        }],
    };

    let cleaner = DefaultCleanerStage::new(
        RuleNamePolicyResolver,
        BuiltinPolicyExecutor,
        DefaultLoadErrorAuditMapper,
    );

    let err = cleaner
        .run(&input_records, &[issue], &[], &handling)
        .expect_err("should fail on unknown action");

    match err {
        CleanerError::PolicyExecution { rule_name, .. } => {
            assert_eq!(rule_name, "NegativePriceRule")
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn cleaner_returns_policy_error_on_invalid_params() {
    let input_records = vec![make_record("-1.00", "10.00")];
    let issue = make_issue();

    let handling = HandlingConfig {
        policies: vec![PolicyConfig {
            rule_name: "NegativePriceRule".to_string(),
            action: "set_literal".to_string(),
            params: serde_yaml::from_str("{}").expect("yaml"),
        }],
    };

    let cleaner = DefaultCleanerStage::new(
        RuleNamePolicyResolver,
        BuiltinPolicyExecutor,
        DefaultLoadErrorAuditMapper,
    );

    let err = cleaner
        .run(&input_records, &[issue], &[], &handling)
        .expect_err("should fail on missing params.value");

    match err {
        CleanerError::PolicyExecution { rule_name, .. } => {
            assert_eq!(rule_name, "NegativePriceRule")
        }
        other => panic!("unexpected error: {other:?}"),
    }
}
