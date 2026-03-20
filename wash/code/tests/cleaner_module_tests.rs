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
        volume: d("100"),
        turnover: d("1000"),
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
    let input_records = vec![make_record("10.00", "10.00")];
    let issue = Issue {
        issue_type: IssueType::NegativePrice,
        category: "IntraBarLogic".to_string(),
        rule_name: "MissingPolicyRule".to_string(),
        ticker: "000001.SZ".to_string(),
        date: "2026-03-06".to_string(),
        field: "close".to_string(),
        value: "-1.00".to_string(),
        detail: "Issue is intentionally left unresolved".to_string(),
    };

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
    assert_eq!(out.cleaned_records[0].close, d("10.00"));

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

#[test]
fn cleaner_marks_unmatched_issue_as_unresolved_with_audit() {
    let input_records = vec![make_record("10.00", "10.00")];
    let missing_dates_issue = Issue {
        issue_type: IssueType::MissingDates,
        category: "DataIntegrity".to_string(),
        rule_name: "MissingDatesRule".to_string(),
        ticker: "000001.SZ".to_string(),
        date: "2026-03-07|2026-03-10".to_string(),
        field: "date".to_string(),
        value: "gap".to_string(),
        detail: "Trading days missing between records".to_string(),
    };

    let cleaner = DefaultCleanerStage::new(
        RuleNamePolicyResolver,
        NoopPolicyExecutor,
        DefaultLoadErrorAuditMapper,
    );

    let out = cleaner
        .run(&input_records, &[missing_dates_issue], &[], &HandlingConfig { policies: vec![] })
        .expect("cleaning succeeds");

    assert_eq!(out.processed_issues, 0);
    assert_eq!(out.unresolved_issues, 1);

    let unresolved = out
        .audit_entries
        .iter()
        .find(|a| a.action == "UNRESOLVED" && a.rule_name == "MissingDatesRule")
        .expect("must keep unmatched issue in audit");
    assert_eq!(unresolved.ticker, "000001.SZ");
    assert_eq!(unresolved.date, "2026-03-07|2026-03-10");
}

#[test]
fn cleaner_mixed_matched_and_unmatched_issues_are_both_accounted_for() {
    let input_records = vec![make_record("-1.00", "10.00")];
    let matched_issue = make_issue();
    let unmatched_issue = Issue {
        issue_type: IssueType::MissingDates,
        category: "DataIntegrity".to_string(),
        rule_name: "MissingDatesRule".to_string(),
        ticker: "000001.SZ".to_string(),
        date: "2026-03-07|2026-03-10".to_string(),
        field: "date".to_string(),
        value: "gap".to_string(),
        detail: "Trading days missing between records".to_string(),
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

    let out = cleaner
        .run(
            &input_records,
            &[matched_issue, unmatched_issue],
            &[],
            &handling,
        )
        .expect("cleaning succeeds");

    assert_eq!(out.cleaned_records[0].close, d("10.10"));
    assert_eq!(out.processed_issues, 1);
    assert_eq!(out.unresolved_issues, 1);
    assert!(out
        .audit_entries
        .iter()
        .any(|a| a.action == "set_literal" && a.rule_name == "NegativePriceRule"));
    assert!(out
        .audit_entries
        .iter()
        .any(|a| a.action == "UNRESOLVED" && a.rule_name == "MissingDatesRule"));
}

#[test]
fn cleaner_fails_fast_when_cleaned_row_breaks_open_positive_invariant() {
    let input_records = vec![make_record("10.00", "10.00")];
    let issue = Issue {
        issue_type: IssueType::NegativePrice,
        category: "IntraBarLogic".to_string(),
        rule_name: "ForceOpenZeroRule".to_string(),
        ticker: "000001.SZ".to_string(),
        date: "2026-03-06".to_string(),
        field: "open".to_string(),
        value: "10.00".to_string(),
        detail: "force open to zero for test".to_string(),
    };

    let handling = HandlingConfig {
        policies: vec![PolicyConfig {
            rule_name: "ForceOpenZeroRule".to_string(),
            action: "set_literal".to_string(),
            params: serde_yaml::from_str("value: '0'").expect("yaml"),
        }],
    };

    let cleaner = DefaultCleanerStage::new(
        RuleNamePolicyResolver,
        BuiltinPolicyExecutor,
        DefaultLoadErrorAuditMapper,
    );

    let err = cleaner
        .run(&input_records, &[issue], &[], &handling)
        .expect_err("should fail on invariant violation");

    match err {
        CleanerError::InvariantViolation {
            rule_name,
            detail,
            original_row,
            cleaned_row,
        } => {
            assert_eq!(rule_name, "PriceInvariant::LowLteOpen");
            assert!(detail.contains("low must be <= open"));
            assert!(original_row.contains("open=10.00"));
            assert!(cleaned_row.contains("open=0"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn cleaner_fails_on_amount_positive_but_volume_non_positive() {
    let mut bad_record = make_record("10.00", "10.00");
    bad_record.volume = d("0");
    bad_record.turnover = d("1000");

    let cleaner = DefaultCleanerStage::new(
        RuleNamePolicyResolver,
        BuiltinPolicyExecutor,
        DefaultLoadErrorAuditMapper,
    );

    let err = cleaner
        .run(&[bad_record], &[], &[], &HandlingConfig { policies: vec![] })
        .expect_err("should fail on amount-volume invariant");

    match err {
        CleanerError::InvariantViolation {
            rule_name,
            detail,
            ..
        } => {
            assert_eq!(rule_name, "VolumeAmount::AmountImpliesVolume");
            assert!(detail.contains("volume must be > 0 when amount(turnover) > 0"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}
