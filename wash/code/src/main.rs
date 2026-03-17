use std::collections::{HashMap, HashSet};
use std::env;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use rust_decimal::Decimal;
use wash_load::{
    load_and_validate_config, load_data, validate_records, AuditService, BasicReviewChartRenderer,
    BuiltinPolicyExecutor, CleanerStage, CommitArtifacts, DefaultAuditService, DefaultCleanerStage,
    DefaultLoadErrorAuditMapper, DefaultPerformanceSummaryBuilder, DefaultReviewStage,
    DefaultVersioningService, FileAuditLogWriter, FileDisabledIssueProvider, FileHistoryStore,
    FileReviewReportStore, ReviewChartConfig, ReviewChartType, ReviewConfig, ReviewPreviewConfig,
    ReviewStage, RuleNamePolicyResolver, RunMode, StaticRuleRegistry, ValidationContext,
    ValidationPlan, ValidationRegistry, VersionCommitInput, VersioningConfig, VersioningService,
};

#[derive(Debug, Clone)]
struct CliArgs {
    config: PathBuf,
    mode_override: Option<RunMode>,
    review_output_dir: PathBuf,
    output_dir: PathBuf,
    no_versioning: bool,
    author: String,
    message: Option<String>,
}

fn main() {
    if let Err(err) = run() {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let args = parse_args(env::args().collect::<Vec<_>>())?;

    let registry = pipeline_registry();
    let mut cfg = load_and_validate_config(&args.config, &registry)?;
    if let Some(mode) = args.mode_override {
        cfg.mode = mode;
    }

    let started_at = Instant::now();
    let load_out = load_data(&cfg)?;

    let validation_plan = ValidationPlan::from_rule_switch(&cfg.rules);
    let validation_ctx = build_validation_ctx(&cfg.calendar.trading_calendar_path, &cfg.market_rules.path)?;
    let validation_out = validate_records(
        &load_out.records,
        &validation_ctx,
        &validation_plan,
        &ValidationRegistry::default(),
    )?;

    let review_stage = DefaultReviewStage::new(
        FileDisabledIssueProvider::default(),
        BasicReviewChartRenderer,
        wash_load::RuleBasedPreviewEngine::default(),
        FileReviewReportStore::default(),
    );

    let review_out = review_stage.run(
        &validation_out.issues,
        &ReviewConfig {
            charts: ReviewChartConfig {
                enabled: true,
                types: HashSet::from([
                    ReviewChartType::IssueByDate,
                    ReviewChartType::IssueByCategory,
                    ReviewChartType::IssueByRule,
                ]),
            },
            preview: ReviewPreviewConfig {
                enabled: true,
                sample_size: 20,
            },
            output_dir: args.review_output_dir.clone(),
        },
    )?;

    println!(
        "review complete: total_issues={}, approved={}, disabled={}",
        validation_out.total_issues,
        review_out.approved_issues.len(),
        review_out.disabled_issues.len()
    );
    println!(
        "review artifacts: {}/{}",
        args.review_output_dir.display(),
        wash_load::REVIEW_REPORT_FILE
    );

    if cfg.mode == RunMode::ReviewOnly {
        return Ok(());
    }

    let cleaner = DefaultCleanerStage::new(
        RuleNamePolicyResolver,
        BuiltinPolicyExecutor,
        DefaultLoadErrorAuditMapper,
    );
    let cleaner_out = cleaner.run(
        &load_out.records,
        &review_out.approved_issues,
        &load_out.load_errors,
        &cfg.handling,
    )?;

    let cleaned_csv_path = args.output_dir.join("cleaned.csv");
    let audit_json_path = args.output_dir.join("audit_log.json");
    let audit_csv_path = args.output_dir.join("audit_log.csv");
    let summary_json_path = args.output_dir.join("summary.json");

    write_cleaned_csv(&cleaned_csv_path, &cleaner_out.cleaned_records)?;

    let rule_time_breakdown = validation_out
        .metrics
        .iter()
        .map(|m| (m.rule_name.clone(), m.elapsed.as_millis()))
        .collect::<HashMap<String, u128>>();

    let audit_service = DefaultAuditService::new(DefaultPerformanceSummaryBuilder, FileAuditLogWriter);
    let perf = audit_service.publish(
        &cleaner_out.audit_entries,
        wash_load::PerformanceSummaryInput {
            total_rows: load_out.records.len(),
            total_issues: validation_out.total_issues,
            disabled_issues: review_out.disabled_issues.len(),
            load_error_count: load_out.load_errors.len(),
            cleaner_output: &cleaner_out,
            total_time_ms: started_at.elapsed().as_millis(),
            rule_time_breakdown,
        },
        &audit_json_path,
        &audit_csv_path,
    )?;

    write_summary_json(&summary_json_path, &perf)?;

    println!("clean output: {}", cleaned_csv_path.display());
    println!("audit json: {}", audit_json_path.display());
    println!("audit csv: {}", audit_csv_path.display());
    println!("summary: {}", summary_json_path.display());

    if cfg.mode == RunMode::Full && !args.no_versioning {
        let versioning_cfg = VersioningConfig::default();
        let versioning = DefaultVersioningService::new(FileHistoryStore, wash_load::EpochCommitIdStrategy);
        let run_mode_label = run_mode_name(cfg.mode.clone()).to_string();

        let commit_input = VersionCommitInput {
            author: args.author,
            message: args
                .message
                .unwrap_or_else(|| format!("pipeline run ({})", run_mode_label)),
            run_mode: run_mode_label,
            artifacts: CommitArtifacts {
                config_yaml: args.config.clone(),
                cleaned_csv: Some(cleaned_csv_path.clone()),
                audit_log_json: Some(audit_json_path.clone()),
                audit_log_csv: Some(audit_csv_path.clone()),
                report_md: None,
                summary_json: summary_json_path.clone(),
            },
        };

        let commit_id = versioning.commit(&versioning_cfg, commit_input)?;
        println!("version snapshot commit: {commit_id}");
        println!("history head: {}/{}", versioning_cfg.history_dir.display(), versioning_cfg.head_file);
    }

    Ok(())
}

fn parse_args(argv: Vec<String>) -> Result<CliArgs, Box<dyn Error>> {
    if argv.len() == 1 {
        print_help();
        return Err("missing arguments".into());
    }

    let mut config = None;
    let mut mode_override = None;
    let mut review_output_dir = PathBuf::from("output/review");
    let mut output_dir = PathBuf::from("output/final");
    let mut no_versioning = false;
    let mut author = String::from("cli-user");
    let mut message = None;

    let mut i = 1usize;
    while i < argv.len() {
        match argv[i].as_str() {
            "-h" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            "--config" => {
                i += 1;
                let value = argv.get(i).ok_or("--config requires a path")?;
                config = Some(PathBuf::from(value));
            }
            "--mode" => {
                i += 1;
                let value = argv.get(i).ok_or("--mode requires a value")?;
                mode_override = Some(parse_mode(value)?);
            }
            "--review-only" => {
                mode_override = Some(RunMode::ReviewOnly);
            }
            "--review-output-dir" => {
                i += 1;
                let value = argv.get(i).ok_or("--review-output-dir requires a path")?;
                review_output_dir = PathBuf::from(value);
            }
            "--output-dir" => {
                i += 1;
                let value = argv.get(i).ok_or("--output-dir requires a path")?;
                output_dir = PathBuf::from(value);
            }
            "--no-versioning" => {
                no_versioning = true;
            }
            "--author" => {
                i += 1;
                let value = argv.get(i).ok_or("--author requires a value")?;
                author = value.clone();
            }
            "--message" => {
                i += 1;
                let value = argv.get(i).ok_or("--message requires a value")?;
                message = Some(value.clone());
            }
            other => {
                return Err(format!("unknown argument: {other}").into());
            }
        }
        i += 1;
    }

    let config = config.ok_or("--config is required")?;

    Ok(CliArgs {
        config,
        mode_override,
        review_output_dir,
        output_dir,
        no_versioning,
        author,
        message,
    })
}

fn parse_mode(raw: &str) -> Result<RunMode, Box<dyn Error>> {
    match raw {
        "review-only" => Ok(RunMode::ReviewOnly),
        "clean" => Ok(RunMode::Clean),
        "full" => Ok(RunMode::Full),
        _ => Err(format!("invalid mode: {raw}").into()),
    }
}

fn run_mode_name(mode: RunMode) -> &'static str {
    match mode {
        RunMode::ReviewOnly => "review-only",
        RunMode::Clean => "clean",
        RunMode::Full => "full",
    }
}

fn pipeline_registry() -> StaticRuleRegistry {
    StaticRuleRegistry::new(
        vec![
            "MissingDatesRule",
            "DuplicateDatesRule",
            "NonTradingDayRule",
            "HighLowLogicRule",
            "NegativePriceRule",
            "TickSizeRule",
            "VwapRangeRule",
        ],
        vec!["DataIntegrity", "IntraBarLogic"],
    )
}

fn build_validation_ctx(calendar_path: &Path, market_rules_path: &Path) -> Result<ValidationContext, Box<dyn Error>> {
    let trading_days = load_trading_days(calendar_path)?;
    let tick_size = load_tick_size(market_rules_path).unwrap_or_else(|| Decimal::new(1, 2));
    Ok(ValidationContext::new(trading_days, tick_size))
}

fn load_trading_days(path: &Path) -> Result<Vec<String>, Box<dyn Error>> {
    if !path.exists() {
        return Ok(Vec::new());
    }

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(path)?;
    let mut out = Vec::new();
    for row in reader.records() {
        let rec = row?;
        let exchange = rec.get(0).unwrap_or("").trim();
        let cal_date = rec.get(1).unwrap_or("").trim();
        let is_open = rec.get(2).unwrap_or("").trim();

        if exchange == "SSE" && is_open == "1" && !cal_date.is_empty() {
            out.push(cal_date.to_string());
        }
    }

    out.sort();

    Ok(out)
}

fn load_tick_size(path: &Path) -> Option<Decimal> {
    let raw = fs::read_to_string(path).ok()?;
    let yaml = serde_yaml::from_str::<serde_yaml::Value>(&raw).ok()?;

    // Accept both top-level tick_size and nested market_rules.tick_size.
    if let Some(v) = yaml.get("tick_size").and_then(parse_tick_size_value) {
        return Some(v);
    }

    yaml
        .get("market_rules")
        .and_then(|v| v.get("tick_size"))
        .and_then(parse_tick_size_value)
}

fn parse_tick_size_value(v: &serde_yaml::Value) -> Option<Decimal> {
    match v {
        serde_yaml::Value::String(s) => s.parse::<Decimal>().ok(),
        serde_yaml::Value::Number(n) => n.to_string().parse::<Decimal>().ok(),
        _ => None,
    }
}

fn write_cleaned_csv(path: &Path, records: &[wash_load::Record]) -> Result<(), Box<dyn Error>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut out = String::from("date,ticker,open,high,low,close,vwap,volume,turnover,status\n");
    for row in records {
        out.push_str(&format!(
            "{},{},{},{},{},{},{},{},{},{}\n",
            row.date,
            row.ticker,
            row.open,
            row.high,
            row.low,
            row.close,
            row.vwap,
            row.volume,
            row.turnover,
            trade_status_name(&row.status),
        ));
    }

    fs::write(path, out)?;
    Ok(())
}

fn write_summary_json(path: &Path, summary: &wash_load::PerformanceSummary) -> Result<(), Box<dyn Error>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let payload = format!(
        "{{\n  \"total_rows\": {},\n  \"total_issues\": {},\n  \"processed_issues\": {},\n  \"unresolved_issues\": {},\n  \"disabled_issues\": {},\n  \"load_error_count\": {},\n  \"total_time_ms\": {},\n  \"throughput_rows_per_sec\": {}\n}}\n",
        summary.total_rows,
        summary.total_issues,
        summary.processed_issues,
        summary.unresolved_issues,
        summary.disabled_issues,
        summary.load_error_count,
        summary.total_time_ms,
        summary.throughput_rows_per_sec,
    );

    fs::write(path, payload)?;
    Ok(())
}

fn trade_status_name(status: &wash_load::TradeStatus) -> String {
    match status {
        wash_load::TradeStatus::Normal => "NORMAL".to_string(),
        wash_load::TradeStatus::Halted => "HALTED".to_string(),
        wash_load::TradeStatus::Delisted => "DELISTED".to_string(),
        wash_load::TradeStatus::Other(v) => v.clone(),
    }
}

fn print_help() {
    println!(
        "cleaner\n\
Usage:\n\
  cleaner --config <path> [options]\n\
\n\
Options:\n\
  --config <path>             Path to YAML config (required)\n\
  --mode <review-only|clean|full>\n\
                              Override mode in YAML\n\
  --review-only               Shorthand for --mode review-only\n\
  --review-output-dir <path>  Review artifacts directory (default: output/review)\n\
  --output-dir <path>         Clean/audit output directory (default: output/final)\n\
  --no-versioning             Skip history snapshot in full mode\n\
  --author <name>             Commit author for snapshot (default: cli-user)\n\
  --message <text>            Commit message for snapshot\n\
  -h, --help                  Show this help\n"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_yaml::Value;

    #[test]
    fn parse_args_supports_review_only() {
        let args = parse_args(vec![
            "cleaner".to_string(),
            "--config".to_string(),
            "config.yaml".to_string(),
            "--review-only".to_string(),
        ])
        .expect("parse args");

        assert_eq!(args.config, PathBuf::from("config.yaml"));
        assert_eq!(args.mode_override, Some(RunMode::ReviewOnly));
    }

    #[test]
    fn parse_args_requires_config() {
        let err = parse_args(vec!["cleaner".to_string()]).expect_err("missing config should fail");
        assert!(err.to_string().contains("missing arguments"));
    }

    #[test]
    fn parse_tick_size_value_supports_string_and_number() {
        let as_string: Value = serde_yaml::from_str("tick_size: \"0.0001\"").expect("yaml");
        let as_number: Value = serde_yaml::from_str("tick_size: 0.0001").expect("yaml");

        let v1 = parse_tick_size_value(as_string.get("tick_size").expect("tick_size key"));
        let v2 = parse_tick_size_value(as_number.get("tick_size").expect("tick_size key"));

        assert_eq!(v1.expect("parse string"), "0.0001".parse::<Decimal>().expect("decimal"));
        assert_eq!(v2.expect("parse number"), "0.0001".parse::<Decimal>().expect("decimal"));
    }
}
