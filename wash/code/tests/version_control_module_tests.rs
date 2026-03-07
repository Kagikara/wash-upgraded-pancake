use std::fs;
use std::path::{Path, PathBuf};

use tempfile::tempdir;
use wash_load::{
    CheckpointRecord, CheckpointStore, CommitArtifacts, DefaultRecoveryService,
    DefaultVersioningService, EpochCommitIdStrategy, FileCheckpointStore, FileHistoryStore,
    PipelineStage, RecoveryService, VersionCommitInput, VersioningConfig, VersioningService,
};

fn write_file(path: &Path, content: &str) {
    fs::write(path, content).expect("write file");
}

fn build_config(base: &Path) -> VersioningConfig {
    VersioningConfig {
        history_dir: base.join(".history"),
        head_file: "HEAD".to_string(),
        commits_dir: "commits".to_string(),
        checkpoint_dir: base.join(".checkpoint"),
    }
}

fn prepare_artifacts(base: &Path, with_report: bool) -> CommitArtifacts {
    let config_yaml = base.join("config.yaml");
    let cleaned_csv = base.join("cleaned.csv");
    let audit_json = base.join("audit_log.json");
    let audit_csv = base.join("audit_log.csv");
    let summary = base.join("summary.json");
    let report = base.join("report.md");

    write_file(&config_yaml, "mode: full\n");
    write_file(&cleaned_csv, "date,ticker\n2026-03-06,000001.SZ\n");
    write_file(&audit_json, "{\"audit_entries\":[]}\n");
    write_file(&audit_csv, "ticker,action\n000001.SZ,FIXED\n");
    write_file(&summary, "{\"total_rows\":1}\n");
    if with_report {
        write_file(&report, "# report\n");
    }

    CommitArtifacts {
        config_yaml,
        cleaned_csv: Some(cleaned_csv),
        audit_log_json: Some(audit_json),
        audit_log_csv: Some(audit_csv),
        report_md: if with_report { Some(report) } else { None },
        summary_json: summary,
    }
}

#[test]
fn commit_success_writes_layout_and_head() {
    let dir = tempdir().expect("tmp dir");
    let cfg = build_config(dir.path());
    let artifacts = prepare_artifacts(dir.path(), true);

    let svc = DefaultVersioningService::new(FileHistoryStore, EpochCommitIdStrategy);
    let commit_id = svc
        .commit(
            &cfg,
            VersionCommitInput {
                author: "tester".to_string(),
                message: "snapshot for full run".to_string(),
                run_mode: "full".to_string(),
                artifacts,
            },
        )
        .expect("commit success");

    let head = fs::read_to_string(cfg.history_dir.join("HEAD")).expect("head should exist");
    assert_eq!(head.trim(), commit_id);

    let commit_dir = cfg.history_dir.join("commits").join(&commit_id);
    assert!(commit_dir.join("meta.json").exists());
    assert!(commit_dir.join("config.yaml").exists());
    assert!(commit_dir.join("cleaned.csv").exists());
    assert!(commit_dir.join("audit_log.json").exists());
    assert!(commit_dir.join("audit_log.csv").exists());
    assert!(commit_dir.join("report.md").exists());
    assert!(commit_dir.join("summary.json").exists());
}

#[test]
fn commit_missing_required_artifact_fails_and_head_not_written() {
    let dir = tempdir().expect("tmp dir");
    let cfg = build_config(dir.path());
    let mut artifacts = prepare_artifacts(dir.path(), false);
    artifacts.summary_json = PathBuf::from(dir.path().join("missing_summary.json"));

    let svc = DefaultVersioningService::new(FileHistoryStore, EpochCommitIdStrategy);
    let result = svc.commit(
        &cfg,
        VersionCommitInput {
            author: "tester".to_string(),
            message: "broken snapshot".to_string(),
            run_mode: "full".to_string(),
            artifacts,
        },
    );

    assert!(result.is_err());
    assert!(!cfg.history_dir.join("HEAD").exists());
}

#[test]
fn rollback_to_existing_commit_updates_head() {
    let dir = tempdir().expect("tmp dir");
    let cfg = build_config(dir.path());

    let svc = DefaultVersioningService::new(FileHistoryStore, EpochCommitIdStrategy);

    let first_id = svc
        .commit(
            &cfg,
            VersionCommitInput {
                author: "tester".to_string(),
                message: "first".to_string(),
                run_mode: "full".to_string(),
                artifacts: prepare_artifacts(dir.path(), false),
            },
        )
        .expect("first commit");

    let second_id = svc
        .commit(
            &cfg,
            VersionCommitInput {
                author: "tester".to_string(),
                message: "second".to_string(),
                run_mode: "full".to_string(),
                artifacts: prepare_artifacts(dir.path(), false),
            },
        )
        .expect("second commit");
    assert_ne!(first_id, second_id);

    svc.rollback(&cfg, &first_id).expect("rollback success");
    let current = svc.current_head(&cfg).expect("head read").expect("head exists");
    assert_eq!(current, first_id);
}

#[test]
fn rollback_to_missing_commit_returns_error() {
    let dir = tempdir().expect("tmp dir");
    let cfg = build_config(dir.path());
    let svc = DefaultVersioningService::new(FileHistoryStore, EpochCommitIdStrategy);

    let err = svc.rollback(&cfg, "does-not-exist");
    assert!(err.is_err());
}

#[test]
fn checkpoint_and_recovery_plan_success() {
    let dir = tempdir().expect("tmp dir");
    let cfg = build_config(dir.path());
    let store = FileCheckpointStore;

    store
        .save(&cfg, "run-1", PipelineStage::Load, b"load-ok", None)
        .expect("save load");
    store
        .save(&cfg, "run-1", PipelineStage::Validate, b"validate-ok", None)
        .expect("save validate");

    let latest = store
        .latest(&cfg, "run-1")
        .expect("latest ok")
        .expect("latest exists");
    assert_eq!(latest.stage, PipelineStage::Validate);

    let payload = store.load_payload(&latest).expect("payload read");
    assert_eq!(payload, b"validate-ok");

    let recovery = DefaultRecoveryService::new(FileCheckpointStore);
    let plan = recovery
        .plan_resume(&cfg, "run-1")
        .expect("plan ok")
        .expect("plan exists");
    assert_eq!(plan.resume_from, PipelineStage::Review);
}

#[test]
fn checkpoint_clear_run_and_payload_error_paths() {
    let dir = tempdir().expect("tmp dir");
    let cfg = build_config(dir.path());
    let store = FileCheckpointStore;

    store
        .save(&cfg, "run-2", PipelineStage::Load, b"payload", None)
        .expect("save load");
    store.clear_run(&cfg, "run-2").expect("clear run");

    let recovery = DefaultRecoveryService::new(FileCheckpointStore);
    let plan = recovery.plan_resume(&cfg, "run-2").expect("plan ok");
    assert!(plan.is_none());

    let fake_record = CheckpointRecord {
        run_id: "run-404".to_string(),
        stage: PipelineStage::Load,
        created_at_epoch_ms: 0,
        payload_path: cfg.checkpoint_dir.join("run-404/load.payload"),
        error_message: None,
    };
    assert!(store.load_payload(&fake_record).is_err());
}
