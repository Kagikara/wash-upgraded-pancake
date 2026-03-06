关键上下游已经收齐：现在可以把各模块关系从“文档描述”落成统一接口契约，并标出调用顺序、可选分支和失败恢复点。下一条我直接给你完整的模块关系图和接口定义清单。模块关系可以先看成一条主流水线 + 两条旁路（可选报告、恢复/版本）：

Config -> Loader -> Validator -> Review -> Cleaner -> Writer -> (LLM Report?) -> Versioning
并且全程有 Audit 贯穿记录，失败时走 Checkpoint Recovery。

1. 模块依赖关系（谁依赖谁）

Config 是所有模块的上游依赖
来源：input_validation_pseudocode_Version4.txt、config_Version2.yaml
Loader 依赖 config.input，输出标准 records 给 Validator/Cleaner
来源：data_loader_pseudocode_Version2.txt
Validator 依赖 records + Context，只产出 issues，不改数据
来源：data_validator_modular_pseudocode_Version2.txt、data_validator_modular_pseudocode_Version3.txt、data_validator_modular_pseudocode_Version4.txt
Review 依赖 issues + config.review，输出 approved_issues/disabled_issues/review_report
来源：review_stage_pseudocode_Version4.txt
Cleaner 依赖 records + approved_issues + config.handling，输出 cleaned_records + audit_entries
来源：clean_and_write_pseudocode_Version4.txt
Writer 依赖 cleaned_records + audit_entries + config.output，落盘清洗结果和审计日志
来源：clean_and_write_pseudocode_Version4.txt
Audit 依赖各阶段事件，形成 AuditEntry 和 PerformanceSummary
来源：audit_log_pseudocode_Version4.txt
LLM Report 依赖 audit_entries + performance_summary + audit_csv_sample，可选
来源：llm_report_pseudocode_Version4.txt
Versioning 依赖最终产物（config/cleaned/audit/report/summary）生成快照
来源：versioning_layout_Version4.txt
Failure Recovery 包裹整条流水线，每阶段写 checkpoint
来源：failure_recovery_pseudocode_Version4.txt
2. 核心接口（输入/输出）

load_and_validate_config(config_path) -> Config
职责：语法校验、schema 校验、默认值合并、规则与策略存在性校验、路径合法性检查。
load_data(input_path, format, schema) -> (records, load_errors)
职责：CSV/Parquet 读取与字段映射，解析失败留在 load_errors。
validate_records(records, context) -> issues
职责：规则注册表驱动执行，产出 Issue[]。
review_stage(issues, config) -> (approved_issues, disabled_issues, review_report)
职责：统计/图表/预览 + 用户禁用过滤（只能禁用，不能新增 issue）。
clean_records(records, approved_issues, config) -> (cleaned_records, audit_entries)
职责：按 policy 修复副本；无 policy 则 UNRESOLVED 并记审计。
write_outputs(cleaned_records, audit_entries, config)
职责：输出 cleaned 数据 + audit JSON/CSV。
build_performance_summary(...) -> PerformanceSummary
职责：统计吞吐、耗时、规则耗时、处理/未处理/禁用数量。
generate_llm_report(audit_entries, performance_summary, config) -> report_text | None
职责：可选报告生成。
versioning.commit(config, cleaned, audit_entries)
职责：快照化，支持追溯回滚。
run_pipeline_with_recovery(config)
职责：统一编排 + 失败 checkpoint。
3. 统一数据契约（建议你实现时固定）

Record
date, ticker, open, high, low, close, vwap, volume, turnover, status
Issue
type, category, rule_name(建议补齐), ticker, date, field, value, detail
AuditEntry
timestamp, ticker, date, issue_type, category, rule_name, field, old_value, new_value, action, action_source, comment
PerformanceSummary
total_rows, total_issues, processed_issues, unresolved_issues, disabled_issues, total_time_ms, throughput_rows_per_sec, rule_time_breakdown
Context
trading_calendar, corp_actions, lifecycle_map, market_rules, point_in_time_meta, tz_rules
4. 关键耦合点和边界

Validator -> Cleaner 的唯一桥梁是 Issue，所以 Issue 字段必须稳定，特别是 type/field/rule_name。
Review 只做过滤，不做“修复意见编辑”，保证流程简单和可追溯。
Cleaner 必须只改副本，不改源记录，这是可审计和可回滚前提。
Writer/LLM/Versioning 都基于 audit_entries + performance_summary，这两份结构是后处理核心。
Recovery 在每 stage 后 checkpoint，意味着每个 stage 输出都要可序列化。
5. 你这个设计里目前的一个接口缺口
load_errors 在 Loader 已产出，但后续主干接口没有明确传递它。
建议改成：
Loader -> (records, load_errors)，并把 load_errors 合入 audit_entries 或 summary，否则解析失败数据会“看得见定义、看不见落地”。

如果你要，我下一步可以直接给你一版“Rust 代码层面的 trait/interface 草案”（Rule, Policy, Stage, PipelineContext, StageResult），让你可以按这个关系直接开工。

