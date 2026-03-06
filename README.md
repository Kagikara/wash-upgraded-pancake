# wash-upgraded-pancake
*！给量化人提供最真实的可用于策略的数据！*

L1和L2股票数据清洗程序，提供用户客制化清洗需求选择。

支持配置.YAML文件记录问题处理模块扩展，提高可处理问题的广泛性。用户通过自由配置“需要解决什么问题”和“怎么解决问题”，搭配出策略最适的数据清洗方式。

# L1数据清洗程序全流程说明（LCI 配置驱动）

> 目标：以 **YAML 配置驱动 + 模块化流水线** 的方式完成数据审查、人工核验、清洗、输出、审计与版本控制。  
> 原则：**审查与清洗解耦、可扩展、可回滚、可审计**。

---

## 0. CLI 调用方式（LCI）
```bash
cleaner --config config.yaml --review-only
```

---

## 1. 配置加载（LCI Config）
**输入：** `config.yaml`  
**输出：** `Config` 对象（合并默认值）

- 校验并加载 YAML
- 自动补全默认值（calendar / market_rules）
- corporate_actions / lifecycle_map 可选
- 解析 rules / handling / review / output 等配置

---

## 2. 读取数据（Loader）
**输入：** 原始数据文件  
**输出：** `records`, `load_errors`

- 支持 CSV / Parquet
- 使用 schema 映射字段
- 解析错误写入 `load_errors`（供审计）

---

## 3. 审查/校验（Validator）
**输入：** `records`, `Context`  
**输出：** `issues`

- 规则插件化（Rule 接口 + 注册表）
- enabled_categories / enabled_rules / disabled_rules 控制启用范围
- 仅产生 Issue，不修改数据

---

## 4. 人工核验阶段（Review Stage）
**输入：** `issues`, `config.review`  
**输出：** `approved_issues`, `disabled_issues`, `review_report`

- 统计汇总（按日期/类别/规则分布）
- 生成可视化图表
- 生成修改预览（不真正修改）
- 用户仅允许**禁用问题**（不可新增问题）
- 结果保存到 `output/review`

---

## 5. 清洗处理（Cleaner / Policy Engine）
**输入：** `records`, `approved_issues`, `config.handling`  
**输出：** `cleaned_records`, `audit_entries`

- 根据 YAML 或默认策略修复
- 不覆盖原始记录，生成新副本
- 每次修改写入审计条目（AuditEntry）

---

## 6. 输出层（Writer）
**输入：** `cleaned_records`, `audit_entries`  
**输出：** 新数据文件 + Audit Log

- 清洗后数据写入新路径（不覆盖源文件）
- Audit Log 输出 **JSON + CSV**
  - JSON：完整结构
  - CSV：便于 LLM 分析 / 节省 token

---

## 7. 审计日志（Audit Log）
**记录内容：**
- Issue 类型 / 规则 / 类别
- 修改前后值
- 动作来源（AUTO / MANUAL / DISABLED）
- 运行性能指标（耗时 / 吞吐量 / 规则耗时分布）

---

## 8. LLM 报告（可选）
**输入：** Audit Log + 汇总统计  
**输出：** `report.md`

- 由 `llm_report.enabled` 控制
- 生成清洗报告：问题概况、处理统计、风险提示

---

## 9. 版本控制（类 Git）
**目标：** 可追溯 + 可回滚

- 每次运行生成 commit 快照
- 快照包含：
  - config.yaml
  - cleaned.csv
  - audit_log.json / csv
  - report.md（可选）
  - summary.json
- 使用 `.history/HEAD` 指向当前版本
- 支持历史回滚与差异对比

---

# 全流程数据流图

```
LCI Config
   ↓
Loader
   ↓
Validator
   ↓
Review Stage（人工核验）
   ↓
Cleaner
   ↓
Writer
   ↓
Audit Log + Performance
   ↓
LLM Report（可选）
   ↓
Versioning Snapshot
```

---

# 总结
该流程实现：
- **规则扩展性**（插件化）
- **审查/清洗解耦**
- **人工核验可控**
- **可审计、可回滚**
- **可选 LLM 报告**

后续仅需补充：具体规则与处理策略即可投入使用。