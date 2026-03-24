use super::*;

pub trait ValidationRule: Send + Sync {
    fn name(&self) -> &'static str;
    fn category(&self) -> &'static str;
    fn metadata(&self) -> RuleMetadata;
    fn validate(
        &self,
        records: &[Record],
        ctx: &ValidationContext,
        plan: &ValidationPlan,
    ) -> Result<Vec<Issue>, ValidationError>;
}

pub trait ValidationRulePack: Send + Sync {
    fn pack_name(&self) -> &'static str;
    fn rules(&self) -> Vec<Box<dyn ValidationRule>>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CoreRulePack;

impl ValidationRulePack for CoreRulePack {
    fn pack_name(&self) -> &'static str {
        "core"
    }

    fn rules(&self) -> Vec<Box<dyn ValidationRule>> {
        vec![
            Box::new(MissingDatesRule),
            Box::new(DuplicateDatesRule),
            Box::new(NonTradingDayRule),
            Box::new(HighLowLogicRule),
            Box::new(NegativePriceRule),
            Box::new(TickSizeRule),
            Box::new(VwapRangeRule),
        ]
    }
}

#[derive(Debug, Error)]
pub enum RegistryComposeError {
    #[error("duplicate rule name while composing registry: {0}")]
    DuplicateRuleName(String),
}

#[derive(Default)]
pub struct ValidationRegistryBuilder {
    packs: Vec<Box<dyn ValidationRulePack>>,
}

impl ValidationRegistryBuilder {
    pub fn new() -> Self {
        Self { packs: Vec::new() }
    }

    pub fn with_core_rules(mut self) -> Self {
        self.packs.push(Box::new(CoreRulePack));
        self
    }

    pub fn add_pack(mut self, pack: Box<dyn ValidationRulePack>) -> Self {
        self.packs.push(pack);
        self
    }

    pub fn build(self) -> Result<ValidationRegistry, RegistryComposeError> {
        let mut rules = Vec::<Box<dyn ValidationRule>>::new();
        let mut seen_names = HashSet::<String>::new();

        for pack in self.packs {
            for rule in pack.rules() {
                let name = rule.name().to_string();
                if !seen_names.insert(name.clone()) {
                    return Err(RegistryComposeError::DuplicateRuleName(name));
                }
                rules.push(rule);
            }
        }

        Ok(ValidationRegistry { rules })
    }
}

pub struct ValidationRegistry {
    rules: Vec<Box<dyn ValidationRule>>,
}

impl ValidationRegistry {
    pub fn builder() -> ValidationRegistryBuilder {
        ValidationRegistryBuilder::new()
    }

    pub fn with_packs(packs: Vec<Box<dyn ValidationRulePack>>) -> Result<Self, RegistryComposeError> {
        let mut builder = ValidationRegistry::builder();
        for pack in packs {
            builder = builder.add_pack(pack);
        }
        builder.build()
    }

    pub fn default() -> Self {
        ValidationRegistry::builder()
            .with_core_rules()
            .build()
            .expect("core validation registry composition must succeed")
    }

    fn all_rule_names(&self) -> HashSet<String> {
        self.rules.iter().map(|r| r.name().to_string()).collect()
    }

    fn all_categories(&self) -> HashSet<String> {
        self.rules.iter().map(|r| r.category().to_string()).collect()
    }

    pub fn metadata_catalog(&self) -> Vec<RuleMetadata> {
        let mut out = self.rules.iter().map(|r| r.metadata()).collect::<Vec<_>>();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    fn select_rules(&self, plan: &ValidationPlan) -> Result<Vec<&dyn ValidationRule>, ValidationError> {
        let all_categories = self.all_categories();
        for category in &plan.enabled_categories {
            if !all_categories.contains(category) {
                return Err(ValidationError::UnknownCategory(category.clone()));
            }
        }

        let all_rule_names = self.all_rule_names();
        for rule in &plan.enabled_rules {
            if !all_rule_names.contains(rule) {
                return Err(ValidationError::UnknownRule(rule.clone()));
            }
        }

        for rule in &plan.disabled_rules {
            if !all_rule_names.contains(rule) {
                return Err(ValidationError::UnknownRule(rule.clone()));
            }
        }

        let use_enabled_rules = !plan.enabled_rules.is_empty();

        let selected = self
            .rules
            .iter()
            .filter(|rule| {
                if plan.disabled_rules.contains(rule.name()) {
                    return false;
                }

                if use_enabled_rules {
                    return plan.enabled_rules.contains(rule.name());
                }

                if !plan.enabled_categories.is_empty() {
                    return plan.enabled_categories.contains(rule.category());
                }

                true
            })
            .map(|rule| rule.as_ref())
            .collect();

        Ok(selected)
    }
}

#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("unknown category: {0}")]
    UnknownCategory(String),
    #[error("unknown rule: {0}")]
    UnknownRule(String),
    #[error("rule execution failed: {rule_name}: {detail}")]
    RuleExecution { rule_name: String, detail: String },
}

pub fn validate_records(
    records: &[Record],
    ctx: &ValidationContext,
    plan: &ValidationPlan,
    registry: &ValidationRegistry,
) -> Result<ValidationOutput, ValidationError> {
    // Resolve actual executable rule set from user switches.
    let selected_rules = registry.select_rules(plan)?;

    let mut issues = Vec::new();
    let mut metrics = Vec::new();
    let mut rule_metadata = Vec::new();

    for rule in selected_rules {
        // Measure each rule to feed performance / hot-spot diagnostics.
        let meta = rule.metadata();
        let start = Instant::now();
        let mut rule_issues = rule.validate(records, ctx, plan)?;
        let elapsed = start.elapsed();
        let issue_count = rule_issues.len();

        issues.append(&mut rule_issues);
        rule_metadata.push(meta.clone());
        metrics.push(RuleMetric {
            rule_name: meta.name,
            category: meta.category,
            elapsed,
            issue_count,
        });
    }

    rule_metadata.sort_by(|a, b| a.name.cmp(&b.name));

    issues.sort_by(|a, b| {
        (&a.ticker, &a.date, &a.rule_name, &a.field, &a.detail).cmp(&(
            &b.ticker,
            &b.date,
            &b.rule_name,
            &b.field,
            &b.detail,
        ))
    });

    Ok(ValidationOutput {
        total_issues: issues.len(),
        issues,
        metrics,
        rule_metadata,
    })
}

struct MissingDatesRule;

impl ValidationRule for MissingDatesRule {
    fn name(&self) -> &'static str {
        "MissingDatesRule"
    }

    fn category(&self) -> &'static str {
        "DataIntegrity"
    }

    fn metadata(&self) -> RuleMetadata {
        RuleMetadata {
            name: self.name().to_string(),
            category: self.category().to_string(),
            required_fields: vec!["ticker".to_string(), "date".to_string()],
            default_severity: RuleSeverity::High,
            configurable_thresholds: Vec::new(),
        }
    }

    fn validate(
        &self,
        records: &[Record],
        ctx: &ValidationContext,
        _plan: &ValidationPlan,
    ) -> Result<Vec<Issue>, ValidationError> {
        let mut grouped: HashMap<&str, Vec<&Record>> = HashMap::new();
        for row in records {
            grouped.entry(&row.ticker).or_default().push(row);
        }

        let mut issues = Vec::new();
        for (ticker, mut rows) in grouped {
            rows.sort_by(|a, b| a.date.cmp(&b.date));
            for pair in rows.windows(2) {
                let prev = pair[0];
                let cur = pair[1];
                let missing_days = ctx.missing_days_between(&prev.date, &cur.date);
                if !missing_days.is_empty() {
                    issues.push(Issue {
                        issue_type: IssueType::MissingDates,
                        category: self.category().to_string(),
                        rule_name: self.name().to_string(),
                        ticker: ticker.to_string(),
                        date: missing_days.join("|"),
                        field: "date".to_string(),
                        value: "gap".to_string(),
                        detail: "Trading days missing between records".to_string(),
                    });
                }
            }
        }

        Ok(issues)
    }
}

struct DuplicateDatesRule;

impl ValidationRule for DuplicateDatesRule {
    fn name(&self) -> &'static str {
        "DuplicateDatesRule"
    }

    fn category(&self) -> &'static str {
        "DataIntegrity"
    }

    fn metadata(&self) -> RuleMetadata {
        RuleMetadata {
            name: self.name().to_string(),
            category: self.category().to_string(),
            required_fields: vec!["ticker".to_string(), "date".to_string()],
            default_severity: RuleSeverity::High,
            configurable_thresholds: Vec::new(),
        }
    }

    fn validate(
        &self,
        records: &[Record],
        _ctx: &ValidationContext,
        _plan: &ValidationPlan,
    ) -> Result<Vec<Issue>, ValidationError> {
        let mut seen = HashSet::new();
        let mut issues = Vec::new();

        for row in records {
            let key = (row.ticker.as_str(), row.date.as_str());
            if seen.contains(&key) {
                issues.push(Issue {
                    issue_type: IssueType::DuplicateDate,
                    category: self.category().to_string(),
                    rule_name: self.name().to_string(),
                    ticker: row.ticker.clone(),
                    date: row.date.clone(),
                    field: "date".to_string(),
                    value: row.date.clone(),
                    detail: "Multiple rows for same ticker & date".to_string(),
                });
            } else {
                seen.insert(key);
            }
        }

        Ok(issues)
    }
}

struct NonTradingDayRule;

impl ValidationRule for NonTradingDayRule {
    fn name(&self) -> &'static str {
        "NonTradingDayRule"
    }

    fn category(&self) -> &'static str {
        "DataIntegrity"
    }

    fn metadata(&self) -> RuleMetadata {
        RuleMetadata {
            name: self.name().to_string(),
            category: self.category().to_string(),
            required_fields: vec!["date".to_string()],
            default_severity: RuleSeverity::Medium,
            configurable_thresholds: Vec::new(),
        }
    }

    fn validate(
        &self,
        records: &[Record],
        ctx: &ValidationContext,
        _plan: &ValidationPlan,
    ) -> Result<Vec<Issue>, ValidationError> {
        let mut issues = Vec::new();
        for row in records {
            if !ctx.is_trading_day(&row.date) {
                issues.push(Issue {
                    issue_type: IssueType::NonTradingDayData,
                    category: self.category().to_string(),
                    rule_name: self.name().to_string(),
                    ticker: row.ticker.clone(),
                    date: row.date.clone(),
                    field: "date".to_string(),
                    value: row.date.clone(),
                    detail: "Data exists on non-trading day".to_string(),
                });
            }
        }
        Ok(issues)
    }
}

struct HighLowLogicRule;

impl ValidationRule for HighLowLogicRule {
    fn name(&self) -> &'static str {
        "HighLowLogicRule"
    }

    fn category(&self) -> &'static str {
        "IntraBarLogic"
    }

    fn metadata(&self) -> RuleMetadata {
        RuleMetadata {
            name: self.name().to_string(),
            category: self.category().to_string(),
            required_fields: vec![
                "open".to_string(),
                "high".to_string(),
                "low".to_string(),
                "close".to_string(),
            ],
            default_severity: RuleSeverity::High,
            configurable_thresholds: vec![RuleThresholdMetadata {
                key: "epsilon".to_string(),
                description: "Tolerance applied to high/low ordering checks".to_string(),
                default_value: "0".to_string(),
            }],
        }
    }

    fn validate(
        &self,
        records: &[Record],
        _ctx: &ValidationContext,
        plan: &ValidationPlan,
    ) -> Result<Vec<Issue>, ValidationError> {
        let mut issues = Vec::new();
        let epsilon = plan.threshold_or_default(self.name(), "epsilon", Decimal::ZERO);

        for row in records {
            let max_other = row.open.max(row.close).max(row.low);
            if row.high + epsilon < max_other {
                issues.push(Issue {
                    issue_type: IssueType::HighBelowOthers,
                    category: self.category().to_string(),
                    rule_name: self.name().to_string(),
                    ticker: row.ticker.clone(),
                    date: row.date.clone(),
                    field: "high".to_string(),
                    value: row.high.to_string(),
                    detail: "High is below Open/Close/Low".to_string(),
                });
            }

            let min_other = row.open.min(row.close).min(row.high);
            if row.low - epsilon > min_other {
                issues.push(Issue {
                    issue_type: IssueType::LowAboveOthers,
                    category: self.category().to_string(),
                    rule_name: self.name().to_string(),
                    ticker: row.ticker.clone(),
                    date: row.date.clone(),
                    field: "low".to_string(),
                    value: row.low.to_string(),
                    detail: "Low is above Open/Close/High".to_string(),
                });
            }
        }

        Ok(issues)
    }
}

struct NegativePriceRule;

impl ValidationRule for NegativePriceRule {
    fn name(&self) -> &'static str {
        "NegativePriceRule"
    }

    fn category(&self) -> &'static str {
        "IntraBarLogic"
    }

    fn metadata(&self) -> RuleMetadata {
        RuleMetadata {
            name: self.name().to_string(),
            category: self.category().to_string(),
            required_fields: vec![
                "open".to_string(),
                "high".to_string(),
                "low".to_string(),
                "close".to_string(),
                "vwap".to_string(),
            ],
            default_severity: RuleSeverity::Critical,
            configurable_thresholds: vec![RuleThresholdMetadata {
                key: "min_allowed_price".to_string(),
                description: "Minimum allowed price floor".to_string(),
                default_value: "0".to_string(),
            }],
        }
    }

    fn validate(
        &self,
        records: &[Record],
        _ctx: &ValidationContext,
        plan: &ValidationPlan,
    ) -> Result<Vec<Issue>, ValidationError> {
        let mut issues = Vec::new();
        let floor = plan.threshold_or_default(self.name(), "min_allowed_price", Decimal::ZERO);

        for row in records {
            if row.open < floor
                || row.high < floor
                || row.low < floor
                || row.close < floor
                || row.vwap < floor
            {
                issues.push(Issue {
                    issue_type: IssueType::NegativePrice,
                    category: self.category().to_string(),
                    rule_name: self.name().to_string(),
                    ticker: row.ticker.clone(),
                    date: row.date.clone(),
                    field: "price".to_string(),
                    value: format!(
                        "open={},high={},low={},close={},vwap={}",
                        row.open, row.high, row.low, row.close, row.vwap
                    ),
                    detail: "Negative price not allowed".to_string(),
                });
            }
        }

        Ok(issues)
    }
}

struct TickSizeRule;

impl ValidationRule for TickSizeRule {
    fn name(&self) -> &'static str {
        "TickSizeRule"
    }

    fn category(&self) -> &'static str {
        "IntraBarLogic"
    }

    fn metadata(&self) -> RuleMetadata {
        RuleMetadata {
            name: self.name().to_string(),
            category: self.category().to_string(),
            required_fields: vec![
                "open".to_string(),
                "high".to_string(),
                "low".to_string(),
                "close".to_string(),
                "vwap".to_string(),
            ],
            default_severity: RuleSeverity::Medium,
            configurable_thresholds: vec![RuleThresholdMetadata {
                key: "remainder_tolerance".to_string(),
                description: "Allowed modulo remainder tolerance for tick check".to_string(),
                default_value: "0".to_string(),
            }],
        }
    }

    fn validate(
        &self,
        records: &[Record],
        ctx: &ValidationContext,
        plan: &ValidationPlan,
    ) -> Result<Vec<Issue>, ValidationError> {
        let mut issues = Vec::new();
        if ctx.tick_size <= Decimal::ZERO {
            return Ok(issues);
        }
        let tolerance = plan.threshold_or_default(self.name(), "remainder_tolerance", Decimal::ZERO);

        for row in records {
            let invalid = !is_valid_tick(row.open, ctx.tick_size, tolerance)
                || !is_valid_tick(row.high, ctx.tick_size, tolerance)
                || !is_valid_tick(row.low, ctx.tick_size, tolerance)
                || !is_valid_tick(row.close, ctx.tick_size, tolerance)
                || !is_valid_tick(row.vwap, ctx.tick_size, tolerance);

            if invalid {
                issues.push(Issue {
                    issue_type: IssueType::InvalidTickSize,
                    category: self.category().to_string(),
                    rule_name: self.name().to_string(),
                    ticker: row.ticker.clone(),
                    date: row.date.clone(),
                    field: "price".to_string(),
                    value: format!(
                        "open={},high={},low={},close={},vwap={}",
                        row.open, row.high, row.low, row.close, row.vwap
                    ),
                    detail: "Price not aligned to tick size".to_string(),
                });
            }
        }

        Ok(issues)
    }
}

struct VwapRangeRule;

impl ValidationRule for VwapRangeRule {
    fn name(&self) -> &'static str {
        "VwapRangeRule"
    }

    fn category(&self) -> &'static str {
        "IntraBarLogic"
    }

    fn metadata(&self) -> RuleMetadata {
        RuleMetadata {
            name: self.name().to_string(),
            category: self.category().to_string(),
            required_fields: vec!["vwap".to_string(), "low".to_string(), "high".to_string()],
            default_severity: RuleSeverity::Medium,
            configurable_thresholds: vec![RuleThresholdMetadata {
                key: "tolerance".to_string(),
                description: "Absolute tolerance for VWAP range check".to_string(),
                default_value: "0".to_string(),
            }],
        }
    }

    fn validate(
        &self,
        records: &[Record],
        _ctx: &ValidationContext,
        plan: &ValidationPlan,
    ) -> Result<Vec<Issue>, ValidationError> {
        let mut issues = Vec::new();
        let tolerance = plan.threshold_or_default(self.name(), "tolerance", Decimal::ZERO);

        for row in records {
            if row.vwap < row.low - tolerance || row.vwap > row.high + tolerance {
                issues.push(Issue {
                    issue_type: IssueType::VwapOutOfRange,
                    category: self.category().to_string(),
                    rule_name: self.name().to_string(),
                    ticker: row.ticker.clone(),
                    date: row.date.clone(),
                    field: "vwap".to_string(),
                    value: row.vwap.to_string(),
                    detail: "VWAP is outside [Low, High]".to_string(),
                });
            }
        }

        Ok(issues)
    }
}

fn is_valid_tick(value: Decimal, tick_size: Decimal, tolerance: Decimal) -> bool {
    let rem = value % tick_size;
    if rem == Decimal::ZERO {
        return true;
    }

    if tolerance <= Decimal::ZERO {
        return false;
    }

    let distance_to_zero = rem.abs();
    let distance_to_tick = (tick_size - rem.abs()).abs();
    distance_to_zero <= tolerance || distance_to_tick <= tolerance
}

