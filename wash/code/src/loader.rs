use super::*;

pub fn load_data(cfg: &LoadConfig) -> Result<LoadOutput, LoadStageError> {
    match cfg.input.format {
        InputFormat::Csv => load_csv_data(cfg),
        InputFormat::Parquet => Err(LoadStageError::UnsupportedFormat("parquet".to_string())),
    }
}

fn load_csv_data(cfg: &LoadConfig) -> Result<LoadOutput, LoadStageError> {
    let path = &cfg.input.path;
    if !path.exists() {
        return Err(LoadStageError::OpenInput(path.display().to_string()));
    }

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(path)
        .map_err(|e| LoadStageError::CsvRead(e.to_string()))?;

    let headers = reader
        .headers()
        .map_err(|e| LoadStageError::CsvRead(e.to_string()))?
        .clone();

    // Build header lookup once and pass to row parser.
    let mut header_index = HashMap::new();
    for (idx, col) in headers.iter().enumerate() {
        header_index.insert(col.to_string(), idx);
    }

    let mut records = Vec::new();
    let mut load_errors = Vec::new();
    let mut total_rows = 0usize;

    for (idx, row) in reader.records().enumerate() {
        total_rows += 1;
        let row_number = idx + 1;
        match row {
            Ok(rec) => match parse_csv_row(&rec, &header_index, &cfg.input.schema, row_number) {
                Ok(parsed) => records.push(parsed),
                Err(err) => load_errors.push(err),
            },
            Err(err) => load_errors.push(LoadError {
                stage: "LOAD",
                row_number,
                raw_row: String::new(),
                error_code: LoadErrorCode::ParseFail,
                error_detail: err.to_string(),
            }),
        }
    }

    Ok(LoadOutput {
        total_rows,
        records,
        load_errors,
    })
}

fn parse_csv_row(
    row: &csv::StringRecord,
    header_index: &HashMap<String, usize>,
    schema: &InputSchemaMap,
    row_number: usize,
) -> Result<Record, LoadError> {
    // Keep original row for diagnostics so bad rows remain inspectable later.
    let raw_row = row.iter().collect::<Vec<_>>().join(",");

    let get = |column: &str| -> Result<&str, LoadError> {
        let idx = header_index.get(column).copied().ok_or_else(|| LoadError {
            stage: "LOAD",
            row_number,
            raw_row: raw_row.clone(),
            error_code: LoadErrorCode::MissingField,
            error_detail: format!("column not found in header: {column}"),
        })?;

        row.get(idx).ok_or_else(|| LoadError {
            stage: "LOAD",
            row_number,
            raw_row: raw_row.clone(),
            error_code: LoadErrorCode::MissingField,
            error_detail: format!("missing value for column: {column}"),
        })
    };

    let parse_decimal = |s: &str, field: &str| -> Result<Decimal, LoadError> {
        Decimal::from_str(s).map_err(|_| LoadError {
            stage: "LOAD",
            row_number,
            raw_row: raw_row.clone(),
            error_code: LoadErrorCode::TypeCastFail,
            error_detail: format!("invalid decimal for {field}: {s}"),
        })
    };

    let parse_decimal_metric = |s: &str, field: &str| -> Result<Decimal, LoadError> {
        Decimal::from_str(s).map_err(|_| LoadError {
            stage: "LOAD",
            row_number,
            raw_row: raw_row.clone(),
            error_code: LoadErrorCode::TypeCastFail,
            error_detail: format!("invalid decimal for {field}: {s}"),
        })
    };

    let date = get(&schema.date)?.to_string();
    let ticker = get(&schema.ticker)?.to_string();

    let open = parse_decimal(get(&schema.open)?, "open")?;
    let high = parse_decimal(get(&schema.high)?, "high")?;
    let low = parse_decimal(get(&schema.low)?, "low")?;
    let close = parse_decimal(get(&schema.close)?, "close")?;
    let vwap = parse_decimal(get(&schema.vwap)?, "vwap")?;

    let volume = parse_decimal_metric(get(&schema.volume)?, "volume")?;
    let turnover = parse_decimal_metric(get(&schema.turnover)?, "turnover")?;
    // Some vendor datasets do not provide an explicit status column.
    // Treat missing/blank status as NORMAL to keep loading resilient.
    let status = header_index
        .get(&schema.status)
        .and_then(|idx| row.get(*idx))
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(TradeStatus::parse)
        .unwrap_or(TradeStatus::Normal);

    Ok(Record {
        date,
        ticker,
        open,
        high,
        low,
        close,
        vwap,
        volume,
        turnover,
        status,
    })
}
