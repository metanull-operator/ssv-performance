import argparse
import pandas as pd
from datetime import datetime

def parse_metric_value(raw):
    if pd.isna(raw):
        return None
    try:
        str_val = str(raw).strip()
        print(f"Parsing metric value: '{str_val}'")
        if str_val.endswith('%'):
            print(f"Detected percentage format: '{str_val}'")
            print(f"Converted to float: {float(str_val.rstrip('%')) / 100.0}")
            return float(str_val.rstrip('%')) / 100.0
        print(f"Converted to float: {str_val}")
        return float(str_val)
    except Exception as e:
        print(f"⚠️ Skipping invalid value '{raw}': {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Generate ClickHouse INSERT SQL from CSV")
    parser.add_argument("csv_file", help="Path to exported CSV")
    parser.add_argument("--network", required=True, help="Network name (e.g. 'mainnet')")
    parser.add_argument("--metric_type", required=True, choices=["24h", "30d"], help="Metric type")
    parser.add_argument("--output", default="performance_restore.sql", help="Output SQL file path")
    args = parser.parse_args()

    df = pd.read_csv(args.csv_file, header=0)
    df.columns = [col.strip() for col in df.columns]

    metric_dates = df.columns[1:]

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    rows = []

    for _, row in df.iterrows():
        try:
            operator_id = int(row['OperatorID'])
        except Exception as e:
            print(f"⚠️ Skipping row with invalid OperatorID: {e}")
            continue

        for col in metric_dates:  # metric_dates = df.columns[1:]
            val = row[col]

            metric_value = parse_metric_value(val)
            print(f"Processing operator {operator_id}, column '{col}': value = {metric_value}")

            if metric_value is None:
                continue

            try:
                metric_date = pd.to_datetime(col, errors='raise').strftime("%Y-%m-%d")

                rows.append(
                    f"('{args.network}', {operator_id}, '{args.metric_type}', "
                    f"'{metric_date}', {metric_value}, 'api.ssv.network', '{now}')"
                )

            except Exception as e:
                print(f"⚠️ Skipping row for operator {operator_id} on column '{col}': {e}")

    if not rows:
        print("❌ No valid rows found.")
        return

    insert_sql = (
        "INSERT INTO default.performance "
        "(network, operator_id, metric_type, metric_date, metric_value, source, updated_at) VALUES\n"
        + ",\n".join(rows) + ";\n"
        + "OPTIMIZE TABLE default.performance FINAL;\n"
    )

    with open(args.output, "w") as f:
        f.write(insert_sql)

    print(f"✅ SQL written to {args.output} ({len(rows)} rows)")

if __name__ == "__main__":
    main()
