import argparse
import pandas as pd
from datetime import datetime

def main():
    parser = argparse.ArgumentParser(description="Generate ClickHouse INSERT SQL from CSV")
    parser.add_argument("csv_file", help="Path to exported CSV")
    parser.add_argument("--network", required=True, help="Network name (e.g. 'mainnet')")
    parser.add_argument("--metric_type", required=True, choices=["24h", "30d"], help="Metric type")
    parser.add_argument("--output", default="performance_restore.sql", help="Output SQL file path")
    args = parser.parse_args()

    df = pd.read_csv(args.csv_file)

    metric_dates = df.iloc[0, 1:]
    df = df.iloc[1:]
    df.columns = ['operator_id'] + list(metric_dates)

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    rows = []

    for _, row in df.iterrows():
        operator_id = int(row['operator_id'])
        for col, val in row.items():
            if col == 'operator_id' or pd.isna(val):
                continue
            try:
                metric_date = pd.to_datetime(col).strftime("%Y-%m-%d")
                metric_value = float(val)
                rows.append(
                    f"('{args.network}', {operator_id}, '{args.metric_type}', "
                    f"'{metric_date}', {metric_value}, 'api.ssv.network', '{now}')"
                )
            except Exception as e:
                print(f"Skipping row for operator {operator_id} on {col}: {e}")

    if not rows:
        print("No valid rows found.")
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