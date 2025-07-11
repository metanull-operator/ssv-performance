Files with the `.sql` file extension in the `sql-import/` directory will be imported into the Clickhouse database when running the clickhouse-import script.

Filenames must be the table name with the `.sql` extension. Presently allowed filenames include:

- operators.sql
- performance.sql
- subscriptions.sql
- validator_counts.sql
- operator_fees.sql