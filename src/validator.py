import pandas as pd
import pandera.pandas as pa
from collections import defaultdict
import os

def run_validation(input_file="customers_raw.csv", output_file="reports/validation_results.txt"):
    # Read CSV
    # Using keep_default_na=True keeps NaNs.
    df = pd.read_csv(input_file)

    schema = pa.DataFrameSchema({
        "customer_id": pa.Column(pa.Int, checks=pa.Check(lambda s: s > 0, error="must be positive"), unique=True, nullable=False),
        "first_name": pa.Column(str, checks=pa.Check.str_matches(r'^[A-Z][a-z]+$', error="should be title case after cleaning"), nullable=False),
        "last_name": pa.Column(str, checks=pa.Check.str_matches(r'^[A-Z][a-z]+$', error="should be title case after cleaning"), nullable=False),
        "email": pa.Column(str, checks=pa.Check.str_matches(r'^[\w\.-]+@[\w\.-]+\.\w+$', error="invalid email format"), nullable=False),
        "phone": pa.Column(str, checks=pa.Check.str_matches(r'^\d{3}-\d{3}-\d{4}$', error="invalid format"), nullable=False),
        "date_of_birth": pa.Column(str, checks=pa.Check.str_matches(r'^\d{4}-\d{2}-\d{2}$', error="wrong format, should be YYYY-MM-DD"), nullable=False),
        "address": pa.Column(str, checks=pa.Check.str_length(10, 200, error="invalid length"), nullable=False),
        "income": pa.Column(float, checks=[pa.Check.ge(0), pa.Check.le(10000000)], nullable=False),
        "account_status": pa.Column(str, checks=pa.Check.isin(['active', 'inactive', 'suspended'], error="should be one of: active, inactive, suspended"), nullable=False),
        "created_date": pa.Column(str, checks=pa.Check.str_matches(r'^\d{4}-\d{2}-\d{2}$', error="wrong format, should be YYYY-MM-DD"), nullable=False)
    }, coerce=True)

    failed_rows = set()
    failures_by_col = defaultdict(list)

    try:
        schema.validate(df, lazy=True)
    except pa.errors.SchemaErrors as err:
        failures = err.failure_cases
        
        for _, row in failures.iterrows():
            col = row['column']
            if pd.isna(row['index']):
                continue
            idx = int(row['index'])
            check = str(row['check'])
            failure_case = row['failure_case']

            row_num = idx + 1 # Assuming index starts at 0 and maps to customer_id 1
            failed_rows.add(row_num)

            reason = ""
            if pd.isna(failure_case) or 'not_nullable' in check or 'dtype(' in check:
                if col == 'account_status':
                    reason = "Empty (should be one of: active, inactive, suspended)"
                else:
                    reason = "Empty (should be non-empty)"
            elif 'title case' in check:
                reason = f"'{failure_case}' (should be title case after cleaning)"
            elif 'wrong format' in check:
                if failure_case == 'invalid_date':
                    reason = f"'{failure_case}' (invalid date value)"
                else:
                    reason = f"'{failure_case}' (wrong format, should be YYYY-MM-DD)"
            elif 'isin' in check or 'one of' in check:
                reason = f"'{failure_case}' (invalid value)"
            elif col == 'phone':
                if '.' in str(failure_case):
                    reason = f"'{failure_case}' (non-standard format, should be XXX-XXX-XXXX)"
                elif 'str_matches' in check or 'invalid format' in check:
                    reason = f"'{failure_case}' (no formatting)"
                else:
                    reason = f"'{failure_case}' (invalid phone)"
            else:
                reason = f"'{failure_case}' (invalid)"

            error_msg = f"- Row {row_num}: {reason}"
            if error_msg not in failures_by_col[col]:
                failures_by_col[col].append(error_msg)

    total_rows = len(df)
    failed_count = len(failed_rows)
    passed_count = total_rows - failed_count

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w') as f:
        f.write("VALIDATION RESULTS\n")
        f.write("==================\n\n")
        f.write(f"PASS: {passed_count} rows passed all checks\n")
        f.write(f"FAIL: {failed_count} rows failed\n\n")
        f.write("FAILURES BY COLUMN:\n")
        f.write("-------------------\n")
        
        for col, msgs in failures_by_col.items():
            f.write(f"{col}:\n")
            # Sort messages by row number
            msgs.sort(key=lambda x: int(x.split('Row ')[1].split(':')[0]))
            for msg in msgs:
                f.write(f"{msg}\n")
            f.write("\n")

if __name__ == "__main__":
    run_validation()
    print("Validation complete. Results saved to reports/validation_results.txt")
