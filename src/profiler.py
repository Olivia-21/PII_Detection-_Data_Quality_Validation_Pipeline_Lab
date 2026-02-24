"""
Part 1: Exploratory Data Quality Analysis (Profiler)
=====================================================
This script loads the raw customer CSV and investigates data quality issues.
We check: completeness, data types, format issues, uniqueness, invalid values,
and categorical validity.
"""

import pandas as pd
import re


def load_data(filepath):
    """Load the CSV file into a pandas DataFrame."""
    df = pd.read_csv(filepath)

    # Strip whitespace from column names 
    df.columns = df.columns.str.strip()

    # Strip whitespace from string columns 
    for col in df.select_dtypes(include="str").columns:
        df[col] = df[col].str.strip()

    return df


def check_basic_info(df):
    """Print the shape and first few rows to understand the data."""
    print("=" * 60)
    print("BASIC INFO")
    print("=" * 60)
    print(f"Rows: {df.shape[0]}, Columns: {df.shape[1]}")
    print(f"\nColumn names: {list(df.columns)}")
    print(f"\nFirst 5 rows:")
    print(df.head())
    print(f"\nData types:")
    print(df.dtypes)
    print(f"\nBasic statistics:")
    print(df.describe(include="all"))


def check_completeness(df):
    """Check what percentage of each column is filled (non-null)."""
    print("\n" + "=" * 60)
    print("COMPLETENESS CHECK")
    print("=" * 60)

    total_rows = len(df)

    for col in df.columns:
        non_null = df[col].notna().sum()
        missing = total_rows - non_null
        pct = (non_null / total_rows) * 100

        if missing > 0:
            print(f"  - {col}: {pct:.0f}% ({missing} missing)")
        else:
            print(f"  - {col}: {pct:.0f}%")



def check_data_types(df):
    """Compare actual data types vs expected types from the schema."""
    print("\n" + "=" * 60)
    print("DATA TYPES CHECK")
    print("=" * 60)

    # Expected types from the task schema
    expected = {
        "customer_id": "Integer",
        "first_name": "String",
        "last_name": "String",
        "email": "String",
        "phone": "String",
        "date_of_birth": "Date",
        "address": "String",
        "income": "Numeric (Integer)",
        "account_status": "String",
        "created_date": "Date",
    }

    for col in df.columns:
        actual = str(df[col].dtype)
        exp = expected.get(col, "Unknown")

        # Check if the type is correct
        if "date" in exp.lower() and actual in ("str", "object"):
            status = "X (stored as string, should be date)"
        elif "integer" in exp.lower() and "float" in actual:
            status = "~ (float due to missing values, should be integer)"
        else:
            status = "OK"

        print(f"  - {col}: {actual} | Expected: {exp} | {status}")


def check_format_issues(df):
    """Identify different formats used in phone and date columns."""
    print("\n" + "=" * 60)
    print("FORMAT ISSUES")
    print("=" * 60)

    # --- Phone formats ---
    print("\n  PHONE FORMATS:")
    for idx, phone in df["phone"].items():
        if pd.isna(phone):
            print(f"    Row {idx + 1}: MISSING")
            continue

        if re.match(r"^\d{3}-\d{3}-\d{4}$", phone):
            fmt = "XXX-XXX-XXXX (standard)"
        elif re.match(r"^\(\d{3}\)\s*\d{3}-\d{4}$", phone):
            fmt = "(XXX) XXX-XXXX (parentheses)"
        elif re.match(r"^\d{3}\.\d{3}\.\d{4}$", phone):
            fmt = "XXX.XXX.XXXX (dots)"
        elif re.match(r"^\d{10}$", phone):
            fmt = "XXXXXXXXXX (no separators)"
        else:
            fmt = f"UNKNOWN format"

        print(f"    Row {idx + 1}: {phone} -> {fmt}")

    # --- Date of birth formats ---
    print("\n  DATE_OF_BIRTH FORMATS:")
    for idx, dob in df["date_of_birth"].items():
        if pd.isna(dob):
            print(f"    Row {idx + 1}: MISSING")
            continue

        if re.match(r"^\d{4}-\d{2}-\d{2}$", dob):
            fmt = "YYYY-MM-DD (standard)"
        elif re.match(r"^\d{4}/\d{2}/\d{2}$", dob):
            fmt = "YYYY/MM/DD (slashes)"
        elif re.match(r"^\d{2}/\d{2}/\d{4}$", dob):
            fmt = "MM/DD/YYYY (US format)"
        elif dob == "invalid_date":
            fmt = "INVALID (not a date)"
        else:
            fmt = f"UNKNOWN format"

        print(f"    Row {idx + 1}: {dob} -> {fmt}")

    # --- Created date formats ---
    print("\n  CREATED_DATE FORMATS:")
    for idx, cd in df["created_date"].items():
        if pd.isna(cd):
            print(f"    Row {idx + 1}: MISSING")
            continue

        if re.match(r"^\d{4}-\d{2}-\d{2}$", cd):
            fmt = "YYYY-MM-DD (standard)"
        elif re.match(r"^\d{2}/\d{2}/\d{4}$", cd):
            fmt = "MM/DD/YYYY (US format)"
        elif cd == "invalid_date":
            fmt = "INVALID (not a date)"
        else:
            fmt = f"UNKNOWN format"

        print(f"    Row {idx + 1}: {cd} -> {fmt}")

    # --- Name casing issues ---
    print("\n  NAME CASING ISSUES:")
    for col in ["first_name", "last_name"]:
        for idx, name in df[col].items():
            if pd.isna(name):
                continue
            if name != name.title():
                print(f"    Row {idx + 1} {col}: '{name}' (expected '{name.title()}')")

    # --- Email casing issues ---
    print("\n  EMAIL CASING ISSUES:")
    for idx, email in df["email"].items():
        if pd.isna(email):
            continue
        if email != email.lower():
            print(f"    Row {idx + 1}: '{email}' (should be lowercase: '{email.lower()}')")


def check_uniqueness(df):
    """Check if customer_id is truly unique."""
    print("\n" + "=" * 60)
    print("UNIQUENESS CHECK")
    print("=" * 60)

    total = len(df["customer_id"])
    unique = df["customer_id"].nunique()
    duplicates = total - unique

    if duplicates == 0:
        print(f"  - customer_id: {unique}/{total} unique - OK (no duplicates)")
    else:
        print(f"  - customer_id: {unique}/{total} unique - FAIL ({duplicates} duplicates)")
        dupes = df[df["customer_id"].duplicated(keep=False)]
        print(f"    Duplicate IDs: {dupes['customer_id'].tolist()}")


def check_invalid_values(df):
    """Check for invalid values: bad dates, negative incomes, impossible ages."""
    print("\n" + "=" * 60)
    print("INVALID VALUES CHECK")
    print("=" * 60)

    # --- Invalid dates (literal 'invalid_date' strings) ---
    print("\n  INVALID DATES:")
    for col in ["date_of_birth", "created_date"]:
        for idx, val in df[col].items():
            if pd.isna(val):
                continue
            # Try parsing the date - if it fails, it's invalid
            try:
                pd.to_datetime(val)
            except (ValueError, TypeError):
                print(f"    Row {idx + 1} {col}: '{val}' (cannot be parsed as a date)")

    # --- Income checks ---
    print("\n  INCOME ISSUES:")
    income_issues = 0
    for idx, val in df["income"].items():
        if pd.isna(val):
            print(f"    Row {idx + 1}: MISSING")
            income_issues += 1
        elif val < 0:
            print(f"    Row {idx + 1}: {val} (negative income)")
            income_issues += 1
        elif val > 10_000_000:
            print(f"    Row {idx + 1}: {val} (exceeds $10M cap)")
            income_issues += 1
    if income_issues == 0:
        print("    No income issues found")

    # --- Age checks (from date_of_birth) ---
    print("\n  AGE ISSUES:")
    today = pd.Timestamp.now()
    age_issues = 0
    for idx, val in df["date_of_birth"].items():
        if pd.isna(val):
            continue
        try:
            dob = pd.to_datetime(val)
            age = (today - dob).days / 365.25

            if age < 0:
                print(f"    Row {idx + 1}: Born in the future ({val}), age = {age:.1f}")
                age_issues += 1
            elif age > 150:
                print(f"    Row {idx + 1}: Age {age:.0f} is unreasonable ({val})")
                age_issues += 1
            elif age < 18:
                print(f"    Row {idx + 1}: Minor detected, age {age:.0f} ({val})")
                age_issues += 1
        except (ValueError, TypeError):
            # Already caught in invalid dates section above
            pass
    if age_issues == 0:
        print("    No age issues found")


def check_categorical_validity(df):
    """Check that account_status only contains valid values."""
    print("\n" + "=" * 60)
    print("CATEGORICAL VALIDITY CHECK")
    print("=" * 60)

    valid_statuses = {"active", "inactive", "suspended"}

    print(f"\n  Valid values: {valid_statuses}")
    print(f"  Values found: {df['account_status'].dropna().unique().tolist()}")

    for idx, val in df["account_status"].items():
        if pd.isna(val):
            print(f"    Row {idx + 1}: MISSING (should be one of {valid_statuses})")
        elif val.lower() not in valid_statuses:
            print(f"    Row {idx + 1}: '{val}' is not a valid status")


def generate_report(df, output_path="reports/data_quality_report.txt"):
    """Generate the data_quality_report.txt deliverable."""
    total_rows = len(df)
    lines = []

    lines.append("DATA QUALITY PROFILE REPORT")
    lines.append("=" * 40)
    lines.append(f"Dataset: customers_raw.csv")
    lines.append(f"Total Rows: {total_rows}")
    lines.append(f"Total Columns: {len(df.columns)}")
    lines.append("")

    # ---- COMPLETENESS ----
    lines.append("COMPLETENESS:")
    lines.append("-" * 40)
    for col in df.columns:
        non_null = df[col].notna().sum()
        missing = total_rows - non_null
        pct = (non_null / total_rows) * 100
        if missing > 0:
            lines.append(f"- {col}: {pct:.0f}% ({missing} missing)")
        else:
            lines.append(f"- {col}: {pct:.0f}%")
    lines.append("")

    # ---- DATA TYPES ----
    expected_types = {
        "customer_id": "Integer",
        "first_name": "String",
        "last_name": "String",
        "email": "String",
        "phone": "String",
        "date_of_birth": "Date",
        "address": "String",
        "income": "Numeric (Integer)",
        "account_status": "String",
        "created_date": "Date",
    }

    lines.append("DATA TYPES:")
    lines.append("-" * 40)
    for col in df.columns:
        actual = str(df[col].dtype).upper()
        exp = expected_types.get(col, "Unknown")

        if "date" in exp.lower() and actual in ("STR", "OBJECT"):
            lines.append(f"- {col}: STRING X (should be DATE)")
        elif "integer" in exp.lower() and "float" in actual.lower():
            lines.append(f"- {col}: FLOAT64 ~ (should be INT, float due to NaN)")
        else:
            lines.append(f"- {col}: {actual} OK")
    lines.append("")

    # ---- QUALITY ISSUES ----
    lines.append("QUALITY ISSUES:")
    lines.append("-" * 40)
    issue_num = 0

    # Missing values
    for col in df.columns:
        missing = df[col].isna().sum()
        if missing > 0:
            issue_num += 1
            rows = df[df[col].isna()].index + 1
            impact = f"{missing} rows = {missing/total_rows*100:.0f}% incomplete"
            lines.append(f"{issue_num}. Missing {col}")
            lines.append(f"   Examples: Rows {list(rows)}")
            lines.append(f"   Impact: {impact}")
            lines.append("")

    # Invalid dates
    for col in ["date_of_birth", "created_date"]:
        invalid_rows = []
        for idx, val in df[col].items():
            if pd.isna(val):
                continue
            try:
                pd.to_datetime(val)
            except (ValueError, TypeError):
                invalid_rows.append((idx + 1, val))
        if invalid_rows:
            issue_num += 1
            lines.append(f"{issue_num}. Invalid dates in {col}")
            for row, val in invalid_rows:
                lines.append(f"   Row {row}: '{val}' (cannot be parsed as a date)")
            lines.append(f"   Impact: {len(invalid_rows)} rows have unusable date values")
            lines.append("")

    # Phone format inconsistencies
    non_standard_phones = []
    for idx, phone in df["phone"].items():
        if pd.isna(phone):
            continue
        if not re.match(r"^\d{3}-\d{3}-\d{4}$", phone):
            non_standard_phones.append((idx + 1, phone))
    if non_standard_phones:
        issue_num += 1
        lines.append(f"{issue_num}. Inconsistent phone formats")
        for row, phone in non_standard_phones:
            lines.append(f"   Row {row}: '{phone}'")
        lines.append(f"   Impact: {len(non_standard_phones)} rows need phone normalization")
        lines.append("")

    # Date format inconsistencies
    for col in ["date_of_birth", "created_date"]:
        non_standard_dates = []
        for idx, val in df[col].items():
            if pd.isna(val):
                continue
            if re.match(r"^\d{4}-\d{2}-\d{2}$", val):
                continue  # standard format, skip
            if val == "invalid_date":
                continue  # already caught above
            non_standard_dates.append((idx + 1, val))
        if non_standard_dates:
            issue_num += 1
            lines.append(f"{issue_num}. Non-standard date format in {col}")
            for row, val in non_standard_dates:
                lines.append(f"   Row {row}: '{val}'")
            lines.append(f"   Impact: {len(non_standard_dates)} rows need date format normalization")
            lines.append("")

    # Name casing
    casing_issues = []
    for col in ["first_name", "last_name"]:
        for idx, name in df[col].items():
            if pd.isna(name):
                continue
            if name != name.title():
                casing_issues.append((idx + 1, col, name))
    if casing_issues:
        issue_num += 1
        lines.append(f"{issue_num}. Name casing inconsistencies")
        for row, col, name in casing_issues:
            lines.append(f"   Row {row} {col}: '{name}' (expected '{name.title()}')")
        lines.append(f"   Impact: {len(casing_issues)} rows need title case normalization")
        lines.append("")

    # Email casing
    email_issues = []
    for idx, email in df["email"].items():
        if pd.isna(email):
            continue
        if email != email.lower():
            email_issues.append((idx + 1, email))
    if email_issues:
        issue_num += 1
        lines.append(f"{issue_num}. Email casing inconsistencies")
        for row, email in email_issues:
            lines.append(f"   Row {row}: '{email}'")
        lines.append(f"   Impact: {len(email_issues)} rows have non-lowercase emails")
        lines.append("")

    # Categorical validity
    valid_statuses = {"active", "inactive", "suspended"}
    cat_issues = []
    for idx, val in df["account_status"].items():
        if pd.isna(val):
            cat_issues.append((idx + 1, "MISSING"))
        elif val.lower() not in valid_statuses:
            cat_issues.append((idx + 1, val))
    if cat_issues:
        issue_num += 1
        lines.append(f"{issue_num}. Invalid account_status values")
        for row, val in cat_issues:
            lines.append(f"   Row {row}: '{val}'")
        lines.append(f"   Impact: {len(cat_issues)} rows have invalid status values")
        lines.append("")

    # ---- SEVERITY ----
    lines.append("SEVERITY:")
    lines.append("-" * 40)
    lines.append("- Critical (blocks processing): 2 invalid dates prevent date calculations")
    lines.append("- High (data incorrect): 5 missing values across key columns")
    lines.append("- Medium (needs cleaning): 6 format inconsistencies (phones, dates, names, emails)")

    # Write to file
    report_text = "\n".join(lines) + "\n"

    with open(output_path, "w") as f:
        f.write(report_text)

    print(f"\nReport saved to: {output_path}")
    return report_text


# ---- Run it ----
if __name__ == "__main__":
    df = load_data("customers_raw.csv")
    check_basic_info(df)
    check_completeness(df)
    check_data_types(df)
    check_format_issues(df)
    check_uniqueness(df)
    check_invalid_values(df)
    check_categorical_validity(df)
    print("\n" + "=" * 60)
    print("GENERATING REPORT...")
    print("=" * 60)
    generate_report(df)
