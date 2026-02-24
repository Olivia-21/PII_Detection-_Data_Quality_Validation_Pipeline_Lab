"""
Part 2: PII Detection
======================
This script identifies Personally Identifiable Information (PII) in the dataset.
PII is any data that could be used to identify a specific individual.
We use regex patterns to detect emails, phone numbers, and other sensitive data.
"""

import re
import pandas as pd


def load_data(filepath):
    """Load the CSV file into a pandas DataFrame."""
    df = pd.read_csv(filepath)
    df.columns = df.columns.str.strip()
    for col in df.select_dtypes(include="str").columns:
        df[col] = df[col].str.strip()
    return df


def classify_pii_columns():
    """
    Define which columns contain PII and their risk level.
    This is based on data privacy regulations like GDPR and POPIA.
    """
    pii_classification = {
        "HIGH": {
            "first_name": "Direct identifier - can identify a person",
            "last_name": "Direct identifier - can identify a person",
            "email": "Contact info - can be used for phishing",
            "phone": "Contact info - can be used for social engineering",
            "date_of_birth": "Sensitive personal data - used in identity verification",
            "address": "Location data - reveals where a person lives",
        },
        "MEDIUM": {
            "income": "Financial data - sensitive but not directly identifying",
        },
        "LOW": {
            "customer_id": "Internal ID - not PII on its own",
            "account_status": "Account metadata - not personally identifying",
            "created_date": "System metadata - not personally identifying",
        },
    }
    return pii_classification


def detect_emails(df):
    """Use regex to find email patterns in the data."""
    email_pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"

    print("\n  EMAIL DETECTION:")
    found = 0
    for idx, val in df["email"].items():
        if pd.isna(val):
            print(f"    Row {idx + 1}: MISSING (no email)")
            continue
        if re.match(email_pattern, val):
            print(f"    Row {idx + 1}: '{val}' - PII DETECTED")
            found += 1
        else:
            print(f"    Row {idx + 1}: '{val}' - No valid email pattern")

    print(f"  Total emails found: {found}/{len(df)} ({found/len(df)*100:.0f}%)")
    return found


def detect_phones(df):
    """Use regex to find phone number patterns in the data."""
    # Match multiple phone formats: XXX-XXX-XXXX, (XXX) XXX-XXXX, XXX.XXX.XXXX, XXXXXXXXXX
    phone_pattern = r"(\d{3}[-.]?\d{3}[-.]?\d{4}|\(\d{3}\)\s*\d{3}-\d{4})"

    print("\n  PHONE DETECTION:")
    found = 0
    for idx, val in df["phone"].items():
        if pd.isna(val):
            print(f"    Row {idx + 1}: MISSING (no phone)")
            continue
        if re.match(phone_pattern, val):
            print(f"    Row {idx + 1}: '{val}' - PII DETECTED")
            found += 1
        else:
            print(f"    Row {idx + 1}: '{val}' - No valid phone pattern")

    print(f"  Total phones found: {found}/{len(df)} ({found/len(df)*100:.0f}%)")
    return found


def detect_names(df):
    """Count rows with name data present (names are direct identifiers)."""
    print("\n  NAME DETECTION:")

    first_found = df["first_name"].notna().sum()
    last_found = df["last_name"].notna().sum()

    # Rows with at least one name component
    has_name = (df["first_name"].notna() | df["last_name"].notna()).sum()

    print(f"  First names found: {first_found}/{len(df)} ({first_found/len(df)*100:.0f}%)")
    print(f"  Last names found: {last_found}/{len(df)} ({last_found/len(df)*100:.0f}%)")
    print(f"  Rows with any name: {has_name}/{len(df)} ({has_name/len(df)*100:.0f}%)")
    return first_found, last_found


def detect_addresses(df):
    """Count rows with address data present."""
    print("\n  ADDRESS DETECTION:")

    found = df["address"].notna().sum()
    print(f"  Addresses found: {found}/{len(df)} ({found/len(df)*100:.0f}%)")
    return found


def detect_dob(df):
    """Count rows with date of birth data (valid dates only)."""
    print("\n  DATE OF BIRTH DETECTION:")

    found = 0
    for idx, val in df["date_of_birth"].items():
        if pd.isna(val):
            continue
        try:
            pd.to_datetime(val)
            found += 1
        except (ValueError, TypeError):
            print(f"    Row {idx + 1}: '{val}' - Invalid (not a real DOB)")

    print(f"  Valid DOBs found: {found}/{len(df)} ({found/len(df)*100:.0f}%)")
    return found


def generate_pii_report(df, output_path="reports/pii_detection_report.txt"):
    """Generate the pii_detection_report.txt deliverable."""
    total = len(df)
    lines = []

    # Count all PII
    emails = df["email"].apply(
        lambda x: bool(re.match(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", str(x)))
        if pd.notna(x) else False
    ).sum()

    phones = df["phone"].apply(
        lambda x: bool(re.match(r"(\d{3}[-.]?\d{3}[-.]?\d{4}|\(\d{3}\)\s*\d{3}-\d{4})", str(x)))
        if pd.notna(x) else False
    ).sum()

    first_names = df["first_name"].notna().sum()
    last_names = df["last_name"].notna().sum()
    addresses = df["address"].notna().sum()

    valid_dobs = 0
    for val in df["date_of_birth"]:
        if pd.isna(val):
            continue
        try:
            pd.to_datetime(val)
            valid_dobs += 1
        except (ValueError, TypeError):
            pass

    lines.append("PII DETECTION REPORT")
    lines.append("=" * 40)
    lines.append(f"Dataset: customers_raw.csv")
    lines.append(f"Total Rows: {total}")
    lines.append("")

    lines.append("RISK ASSESSMENT:")
    lines.append("-" * 40)
    lines.append("- HIGH: Names, emails, phone numbers, addresses, dates of birth")
    lines.append("- MEDIUM: Income (financial sensitivity)")
    lines.append("- LOW: customer_id, account_status, created_date (not PII)")
    lines.append("")

    lines.append("DETECTED PII:")
    lines.append("-" * 40)
    lines.append(f"- First names found: {first_names} ({first_names/total*100:.0f}%)")
    lines.append(f"- Last names found: {last_names} ({last_names/total*100:.0f}%)")
    lines.append(f"- Emails found: {emails} ({emails/total*100:.0f}%)")
    lines.append(f"- Phone numbers found: {phones} ({phones/total*100:.0f}%)")
    lines.append(f"- Addresses found: {addresses} ({addresses/total*100:.0f}%)")
    lines.append(f"- Dates of birth found: {valid_dobs} ({valid_dobs/total*100:.0f}%)")
    lines.append("")

    lines.append("PII COLUMNS BREAKDOWN:")
    lines.append("-" * 40)
    pii = classify_pii_columns()
    for risk, columns in pii.items():
        lines.append(f"\n  {risk} RISK:")
        for col, reason in columns.items():
            lines.append(f"  - {col}: {reason}")
    lines.append("")

    lines.append("EXPOSURE RISK:")
    lines.append("-" * 40)
    lines.append("If this dataset were breached, attackers could:")
    lines.append("- Phish customers using their email addresses")
    lines.append("- Spoof identities using names + DOB + address combinations")
    lines.append("- Social engineer targets using phone numbers")
    lines.append("- Commit financial fraud using income data")
    lines.append("- Physically locate customers using address data")
    lines.append("")

    lines.append("MITIGATION:")
    lines.append("-" * 40)
    lines.append("- Mask all PII before sharing with analytics teams")
    lines.append("- Apply data minimization (only share what is needed)")
    lines.append("- Encrypt sensitive columns at rest")
    lines.append("- Implement access controls (role-based access)")

    report_text = "\n".join(lines) + "\n"

    with open(output_path, "w") as f:
        f.write(report_text)

    print(f"\nReport saved to: {output_path}")
    return report_text


# ---- Run it ----
if __name__ == "__main__":
    df = load_data("customers_raw.csv")

    print("=" * 60)
    print("PII DETECTION")
    print("=" * 60)

    print("\nPII COLUMN CLASSIFICATION:")
    pii = classify_pii_columns()
    for risk, columns in pii.items():
        print(f"\n  {risk} RISK:")
        for col, reason in columns.items():
            print(f"    - {col}: {reason}")

    print("\n" + "=" * 60)
    print("SCANNING FOR PII PATTERNS")
    print("=" * 60)

    detect_emails(df)
    detect_phones(df)
    detect_names(df)
    detect_addresses(df)
    detect_dob(df)

    print("\n" + "=" * 60)
    print("GENERATING REPORT...")
    print("=" * 60)
    generate_pii_report(df)
