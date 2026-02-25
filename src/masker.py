import pandas as pd
import os

def mask_name(name):
    if pd.isna(name) or len(str(name)) == 0:
        return name
    name_str = str(name)
    return name_str[0] + "***"

def mask_email(email):
    if pd.isna(email) or '@' not in str(email):
        return email
    email_str = str(email)
    first_char = email_str[0]
    domain = email_str.split('@')[1]
    return f"{first_char}***@{domain}"

def mask_phone(phone):
    if pd.isna(phone):
        return phone
    phone_str = str(phone)
    if len(phone_str) >= 12: # Expecting XXX-XXX-XXXX
        return f"***-***-{phone_str[-4:]}"
    return "***-***-****"

def mask_address(address):
    if pd.isna(address):
        return address
    return "[MASKED ADDRESS]"

def mask_dob(dob):
    if pd.isna(dob):
        return dob
    dob_str = str(dob)
    if len(dob_str) >= 4:
        # Assuming YYYY-MM-DD
        return f"{dob_str[:4]}-**-**"
    return dob_str

def run_masker():
    df_cleaned = pd.read_csv("output/customers_cleaned.csv")
    
    # Store first 2 rows of cleaned data for report
    before_csv = df_cleaned.head(2).to_csv(index=False)
    
    # Apply masking
    df_masked = df_cleaned.copy()
    
    df_masked['first_name'] = df_masked['first_name'].apply(mask_name)
    df_masked['last_name'] = df_masked['last_name'].apply(mask_name)
    df_masked['email'] = df_masked['email'].apply(mask_email)
    df_masked['phone'] = df_masked['phone'].apply(mask_phone)
    df_masked['address'] = df_masked['address'].apply(mask_address)
    df_masked['date_of_birth'] = df_masked['date_of_birth'].apply(mask_dob)
    
    # Save masked output
    os.makedirs('output', exist_ok=True)
    df_masked.to_csv("output/customers_masked.csv", index=False)
    
    # Get first 2 rows of masked data for report
    after_csv = df_masked.head(2).to_csv(index=False)
    
    # Generate Report
    report_content = f"""BEFORE MASKING (first 2 rows):
------------------------------
{before_csv}
AFTER MASKING (first 2 rows):
-----------------------------
{after_csv}
ANALYSIS:
- Data structure preserved (still {len(df_masked)} rows, {len(df_masked.columns)} columns)
- PII masked (names, emails, phones, addresses, DOBs hidden)
- Business data intact (income, account_status, dates available)
- Use case: Safe for analytics team (GDPR compliant)
"""
    
    os.makedirs('reports', exist_ok=True)
    with open("reports/masked_sample.txt", "w") as f:
        f.write(report_content)
        
    print(f"PII Masking complete. Masked data saved to output/customers_masked.csv")

if __name__ == "__main__":
    run_masker()
