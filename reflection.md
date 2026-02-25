# Reflection & Governance

## Top Data Quality Issues Identified & Fixed

1. **Inconsistent Formatting (Dates & Phone Numbers)**: Phone numbers were provided in four different permutations (`555-123-4567`, `(555) 234-5678`, `555.789.0123`, `5557890123`) while Date of Birth and Created Dates lacked a unified standard, including string occurrences like `'invalid_date'` and `'1975/05/10'`.
   * **Fix:** Established a regex-driven normalizer (`src/cleaner.py`) that sanitized everything strictly to `XXX-XXX-XXXX` and parsed dates directly explicitly to `YYYY-MM-DD`. Invalid string dates were intercepted and securely cast to a system default marker (`1999-01-01`).
   * **Impact:** 3 full row structures salvaged for phone numbers, 5 rows for dates—preventing analytics systems reliant on unified structures from crashing upon import.

2. **Null/Missing Values (Scattered)**: Addresses, First/Last names, account status tracking variables, and income were unexpectedly missing.
   * **Fix:** Rather than drop these rows entirely (and risk diminishing the sample size of otherwise valid telemetry records), we opted for safe defaults, including injecting strings like `'[UNKNOWN]'` and zero-ing out numerical missing values where permitted by the schema.
   * **Impact:** Rescued 5 full customer records from being discarded, preserving overall data utility.
   
3. **Improper Casing**: A single value (`PATRICIA.DAVIS@GMAIL.COM`) was overly capitalized where the standard expects strict Title casing for names.
   * **Fix:** Ran the data series through standard pandas string handling to explicitly enforce `Title Case`.
   * **Impact:** 1 row formatted allowing for cleaner reporting display.

## PII Risk Assessment & Masking Trade-Offs

### Detected PII & Sensitivity
We found First/Last Names, Phone Numbers, Emails, home Addresses, and full birth dates within the core dataset.
These are definitively highly sensitive metrics. Their exactness makes an individual easily identifiable and subject to targeted abuse when not insulated appropriately in accordance with global regulations—such as the GDPR or CCPA. 

### Potential Implications of a Breach
Leakage of this data invites grave potential hazards including:
* **Identification Spoofing**: Names, paired with full Date of Births, and associated home addresses represent fundamental identity components capable of bypassing standard security measures across third-party financial institutions. 
* **Targeted Phishing/Social Engineering**: Exposed email structures and phone associations increase susceptibility toward specialized attacks for these specific clients. 
* **Regulatory/Reputational Impact**: Fines for lack of compliance (or, worse, breaches indicating lack of standard data stewardship procedures) routinely reach into the millions while severely eroding customer trust.

### To Mask or Not To Mask?
Masking comes with a trade-off against utility.
**When is it worth it?**
When the data is passed to wide analytical groups modeling macro-level patterns (e.g. churn predictions based on general income levels vs current account status features). Knowing the user is specifically 'John Doe' provides absolutely zero value toward the final machine-learning prediction on cohort behaviors.
**When would you not mask?**
You would retain the raw, original state for operational necessity: direct communications (i.e., transactional billing emails from the CRM platform) or strict multi-factor authentication procedures requiring precise data matching. 

## Validation Strategy

Our validators operated strongly, intercepting structural anomalies via regex immediately on the initial profile load.

They are currently highly robust at identifying type-level mismatches and generic constraint violations (e.g., negative incomes).

**What they might miss:** True logical incongruities.
For example, a user's date of birth could formally validate perfectly (e.g., `YYYY-MM-DD`) but still be wildly inaccurate or contradictory entirely relative to their `created_date` (e.g. they appear to have made the account before they were born).

To improve them, I would add cross-column validations verifying context logic beyond basic schema definitions. 

## Production Operations

In an authentic environment, a pipeline like this should ideally be integrated as part of an Airflow ETL DAG execution running incrementally (e.g. hourly) in response to batch data drops originating from upstream CRMs, saving direct outputs immediately to a secure cloud blob storage bucket.

**What happens upon validation failure?**
The pipeline should explicitly not fail silently.
Rows failing initial validation schemas should be automatically decoupled and shuttled toward a separate 'Dead Letter Queue' architecture structure. Operations teams or downstream data stewards would then receive alerts detailing exactly the nature of the unparseable fields generated, permitting human intervention or deeper programmatic debugging routines later on, while passing only valid rows through the core pipeline.
