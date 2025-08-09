# 🆔 PAN Number Validation — Data Quality in SQL

> **“In compliance-critical domains, bad data is expensive data.”**  
> Part of my *Small but Useful Projects* series — focused, real-world builds to sharpen my SQL and analytics skills.

---

## 📌 The Problem

In India, the **Permanent Account Number (PAN)** is a key tax and compliance identifier.  
Banks, fintechs, and government agencies all depend on it being accurate.  

But when PANs are invalid:
- Loan approvals stall
- Onboarding fails
- Compliance risk increases

**Business challenge:**  
Validate 10,000 incoming PAN records, diagnose *why* each is invalid, and summarize the findings in a way that helps the business take action.

---

## 🛠 My Approach

I built a single SQL pipeline to:
1. **Stage** — Load raw PANs (`stg_pan_numbers`)
2. **Clean** — Standardize case, trim spaces, replace blanks
3. **Helpers** — Functions to detect repeats & sequences
4. **Validate** — Apply official PAN rules:
   ```
   Format: AAAAA9999A
   - First 5: uppercase letters (no repeats/sequences)
   - Next 4: digits (no repeats/sequences)
   - Last: uppercase letter
   ```
5. **Summarize** — BI-friendly views (totals, reasons, examples)
6. **Single-PAN Check** — Validate any given PAN with JSON reasons

---

## Key Results (10,000 Rows)

### Quality Snapshot
| total_rows | valid_rows | invalid_rows | missing_rows | valid_pct | invalid_pct |
|------------|-----------:|-------------:|-------------:|----------:|------------:|
| 10000      | 3186       | 5847         | 967          | 31.86%    | 58.47%      |

> **Insight:** Almost 6 in 10 PANs were invalid — a major operational risk.

---

### Top Reasons for Failure
| reason              | count | pct    |
|---------------------|------:|-------:|
| PATTERN_FAIL        | 4034  | 27.61% |
| ADJ_REPEAT_DIGITS   | 2151  | 14.72% |
| MID4_NOT_DIGITS     | 2047  | 14.01% |
| LEN_NE_10           | 2008  | 13.74% |
| FIRST5_NOT_ALPHA    | 1987  | 13.60% |

---

### Sample Masked Invalids
| reason              | pan_masked |
|---------------------|------------|
| ADJ_REPEAT_ALPHA    | JJCHK4574X |
| ADJ_REPEAT_DIGITS   | XTQIJ2330X |
| SEQ_DIGITS_ASC      | ETVSQ2345X |
| LEN_NE_10           | DOURT5035YX|
| NON_ALNUM           | DOURT5035YX|

---

### Co-Occurrence Insights
Some rules fail together — fixing one upstream process could fix multiple issues:
- `MID4_NOT_DIGITS` + `PATTERN_FAIL` → 2,047 rows
- `LEN_NE_10` + `PATTERN_FAIL` → 2,008 rows
- `FIRST5_NOT_ALPHA` + `PATTERN_FAIL` → 1,987 rows

---

## Try It — Single PAN Check

```sql
SELECT * FROM validate_pan('ABCDE1234F');
```

**Result:**
| input_pan  | status      | reasons                                   |
|------------|-------------|-------------------------------------------|
| ABCDE1234F | Invalid PAN | ["SEQ_ALPHA_ASC", "SEQ_DIGITS_ASC"]        |

---

## How to Run

1. **Create database & run script**
   ```bash
   createdb pan_demo
   psql -d pan_demo -f sql/pan_validation.sql
   ```
2. **Load dataset**
   ```sql
   \copy stg_pan_numbers(pan_raw) FROM 'data/pan_numbers.csv' CSV HEADER
   ```
3. **Run checks**
   ```sql
   SELECT * FROM vw_pan_summary;
   SELECT * FROM vw_invalid_by_reason;
   SELECT * FROM vw_invalid_examples;
   SELECT * FROM validate_pan('ABCDE1234F');
   ```

---

## 💼 Business Value

This pipeline:
- Flags **invalid data before it hits production**
- Explains *why* each record failed, so teams can fix at source
- Produces BI-friendly outputs for monitoring

In a real environment, this could:
- Reduce onboarding delays
- Lower compliance risk
- Improve partner data contracts

---

## 📅 Next Steps

- [ ] Add Power BI dashboard (interactive summary + “Try a PAN” page)
- [ ] Parameterize rules from a config table
- [ ] Add lightweight tests

---

📌 **Part of my “Small but Useful Projects” SQL series**  
Focused, real-world practice — from problem to business-ready insights.
