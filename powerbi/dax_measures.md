# DAX Measure Library

Every measure in the semantic model, grouped by display folder. Table names
match the DirectLake model described in `model_relationships.md` — physical Gold
tables, not the SQL views Chapter 7 originally specified.

---

## Display Folder: Core KPIs

```dax
Total Job Postings =
COUNTROWS ( 'fact_job_postings' )
```

```dax
Total Companies Hiring =
DISTINCTCOUNT ( 'fact_job_postings'[company_key] )
```

```dax
Average Salary =
AVERAGEX (
    FILTER ( 'fact_job_postings', NOT ISBLANK ( 'fact_job_postings'[salary_min] ) ),
    ( 'fact_job_postings'[salary_min] + 'fact_job_postings'[salary_max] ) / 2
)
```

`AVERAGEX` rather than `AVERAGE` because "average salary" is defined as the
midpoint of each posting's range — a row-level calculation that has to happen
inside the iteration, not an average of `salary_min` alone.

The `FILTER` matters more than it looks. Jooble returns salary as free text and
`parse_salaries` leaves it null when unparseable, so a large share of rows have
no salary at all. Without the filter those rows would drag the average toward
zero rather than being excluded.

```dax
Salary Data Coverage % =
DIVIDE (
    CALCULATE (
        COUNTROWS ( 'fact_job_postings' ),
        NOT ISBLANK ( 'fact_job_postings'[salary_min] )
    ),
    COUNTROWS ( 'fact_job_postings' )
)
```

Pair this with `Average Salary` on every page that shows it. An average over 8%
of postings is a different claim from an average over 80%, and the reader cannot
tell which they are looking at unless you show them.

`DIVIDE` rather than `/` — it returns BLANK instead of erroring when a slicer
produces an empty result set.

```dax
Remote % =
DIVIDE (
    CALCULATE (
        COUNTROWS ( 'fact_job_postings' ),
        'fact_job_postings'[is_remote] = TRUE ()
    ),
    [Total Job Postings]
)
```

Caveat to carry into the report: `is_remote` is null for every Adzuna posting —
the connector sets `remote=None` because Adzuna does not expose the field, and
Silver derives it only where it can. This measure therefore reads as "confirmed
remote", not "remote". Label it that way on the page.

---

## Display Folder: Time Intelligence

Both measures below require `dim_date` to be marked as the model's date table
using `full_date`. See `model_relationships.md`.

```dax
Postings MoM Growth % =
VAR CurrentMonthPostings = [Total Job Postings]
VAR PriorMonthPostings =
    CALCULATE ( [Total Job Postings], DATEADD ( 'dim_date'[full_date], -1, MONTH ) )
RETURN
    DIVIDE ( CurrentMonthPostings - PriorMonthPostings, PriorMonthPostings )
```

```dax
Postings Trailing 7 Day Avg =
AVERAGEX (
    DATESINPERIOD ( 'dim_date'[full_date], MAX ( 'dim_date'[full_date] ), -7, DAY ),
    CALCULATE ( [Total Job Postings] )
)
```

**These two need history to mean anything.** The pipeline started collecting
recently and runs once daily, so month-over-month has nothing to compare against
until a second month exists. Build them now, but do not put them on a page until
the data supports them — a growth figure computed against a partial first month
is not a small error, it is a fabricated one.

---

## Display Folder: Skills Analysis

```dax
Postings Requiring Skill =
DISTINCTCOUNT ( 'bridge_job_skill'[source_job_id] )
```

`source_job_id`, not `posting_key` — the fact table has no surrogate key and the
bridge is keyed on the natural one. Chapter 7's original text specified
`posting_key`; it does not exist in the built model.

Relies on the bi-directional `bridge_job_skill <-> dim_skill` relationship:
slicing `dim_skill[skill_name]` filters the bridge, which filters the fact.

```dax
Top Skill Rank =
RANKX (
    ALL ( 'dim_skill'[skill_name] ),
    CALCULATE ( [Postings Requiring Skill] ),
    ,
    DESC
)
```

`ALL` strips the existing filter on `dim_skill` so the ranking is computed
across every skill regardless of what is sliced elsewhere. Without it, `RANKX`
ranks only within the current filter and returns 1 for almost everything.

**Read this measure knowing where skills come from.** `extract_skills` is
keyword matching against a curated taxonomy in `skill_taxonomy.py`, applied to
the description text. It measures *how often a term appears in job descriptions*,
not what employers actually require. A skill missing from the taxonomy scores
zero no matter how in-demand it is.

---

## Display Folder: Company Analysis

```dax
Top Hiring Companies Rank =
RANKX (
    ALL ( 'dim_company_current'[company_name] ),
    CALCULATE ( [Total Job Postings] ),
    ,
    DESC
)
```

```dax
Is Top 10 Company =
VAR CurrentRank = [Top Hiring Companies Rank]
RETURN
    IF ( NOT ISBLANK ( CurrentRank ) && CurrentRank <= 10, 1, 0 )
```

Both use `dim_company_current`, so they are automatically scoped to the current
SCD2 row per company — no measure needs to remember `is_current = TRUE()`.

```dax
Avg Postings Per Company =
DIVIDE ( [Total Job Postings], [Total Companies Hiring] )
```

---

## Measures deliberately not built

**Anything keyed on `posting_key`.** No such column exists.

**Salary in a single currency.** `fact_job_postings[currency]` mixes USD and GBP
because `sources.yaml` enables Adzuna for both `us` and `gb`. Any salary measure
that spans countries is summing across currencies. The measures above are only
honest when a country or currency slicer is applied — or once an FX conversion
step is added to Silver. Worth stating on the page rather than leaving implicit.
