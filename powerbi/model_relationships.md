# Semantic Model — Relationships & Configuration

Companion to `dax_measures.md`. Everything here has to be re-applied by hand if
the model is ever rebuilt, so it is written as a checklist rather than prose.

---

## Storage mode: DirectLake, bound to physical tables

The model reads the Gold Delta tables from OneLake directly. **No SQL views.**

DirectLake can only bind to physical Delta tables. A semantic model table
sourced from a SQL analytics endpoint view still works, but it falls back to
DirectQuery — which would discard the exact mode this chapter chose, quietly,
with no error to tell you. That is why `dim_company_current` is a materialised
table written by `write_dim_company_current()` in notebook 03, rather than the
`vw_dim_company_current` view the chapter originally specified.

`sql/views/` is kept for ad-hoc SQL exploration. It is not the model's source.

---

## Tables to include

| Model table | Delta path | Role |
|---|---|---|
| `fact_job_postings` | `Tables/fact_job_postings` | Fact |
| `dim_company_current` | `Tables/dim_company_current` | Dimension (current SCD2 rows only) |
| `dim_location` | `Tables/dim_location` | Dimension |
| `dim_date` | `Tables/dim_date` | Dimension — mark as date table |
| `dim_skill` | `Tables/dim_skill` | Dimension |
| `bridge_job_skill` | `Tables/bridge_job_skill` | Bridge (many-to-many) |

**Do not include `dim_company`.** It holds SCD2 history, so it has more than one
row per company. Adding both it and `dim_company_current` gives two paths from
the fact to a company and makes filter propagation ambiguous.

---

## Relationships

| From | To | Cardinality | Direction | Active |
|---|---|---|---|---|
| `fact_job_postings[company_key]` | `dim_company_current[company_key]` | Many-to-one | Single | Yes |
| `fact_job_postings[location_key]` | `dim_location[location_key]` | Many-to-one | Single | Yes |
| `fact_job_postings[date_key]` | `dim_date[date_key]` | Many-to-one | Single | Yes |
| `fact_job_postings[source_job_id]` | `bridge_job_skill[source_job_id]` | One-to-many | Single | Yes |
| `bridge_job_skill[skill_key]` | `dim_skill[skill_key]` | Many-to-one | **Both** | Yes |

### The fact-to-bridge key is `source_job_id`, not `posting_key`

`build_fact_and_bridge()` derives the bridge from the fact with
`select("source_job_id", explode("skills"))`, and no surrogate `posting_key`
column is ever produced. Chapter 7's original ER diagram and its
`Postings Requiring Skill` measure both named `posting_key`; both are wrong
against the built model.

Consequence worth knowing: this is a **string-keyed** relationship. VertiPaq
compresses and joins integer keys materially better. It is fine at this
project's volume, and it is exactly the pressure that makes surrogate keys
worth the trouble at a larger one.

### Why only the bridge is bi-directional

`bridge_job_skill <-> dim_skill` is bi-directional so that selecting a skill in
a slicer filters the bridge, and the bridge then filters the fact. Without it,
a skill slicer does nothing to the posting counts.

Every other relationship stays single-direction — dimensions filter the fact,
never the reverse. Bi-directional filters elsewhere create multiple propagation
paths between the same two tables, and the symptom is not an error: it is a
measure returning a number that is wrong in a way nobody notices for a month.

---

## Required manual configuration

### 1. Mark `dim_date` as the date table

**Table tools → Mark as date table**, using `full_date` as the date column.

`DATEADD`, `DATESINPERIOD`, and every other time-intelligence function needs
this. Without it they do not error — they return subtly wrong results. This is
the single most-forgotten step in a model rebuild, which is why it has its own
heading.

### 2. Hide technical columns

Hide from report view:

- `fact_job_postings`: `company_key`, `location_key`, `date_key`, `source_job_id`
- `dim_company_current`: `company_key`, `company_natural_key`
- `dim_location`: `location_key`
- `dim_skill`: `skill_key`
- `dim_date`: `date_key`
- `bridge_job_skill`: every column — the whole table is plumbing

Keys are join machinery. Left visible, someone eventually drags `company_key`
onto a visual and gets a meaningless sum of surrogate identifiers.

### 3. Set the `contract_type` column to hidden

It exists only because of Chapter 5's schema-evolution demo and is never
populated. Visible, it reads as a real attribute that is always blank.

### 4. Display folders

Group measures into `Core KPIs`, `Time Intelligence`, `Skills Analysis`, and
`Company Analysis` to match `dax_measures.md`.

---

## Refresh

DirectLake picks up new Delta versions without an import step, so there is no
refresh schedule to configure — the daily pipeline landing a new Gold version
is the refresh.

Two things that still matter:

- **`OPTIMIZE` on the fact table.** DirectLake falls back to DirectQuery when a
  table has too many small Delta row groups. `optimize_fact_table()` exists for
  this; it is not wired into the daily pipeline, so it is currently a manual job.
- **Schema changes break bindings.** Adding a Gold column is safe; renaming or
  dropping one breaks the model table silently until it is refreshed in the
  model editor.
