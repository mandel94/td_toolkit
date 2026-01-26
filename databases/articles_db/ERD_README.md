# Articles Database ERD Generation

This directory contains tools to generate Entity-Relationship Diagrams (ERD) and database documentation for the Articles Database.

## ERD Generation Tools

### Option 1: SchemaSpy (Recommended)
**Best for:** Interactive HTML documentation with ERD diagrams, relationship details, and statistics.

**Features:**
- ✅ Interactive HTML output with clickable diagrams
- ✅ Shows all relationships, foreign keys, and constraints
- ✅ Includes table details, column types, and indexes
- ✅ Generates multiple diagram views (summary, detailed, relationships)
- ✅ Completely free and open-source
- ✅ No external dependencies required

**Usage:**
```powershell
# Generate ERD documentation
.\generate-erd.ps1

# Generate and automatically open in browser
.\generate-erd.ps1 -Open

# Clean previous output and regenerate
.\generate-erd.ps1 -Clean -Open
```

**Output:**
- Location: `.\schemaspy-output\`
- Main page: `.\schemaspy-output\index.html`
- Diagrams: Navigate to "Relationships" page for full ERD

### Option 2: Manual Generation with Docker

You can also run SchemaSpy directly:

```bash
# Start the database
docker-compose up -d articles_postgres

# Generate ERD (one-time run)
docker-compose --profile erd run --rm schemaspy

# View output
start schemaspy-output/index.html
```

## Alternative Tools Considered

### DBeaver ERD Plugin
- **Pros:** Rich GUI, manual diagram editing
- **Cons:** Requires desktop application, not purely Docker-based

### dbdiagram.io
- **Pros:** Beautiful diagrams, collaborative
- **Cons:** Requires manual schema definition, cloud-based

### tbls (Table Documentation Tool)
- **Pros:** Markdown output, CI/CD friendly
- **Cons:** Less visual, requires additional configuration

### pgModeler
- **Pros:** Professional modeling tool
- **Cons:** Complex setup, desktop application

## Database Schema Overview

### Dimension Tables
- `dim_authors` - Article authors
- `dim_categories` - Article categories
- `dim_weeks` - Week dimension (year, week, date range)
- `dim_articles` - Article master data

### Fact Tables
- `fact_weekly_metrics` - Weekly article performance metrics

### Star Schema
The database follows a star schema design with:
- **Center:** `fact_weekly_metrics` (grain: article per week)
- **Points:** Four dimension tables providing context

## Sharing with Stakeholders

### Option A: Share HTML Documentation
1. Run `.\generate-erd.ps1 -Clean`
2. Zip the `schemaspy-output` folder
3. Share via email/SharePoint
4. Recipients open `index.html` in browser

### Option B: Host Locally
1. Generate documentation
2. Use Python HTTP server: `python -m http.server -d schemaspy-output 8000`
3. Share URL: `http://localhost:8000`

### Option C: Export Diagrams as Images
1. Open `schemaspy-output/index.html`
2. Navigate to Relationships page
3. Take screenshot or use browser "Print to PDF"
4. Share image/PDF

## Tips

- **First run:** May take 10-15 seconds to generate
- **Incremental updates:** Run after schema changes
- **Version control:** Add `schemaspy-output/` to `.gitignore`
- **Cleanup:** Use `-Clean` flag to remove cached data

## Troubleshooting

**Database not running:**
```powershell
docker-compose up -d articles_postgres
```

**Permission issues:**
```powershell
# Ensure output directory is writable
New-Item -ItemType Directory -Force -Path .\schemaspy-output
```

**Port conflicts:**
Check `docker-compose.yml` and ensure ports 5432 and 8080 are available.
