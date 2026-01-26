"""CLI for running flows and management tasks."""

import asyncio
from datetime import date, timedelta

import typer
from rich.console import Console
from rich.table import Table

from movie_trends.orchestration.flows import (
    backfill_trends_flow,
    daily_ingestion_flow,
    full_pipeline_flow,
    weekly_trends_flow,
)

app = typer.Typer(help="Movie Trends API Management CLI")
console = Console()


@app.command()
def ingest_daily():
    """Run daily ingestion flow."""
    console.print("[bold blue]Running daily ingestion flow...[/bold blue]")
    result = asyncio.run(daily_ingestion_flow())
    console.print("[bold green]✓ Daily ingestion completed[/bold green]")
    console.print(result)


@app.command()
def calculate_trends(
    target_date: str = typer.Option(
        None,
        help="Target date (YYYY-MM-DD). Defaults to last week.",
    ),
):
    """Calculate weekly trends."""
    console.print("[bold blue]Calculating weekly trends...[/bold blue]")
    
    date_obj = None
    if target_date:
        date_obj = date.fromisoformat(target_date)
    
    result = asyncio.run(weekly_trends_flow(date_obj))
    console.print("[bold green]✓ Trends calculated[/bold green]")
    console.print(result)


@app.command()
def run_pipeline(
    time_window: str = typer.Option("week", help="Time window: 'day' or 'week'"),
):
    """Run full pipeline (ingest + calculate)."""
    console.print("[bold blue]Running full pipeline...[/bold blue]")
    result = asyncio.run(full_pipeline_flow(ingest_time_window=time_window))
    console.print("[bold green]✓ Pipeline completed[/bold green]")
    console.print(result)


@app.command()
def backfill(
    start_date: str = typer.Argument(..., help="Start date (YYYY-MM-DD)"),
    end_date: str = typer.Option(None, help="End date (YYYY-MM-DD)"),
):
    """Backfill trends for historical weeks."""
    console.print("[bold blue]Running backfill...[/bold blue]")
    
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date) if end_date else None
    
    result = asyncio.run(backfill_trends_flow(start, end))
    console.print("[bold green]✓ Backfill completed[/bold green]")
    console.print(result)


@app.command()
def init_db():
    """Initialize database schema."""
    from movie_trends.database import init_db
    
    console.print("[bold blue]Initializing database...[/bold blue]")
    asyncio.run(init_db())
    console.print("[bold green]✓ Database initialized[/bold green]")


if __name__ == "__main__":
    app()
