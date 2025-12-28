# 🧠 Prompt for Copilot

## Build an Editorial Analytics Dashboard (GA4 → Dash, Python, OOP – 2025 Best Practices)

You are an expert **Python Data Engineer & Analytics Product Developer (2025)**.
Your task is to design and implement a **production-ready analytics dashboard** for an **editorial team**, following **modern object-oriented design patterns**, clean architecture principles, and current best practices.

---

## 🎯 Product Context

- **Domain:** Digital magazine / editorial analytics
- **Users:** Editors, caporedattori, content strategists
- **Primary Question:**
  > *What is the trend of page views over a selected time period, and how should editors interpret it?*
  >
- **Data Source:** Google Analytics 4 (GA4)
- **Tech Stack:**
  - Python 3.11+
  - Dash (Plotly)
  - pandas / numpy
  - GA4 Data API

---

## 🧩 Functional Requirements

The dashboard must:

1. Show **page view trends over time**
2. Allow **date range selection**
3. Support **multiple time granularities** (daily / weekly / monthly)
4. Display **smoothed trends** (moving averages)
5. Enable **period-over-period comparison** (WoW / MoM / YoY)
6. Highlight **seasonality patterns**
7. Include an **automatic textual insight** describing the trend
8. Be **editor-friendly**, not technical

---

## 🧱 Architectural Requirements (Critical)

Use **Object-Oriented Design** with **clear separation of concerns**.

### Mandatory Design Patterns (2025 best practices)

You MUST apply the following patterns where appropriate:

- **Facade** – to abstract GA4 API complexity
- **Repository** – to isolate data access logic
- **Service Layer** – for business logic and transformations
- **Strategy** – for time aggregation logic (daily / weekly / monthly)
- **Factory** – to instantiate visualization components
- **MVC / MVVM-inspired separation** for Dash layout & callbacks

---

## 🗂️ Required Project Structure

Use a modular, scalable structure:

```text
dashboard/
│
├── app.py                     # Dash entry point
├── config/
│   └── settings.py            # GA4, app config
│
├── data/
│   ├── ga4_client.py          # GA4 Facade
│   ├── repositories.py        # Data repositories
│
├── services/
│   ├── analytics_service.py   # Business logic
│   ├── trend_service.py       # Trend & smoothing logic
│
├── strategies/
│   ├── aggregation.py         # Daily / Weekly / Monthly strategies
│
├── ui/
│   ├── layout.py              # Dash layout
│   ├── components.py          # Reusable UI components
│   ├── callbacks.py           # Dash callbacks
│
├── insights/
│   └── insight_generator.py   # Textual insights (AI-ready)
│
└── utils/
    └── date_utils.py
```
