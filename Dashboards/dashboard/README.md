# Editorial Analytics Dashboard

**Production-ready analytics dashboard for editorial teams**  
Built with Python, Dash, and Google Analytics 4 following 2025 best practices.

---

## 🎯 Overview

This dashboard provides editorial teams with actionable insights about page view trends, seasonality patterns, and performance metrics. It's designed to be **editor-friendly**, not technical.

### Key Features

- 📈 **Trend Analysis**: View page views over time with customizable granularity
- 📊 **Period Comparisons**: Compare WoW, MoM, or YoY automatically
- 🔄 **Seasonality Detection**: Identify weekly patterns
- 💡 **AI-Ready Insights**: Automatic textual explanations of trends
- 🎨 **Clean UI**: Editor-focused, minimal technical jargon
- ⚡ **Performance**: Cached data access for fast loading

---

## 🏗️ Architecture

This project follows **modern object-oriented design patterns**:

- **Facade Pattern**: Abstracts GA4 API complexity
- **Repository Pattern**: Isolates data access logic
- **Service Layer**: Handles business logic
- **Strategy Pattern**: Flexible time aggregation
- **Factory Pattern**: Component creation
- **MVC-inspired**: Clear separation in Dash callbacks

### Project Structure

```
dashboard/
├── app.py                          # Entry point
├── config/
│   └── settings.py                 # Configuration
├── data/
│   ├── ga4_client.py              # GA4 Facade
│   └── repositories.py            # Data repositories
├── services/
│   ├── analytics_service.py       # Business logic
│   └── trend_service.py           # Trend analysis
├── strategies/
│   └── aggregation.py             # Time aggregation strategies
├── ui/
│   ├── layout.py                  # Dashboard layout
│   ├── components.py              # Reusable UI components
│   └── callbacks.py               # Dash callbacks
├── insights/
│   └── insight_generator.py       # Textual insights
└── utils/
    └── date_utils.py              # Date utilities
```

---

## 🚀 Getting Started

### Prerequisites

- Python 3.11+
- Google Analytics 4 property
- GA4 service account credentials JSON

### Installation

1. **Clone or navigate to the dashboard directory**

```bash
cd dashboard
```

2. **Install dependencies**

```bash
pip install -r requirements.txt
```

3. **Configure environment variables**

Create a `.env` file:

```env
GA4_PROPERTY_ID=your_property_id
GA4_CREDENTIALS_PATH=path/to/credentials.json
DEBUG=False
HOST=127.0.0.1
PORT=8050
```

Or edit `config/settings.py` directly.

4. **Run the dashboard**

```bash
python app.py
```

5. **Open browser**

Navigate to `http://127.0.0.1:8050`

---

## 📊 Usage

### Date Selection
- Use the date picker to select your analysis period
- Default: last 90 days

### Granularity
- **Daily**: See day-by-day trends
- **Weekly**: View weekly aggregations
- **Monthly**: Analyze monthly patterns

### Comparisons
- **WoW**: Week-over-week comparison
- **MoM**: Month-over-month comparison
- **YoY**: Year-over-year comparison

### Interpreting Insights

The dashboard automatically generates insights:
- ✅ **Growth**: Positive trend detected
- ⚠️ **Decline**: Negative trend detected
- ➡️ **Stable**: Minimal variation

---

## 🔧 Configuration

### GA4 Configuration

Edit `config/settings.py`:

```python
@dataclass
class GA4Config:
    property_id: str = "YOUR_GA4_PROPERTY_ID"
    credentials_path: str = "path/to/credentials.json"
```

### Dashboard Settings

```python
@dataclass
class DashboardConfig:
    app_title: str = "Editorial Analytics Dashboard"
    default_date_range_days: int = 90
    moving_average_windows: dict = {"7d": 7, "14d": 14, "30d": 30}
```

---

## 🧪 Development

### Adding New Metrics

1. Update `services/analytics_service.py`
2. Add visualization in `ui/components.py`
3. Update callbacks in `ui/callbacks.py`

### Adding New Aggregation Strategies

Implement `AggregationStrategy` interface in `strategies/aggregation.py`:

```python
class CustomAggregation(AggregationStrategy):
    def aggregate(self, df: pd.DataFrame) -> pd.DataFrame:
        # Your logic here
        pass
```

### Extending Insights

Modify `insights/insight_generator.py` to add new insight types.

---

## 📦 Dependencies

- **dash**: Web framework
- **plotly**: Visualizations
- **pandas**: Data manipulation
- **google-analytics-data**: GA4 API client
- **google-auth**: Authentication

See `requirements.txt` for full list.

---

## 🎨 Design Principles (2025 Best Practices)

1. **Insight-driven, not metric-driven**: Show meaning, not just numbers
2. **Progressive disclosure**: Start simple, allow drilling down
3. **Human-centered**: Editor-friendly language
4. **AI-ready**: Structured for AI augmentation
5. **Performance-first**: Cached data, fast loads
6. **Transparent**: Clear data sources and definitions

---

## 🤝 Contributing

This is a production dashboard. For modifications:

1. Follow existing design patterns
2. Maintain separation of concerns
3. Add tests for new features
4. Update documentation

---

## 📝 License

Internal use - Havas / Fendi

---

## 🆘 Support

For issues or questions:
- Check configuration in `config/settings.py`
- Verify GA4 credentials
- Review console logs for errors

---

**Built with ❤️ following 2025 analytics best practices**
