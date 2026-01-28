# 🚀 Quick Setup Guide

## Step-by-Step Installation

### 1. Prerequisites Check
✅ Python 3.11+ installed  
✅ GA4 property access  
✅ Service account credentials JSON  

### 2. Navigate to Dashboard Directory
```bash
cd "c:\Users\manuel.deluzi\OneDrive - Havas\Reporting\FENDI\Dashboards\dashboard"
```

### 3. Create Virtual Environment (Recommended)
```bash
python -m venv venv
venv\Scripts\activate
```

### 4. Install Dependencies
```bash
pip install -r requirements.txt
```

### 5. Configure GA4 Credentials

**Option A: Environment Variables**
```bash
# Copy example file
copy .env.example .env

# Edit .env and set:
# GA4_PROPERTY_ID=your_property_id
# GA4_CREDENTIALS_PATH=path/to/credentials.json
```

**Option B: Direct Configuration**

Edit `config/settings.py`:
```python
@dataclass
class GA4Config:
    property_id: str = "YOUR_ACTUAL_PROPERTY_ID"
    credentials_path: str = r"C:\path\to\credentials.json"
```

### 6. Run Dashboard

**Windows (Easy Way)**:
```bash
start_dashboard.bat
```

**Manual Way**:
```bash
python app.py
```

### 7. Access Dashboard
Open browser: `http://127.0.0.1:8050`

---

## ⚠️ Troubleshooting

### Error: "No module named 'dash'"
**Solution**: Install dependencies
```bash
pip install -r requirements.txt
```

### Error: "GA4 authentication failed"
**Solutions**:
1. Check `GA4_PROPERTY_ID` is correct
2. Verify credentials JSON path is absolute
3. Ensure service account has Analytics Reader role

### Error: "No data available"
**Solutions**:
1. Check GA4 property has data for selected period
2. Verify date range is valid
3. Check GA4 API quotas

### Dashboard loads but shows errors
**Solution**: Check browser console (F12) for JavaScript errors

### Slow loading
**Solution**: Use smaller date ranges initially

---

## 🔍 Verifying Installation

### Test 1: Import Check
```bash
python -c "import dash, pandas, plotly; print('All imports OK')"
```

### Test 2: Configuration Check
```bash
python -c "from config import ga4_config; print(f'Property: {ga4_config.property_id}')"
```

### Test 3: GA4 Connection Test
```python
from data import GA4ClientFacade
from config import ga4_config

client = GA4ClientFacade(
    ga4_config.property_id,
    ga4_config.credentials_path
)

if client.test_connection():
    print("✅ GA4 connection successful!")
else:
    print("❌ GA4 connection failed")
```

---

## 📝 Configuration Checklist

- [ ] Python 3.11+ installed
- [ ] Virtual environment created and activated
- [ ] Dependencies installed (`pip install -r requirements.txt`)
- [ ] GA4 property ID configured
- [ ] Service account credentials JSON accessible
- [ ] Service account has Analytics Reader role in GA4
- [ ] `.env` file created (if using environment variables)
- [ ] Dashboard runs without errors
- [ ] Can access dashboard at http://127.0.0.1:8050
- [ ] Data loads when clicking "Aggiorna Dati"

---

## 🎯 Next Steps After Setup

1. **Test with sample data**: Select last 30 days
2. **Explore granularity options**: Daily → Weekly → Monthly
3. **Try comparisons**: WoW, MoM, YoY
4. **Read generated insights**: Check AI-generated text
5. **Analyze seasonality**: Look at weekly patterns

---

## 🆘 Getting Help

If you encounter issues:
1. Check this guide first
2. Review `ARCHITECTURE.md` for technical details
3. Check `README.md` for usage information
4. Review console output for error messages
5. Check GA4 API quotas in Google Cloud Console

---

**Dashboard Version**: 1.0.0  
**Last Updated**: December 2025
