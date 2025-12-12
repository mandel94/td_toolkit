# Quick Start Guide - Articles Analytics API

## 🚀 Start the API in 5 Minutes

### Step 1: Ensure Database is Running

```powershell
cd databases\articles_db
docker-compose up -d articles_postgres
```

Wait 10 seconds for the database to initialize.

### Step 2: Set Up Python Environment

```powershell
cd api

# Create virtual environment
python -m venv venv

# Activate it
.\venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Step 3: Configure Environment

```powershell
# Copy example environment file
copy .env.example .env

# No changes needed if using Docker defaults!
```

### Step 4: Start the API

```powershell
# Start with auto-reload
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

### Step 5: Test It!

Open your browser:
- **Interactive docs**: http://localhost:8000/docs
- **API root**: http://localhost:8000

Try an endpoint:
```powershell
# PowerShell
Invoke-WebRequest "http://localhost:8000/api/v1/analytics/top-articles?limit=5" | Select-Object -ExpandProperty Content
```

## 🎯 Quick Test Endpoints

### Get Top 10 Articles
```http
GET http://localhost:8000/api/v1/analytics/top-articles?limit=10
```

### Get Author Performance
```http
GET http://localhost:8000/api/v1/analytics/author-performance
```

### Get Category Performance
```http
GET http://localhost:8000/api/v1/analytics/category-performance
```

### Get All Authors (for filtering)
```http
GET http://localhost:8000/api/v1/dimensions/authors
```

## 📊 Example: Filter Top Articles by Author

1. Get author IDs:
```http
GET http://localhost:8000/api/v1/dimensions/authors
```

2. Filter articles by specific author:
```http
GET http://localhost:8000/api/v1/analytics/top-articles?author_id=5&limit=20
```

## 🐛 Troubleshooting

**Database connection error?**
- Check database is running: `docker ps`
- Verify credentials in `.env` file
- Ensure port 5432 is not blocked

**Import errors?**
- Activate virtual environment
- Reinstall requirements: `pip install -r requirements.txt`

**Port 8000 already in use?**
```powershell
# Use different port
uvicorn api.main:app --reload --port 8001
```

## 📚 Learn More

- Full documentation: `README.md`
- Business use cases: `../BUSINESS_USE_CASES.md`
- Database schema: Run ERD generation (see `../ERD_README.md`)

## ✨ What's Next?

Once the API is running:
1. ✅ Explore all endpoints in Swagger docs
2. ✅ Test with your actual data
3. ✅ Build a frontend dashboard
4. ✅ Add authentication
5. ✅ Deploy to production

---

**Happy coding! 🎉**
