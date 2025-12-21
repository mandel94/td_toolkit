# 🚀 Quick Start: Launch Documentation in 60 Seconds

Follow these simple steps to view your interactive Movie Trends API documentation.

---

## Step 1: Navigate to Docs Directory

```powershell
cd "C:\Users\manuel.deluzi\OneDrive - Havas\Projects\personali\taxi_drivers\APIs\movie_trends_api\docs"
```

---

## Step 2: Start Docker Container

```powershell
docker-compose up -d
```

**Expected Output:**
```
Creating network "movie-trends-docs-network" ... done
Creating movie-trends-docs ... done
```

---

## Step 3: Open in Browser

```powershell
start http://localhost:8001
```

Or manually navigate to: **http://localhost:8001**

---

## 🎉 That's It!

You should now see the full documentation site with:

- 🏠 **Homepage** - Overview and quick links
- 🏗️ **Architecture** - Interactive system diagrams
- 📊 **Data Flow** - Visual data pipeline
- 🔢 **Trend Calculation** - Formula breakdown with math
- 🐳 **Docker Guide** - Deployment instructions
- 📡 **API Reference** - Complete endpoint documentation

---

## Verify It's Running

Check container status:

```powershell
docker ps | Select-String "movie-trends-docs"
```

Should show:
```
movie-trends-docs   Up 2 minutes   0.0.0.0:8001->8001/tcp
```

---

## View Logs

```powershell
docker-compose logs -f
```

Press `Ctrl+C` to exit logs.

---

## Stop Documentation

When you're done:

```powershell
docker-compose down
```

---

## Troubleshooting

### Port 8001 Already in Use?

Edit `docker-compose.yml` and change the port:

```yaml
ports:
  - "8002:8001"  # Changed from 8001
```

Then use: http://localhost:8002

### Container Won't Start?

Check logs for errors:

```powershell
docker-compose logs
```

### Docker Not Running?

Start Docker Desktop first, then try again.

---

## Next Steps

- Explore the [Architecture](architecture/overview.md) section
- Learn about [Trend Calculation](how-it-works/trend-calculation.md)
- Check out [API Endpoints](api/endpoints.md)

---

**Happy Exploring! 🎬**
