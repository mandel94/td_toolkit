"""
API Testing Script - Best Practices with httpx
Tests all endpoints of the Articles Analytics API
"""
import httpx
import sys
from datetime import date, timedelta

# Base URL for the API
BASE_URL = "http://localhost:8000"

def print_section(title: str):
    """Print a formatted section header"""
    print("\n" + "="*60)
    print(f"  {title}")
    print("="*60)

def print_response(endpoint: str, response: httpx.Response):
    """Print formatted response details"""
    print(f"\n🔗 Endpoint: {endpoint}")
    print(f"📊 Status Code: {response.status_code}")
    if response.status_code == 200:
        print("✅ Success")
        data = response.json()
        if isinstance(data, list):
            print(f"📦 Results: {len(data)} items")
            if data:
                print(f"📄 First item: {data[0]}")
        elif isinstance(data, dict):
            print(f"📄 Response: {data}")
    else:
        print(f"❌ Error: {response.text}")

def test_health_check():
    """Test health check endpoint"""
    print_section("Health Check")
    with httpx.Client() as client:
        response = client.get(f"{BASE_URL}/health")
        print_response("/health", response)

def test_top_articles():
    """Test top articles endpoint"""
    print_section("Top Articles")
    
    # Calculate date range (last 4 weeks)
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(weeks=4)
    
    with httpx.Client() as client:
        # Test 1: Basic request
        print("\n📌 Test 1: Top 10 articles")
        response = client.get(
            f"{BASE_URL}/api/v1/analytics/top-articles",
            params={
                "start_date": str(start_date),
                "end_date": str(end_date),
                "limit": 10
            }
        )
        print_response("/api/v1/analytics/top-articles", response)
        
        # Test 2: Filter by category
        print("\n📌 Test 2: Top articles in 'News' category")
        response = client.get(
            f"{BASE_URL}/api/v1/analytics/top-articles",
            params={
                "start_date": str(start_date),
                "end_date": str(end_date),
                "category": "News",
                "limit": 5
            }
        )
        print_response("/api/v1/analytics/top-articles?category=News", response)

def test_author_performance():
    """Test author performance endpoint"""
    print_section("Author Performance")
    
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(weeks=4)
    
    with httpx.Client() as client:
        print("\n📌 Top 10 authors by performance")
        response = client.get(
            f"{BASE_URL}/api/v1/analytics/author-performance",
            params={
                "start_date": str(start_date),
                "end_date": str(end_date),
                "limit": 10
            }
        )
        print_response("/api/v1/analytics/author-performance", response)

def test_category_performance():
    """Test category performance endpoint"""
    print_section("Category Performance")
    
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(weeks=4)
    
    with httpx.Client() as client:
        response = client.get(
            f"{BASE_URL}/api/v1/analytics/category-performance",
            params={
                "start_date": str(start_date),
                "end_date": str(end_date)
            }
        )
        print_response("/api/v1/analytics/category-performance", response)

def test_engagement_trends():
    """Test engagement trends endpoint"""
    print_section("Engagement Trends")
    
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(weeks=8)
    
    with httpx.Client() as client:
        print("\n📌 Overall engagement trends")
        response = client.get(
            f"{BASE_URL}/api/v1/analytics/engagement-trends",
            params={
                "start_date": str(start_date),
                "end_date": str(end_date),
                "granularity": "week"
            }
        )
        print_response("/api/v1/analytics/engagement-trends", response)

def test_authors_list():
    """Test authors list endpoint"""
    print_section("Authors List")
    
    with httpx.Client() as client:
        response = client.get(f"{BASE_URL}/api/v1/authors")
        print_response("/api/v1/authors", response)

def test_categories_list():
    """Test categories list endpoint"""
    print_section("Categories List")
    
    with httpx.Client() as client:
        response = client.get(f"{BASE_URL}/api/v1/categories")
        print_response("/api/v1/categories", response)

def test_article_detail():
    """Test article detail endpoint"""
    print_section("Article Detail")
    
    # First, get a sample article ID from top articles
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(weeks=4)
    
    with httpx.Client() as client:
        # Get top articles first
        response = client.get(
            f"{BASE_URL}/api/v1/analytics/top-articles",
            params={
                "start_date": str(start_date),
                "end_date": str(end_date),
                "limit": 1
            }
        )
        
        if response.status_code == 200:
            articles = response.json()
            if articles:
                article_id = articles[0]["article_id"]
                print(f"\n📌 Getting details for article ID: {article_id}")
                
                # Get article details
                detail_response = client.get(f"{BASE_URL}/api/v1/articles/{article_id}")
                print_response(f"/api/v1/articles/{article_id}", detail_response)
            else:
                print("⚠️ No articles found to test detail endpoint")
        else:
            print("⚠️ Could not retrieve sample article for testing")

def test_error_handling():
    """Test error handling"""
    print_section("Error Handling")
    
    with httpx.Client() as client:
        # Test 1: Invalid date range
        print("\n📌 Test 1: Invalid date range (start > end)")
        response = client.get(
            f"{BASE_URL}/api/v1/analytics/top-articles",
            params={
                "start_date": "2025-12-01",
                "end_date": "2025-11-01"
            }
        )
        print_response("/api/v1/analytics/top-articles (invalid dates)", response)
        
        # Test 2: Non-existent article
        print("\n📌 Test 2: Non-existent article")
        response = client.get(f"{BASE_URL}/api/v1/articles/999999")
        print_response("/api/v1/articles/999999", response)

def run_all_tests():
    """Run all API tests"""
    print("\n" + "🚀 ARTICLES ANALYTICS API - TEST SUITE ".center(60, "="))
    print(f"Base URL: {BASE_URL}")
    
    try:
        # Check if server is running
        with httpx.Client() as client:
            try:
                client.get(BASE_URL, timeout=2.0)
            except httpx.ConnectError:
                print("\n❌ ERROR: Cannot connect to API server")
                print(f"   Please ensure the server is running at {BASE_URL}")
                print("   Run: uvicorn main:app --reload")
                sys.exit(1)
        
        # Run all tests
        test_health_check()
        test_authors_list()
        test_categories_list()
        test_top_articles()
        test_author_performance()
        test_category_performance()
        test_engagement_trends()
        test_article_detail()
        test_error_handling()
        
        print("\n" + "✅ TEST SUITE COMPLETED ".center(60, "="))
        print("\n💡 View interactive API docs at: http://localhost:8000/docs")
        
    except Exception as e:
        print(f"\n❌ Test suite failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    run_all_tests()
