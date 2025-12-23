# Amazon-Web-Scraper
A high-performance, multi-threaded Amazon product scraper built with Python, Selenium, and BeautifulSoup. Features intelligent parallel processing to scrape multiple products across different locations simultaneously, dramatically reducing scraping time.

## 🚀 Key Features
# ⚡ Parallel Processing Engine

Multi-threaded architecture using ThreadPoolExecutor for concurrent scraping
Configurable worker threads - scale from 1 to N parallel operations
Automatic task distribution across available workers
3-10x faster than sequential scraping depending on network conditions

# 📊 Comprehensive Data Extraction

Product titles, descriptions, and specifications
Real-time pricing (List Price, Selling Price, Discounts)
Customer ratings and review counts
Availability status (In Stock, Out of Stock, etc.)
Seller information (Ships From, Sold By)
Coupon and promotional data
High-resolution product images

# 🗺️ Multi-Location Support

Scrape products across multiple ZIP codes simultaneously
Compare pricing and availability by location
Support for any US ZIP code
Automatic location switching per thread
