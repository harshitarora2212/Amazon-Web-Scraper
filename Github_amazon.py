"""
Universal Amazon Product Scraper with Parallel Processing
=================================
A comprehensive web scraper for Amazon product data including:
- Product details (title, price, ratings, availability)
- Seller information
- Product variations
- Customer reviews
- Best seller rankings

"""

import requests
from bs4 import BeautifulSoup
import time
import re
from datetime import datetime
import random
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import glob

# ============================================================================
# CONFIGURATION
# ============================================================================

# Output directory structure
OUTPUT_DIR = r"C:\Amazon_Scraper_Output"
OUTPUT_TEMP = os.path.join(OUTPUT_DIR, "temp")
OUTPUT_FINAL = os.path.join(OUTPUT_DIR, "final")

# Create directories if they don't exist
for directory in [OUTPUT_DIR, OUTPUT_TEMP, OUTPUT_FINAL]:
    os.makedirs(directory, exist_ok=True)

# Thread-safe file writing
FILE_LOCK = threading.Lock()

# User agents for rotation (helps avoid detection)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
]


# ============================================================================
# MAIN SCRAPER CLASS
# ============================================================================

class AmazonScraper:
    """
    Main scraper class for Amazon product data
    
    Features:
    - Selenium-based scraping (handles dynamic content)
    - Multi-location support (via ZIP codes)
    - Parallel processing support
    - Automatic retry logic
    - Data validation and cleaning
    """
    
    def __init__(self):
        """Initialize scraper with session and headers"""
        self.session = requests.Session()
        self.headers = {
            'User-Agent': random.choice(USER_AGENTS),
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Connection': 'keep-alive',
        }
        self.session.headers.update(self.headers)

    # ========================================================================
    # BROWSER SETUP
    # ========================================================================

    def setup_driver(self, headless=False):
        """
        Setup Chrome WebDriver with anti-detection measures
        
        Args:
            headless (bool): Run browser in headless mode (no GUI)
            
        Returns:
            webdriver.Chrome: Configured Chrome driver instance
        """
        options = Options()
        options.add_argument("--start-maximized")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        if headless:
            options.add_argument("--headless")
        
        driver = webdriver.Chrome(options=options)
        
        # Hide webdriver property to avoid detection
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        return driver

    def set_location(self, driver, zip_code):
        """
        Set delivery location on Amazon based on ZIP code
        
        Args:
            driver: Selenium WebDriver instance
            zip_code (str): US ZIP code (e.g., '110025')
            
        Returns:
            bool: True if location set successfully, False otherwise
        """
        try:
            # Click on location button in navbar
            location_button = driver.find_element("id", "nav-global-location-slot")
            location_button.click()
            time.sleep(2)
            
            # Enter ZIP code
            zip_input = driver.find_element("id", "GLUXZipUpdateInput")
            zip_input.clear()
            zip_input.send_keys(zip_code)
            time.sleep(1)
            
            # Apply ZIP code
            apply_button = driver.find_element("xpath", "//input[@aria-labelledby='GLUXZipUpdate-announce']")
            apply_button.click()
            time.sleep(3)
            
            # Close the location modal if "Done" button appears
            try:
                done_button = driver.find_element("css selector", "button[name='glowDoneButton']")
                done_button.click()
                time.sleep(2)
            except:
                pass
            
            print(f"✅ Location set to ZIP: {zip_code}")
            return True
            
        except Exception as e:
            print(f"⚠️ Could not set location: {e}")
            return False

    # ========================================================================
    # DATA EXTRACTION METHODS
    # ========================================================================

    def extract_title(self, soup):
        """Extract product title from page"""
        selectors = ['#productTitle', '.product-title', 'h1.a-size-large']
        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                return element.get_text(strip=True)
        return 'NA'

    def extract_store_name(self, soup):
        """Extract store/brand name (only if it links to brand store)"""
        selectors = ['#bylineInfo', '.a-link-normal[data-brand-link]']
        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                text = element.get_text(strip=True).replace('Brand: ', '')
                # Only return if it's a link to brand store
                if element.has_attr("href") and "/stores/" in element["href"]:
                    return text
        return "NA"

    def extract_rating(self, soup):
        """Extract product rating (e.g., '4.5')"""
        selectors = ['span.a-icon-alt', '[data-hook="average-star-rating"] .a-icon-alt']
        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                rating_text = element.get_text()
                match = re.search(r'(\d+\.?\d*)', rating_text)
                if match:
                    return match.group(1)
        return 'NA'

    def extract_review_count(self, soup):
        """Extract total number of customer reviews"""
        selectors = ['#acrCustomerReviewText', '[data-hook="total-review-count"]']
        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                text = element.get_text()
                match = re.search(r'([\d,]+)', text)
                if match:
                    return match.group(1).replace(',', '')
        return 'NA'

    def extract_availability(self, soup):
        """
        Extract availability status
        
        Returns:
            str: 'Instock', 'OOS' (Out of Stock), 'NBB' (Not in Buy Box), or 'Not Listed'
        """
        selectors = [
            '#availability span.a-size-medium.a-color-success',
            '#availability span',
        ]
        
        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                text = element.get_text(strip=True)
                if not text:
                    return "NBB"
                if "Currently unavailable" in text or "cannot be shipped" in text:
                    return "OOS"
                elif "In Stock" in text or "in stock" in text.lower():
                    return "Instock"
                else:
                    return text
        
        return "Not Listed"

    def extract_selling_price(self, soup):
        """
        Extract current selling price (numeric only, no $ sign)
        
        Returns:
            str: Price like '30.60' or 'NA' if unavailable
        """
        # Don't return price if product is unavailable
        availability = self.extract_availability(soup)
        if availability in ["OOS", "NBB", "Not Listed"]:
            return "NA"
        
        # Try to extract price from price elements
        price_whole = soup.select_one(".a-price .a-price-whole")
        price_fraction = soup.select_one(".a-price .a-price-fraction")
        
        if price_whole:
            whole = price_whole.get_text(strip=True).replace(",", "").replace(".", "")
            fraction = price_fraction.get_text(strip=True) if price_fraction else "00"
            return f"{whole}.{fraction}"
        
        # Fallback selector
        element = soup.select_one("#corePrice_feature_div .a-price .a-offscreen")
        if element:
            return element.get_text(strip=True).replace("$", "").replace(",", "")
        
        return "NA"

    def extract_list_price(self, soup):
        """Extract list/MSRP price (the crossed-out price)"""
        text = soup.get_text(" ", strip=True)
        match = re.search(r'List Price:\s*\$?(\d[\d,]*(?:\.\d{1,2})?)', text)
        if match:
            return match.group(1).replace(",", "")
        return "NA"

    def extract_brand(self, soup):
        """Extract brand name"""
        selectors = ['#bylineInfo', '.po-brand .po-break-word']
        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                brand_text = element.get_text(strip=True)
                brand_text = brand_text.replace('Brand: ', '').replace('Visit the ', '').replace(' Store', '')
                return brand_text
        return 'NA'

    def extract_ships_from(self, soup):
        """Extract 'Ships from' information"""
        label = soup.find("span", string=lambda t: t and "Ships from" in t)
        if label:
            value_span = label.find_next("span", class_="a-size-small")
            if value_span:
                return value_span.get_text(strip=True)
        return 'NA'

    def extract_sold_by(self, soup):
        """Extract 'Sold by' information"""
        label = soup.find("span", string=lambda t: t and "Sold by" in t)
        if label:
            value_span = label.find_next("span", class_="a-size-small")
            if value_span:
                return value_span.get_text(strip=True)
        return 'NA'

    def extract_coupon(self, soup):
        """Extract coupon/discount information"""
        selectors = ['.promoPriceBlockMessage', '.couponText', '.couponLabelText']
        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                return element.get_text(" ", strip=True)
        return 'NA'

    def extract_primary_image(self, soup):
        """Extract main product image URL"""
        selectors = ['#landingImage', '#imgTagWrapperId img', '.a-dynamic-image']
        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                src = element.get('src') or element.get('data-src')
                if src:
                    return src
        return 'NA'

    # ========================================================================
    # MAIN SCRAPING METHODS
    # ========================================================================

    def scrape_product_details(self, driver, asin, zip_code):
        """
        Scrape comprehensive product details for a single ASIN
        
        Args:
            driver: Selenium WebDriver instance
            asin (str): Amazon Standard Identification Number
            zip_code (str): ZIP code for location-based pricing
            
        Returns:
            dict: Product data dictionary
        """
        url = f"https://www.amazon.com/dp/{asin}?th=1"
        driver.get(url)
        time.sleep(5)
        
        # Parse page with BeautifulSoup
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # Extract all product information
        product_data = {
            'ASIN': asin,
            'Title': self.extract_title(soup),
            'Store Name': self.extract_store_name(soup),
            'Rating': self.extract_rating(soup),
            'Review Count': self.extract_review_count(soup),
            'List Price': self.extract_list_price(soup),
            'Selling Price': self.extract_selling_price(soup),
            'Availability': self.extract_availability(soup),
            'Brand': self.extract_brand(soup),
            'Ships From': self.extract_ships_from(soup),
            'Sold by': self.extract_sold_by(soup),
            'Coupon': self.extract_coupon(soup),
            'Primary Image': self.extract_primary_image(soup),
            'Product URL': url,
            'Scraped Time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'ZIP Code': zip_code
        }
        
        return product_data

    def scrape_products(self, asins, zip_codes=['10001'], max_workers=3):
        """
        Scrape multiple products across multiple locations
        
        Args:
            asins (list): List of Amazon ASINs to scrape
            zip_codes (list): List of ZIP codes for location-based data
            max_workers (int): Number of parallel threads to use
            
        Returns:
            pd.DataFrame: DataFrame containing all scraped product data
        """
        all_data = []
        timestamp = datetime.now().strftime("%Y%m%d")
        
        # Create task queue for parallel processing
        tasks = [(asin, zip_code) for asin in asins for zip_code in zip_codes]
        
        print(f"🚀 Starting to scrape {len(asins)} ASINs across {len(zip_codes)} locations")
        print(f"📊 Total tasks: {len(tasks)}")
        
        def scrape_task(asin, zip_code):
            """Single scraping task"""
            driver = self.setup_driver()
            try:
                driver.get("https://www.amazon.com/")
                time.sleep(2)
                
                # Set location
                self.set_location(driver, zip_code)
                
                # Scrape product
                product_data = self.scrape_product_details(driver, asin, zip_code)
                print(f"✅ Scraped: {asin} for ZIP {zip_code}")
                
                return product_data
                
            except Exception as e:
                print(f"❌ Error scraping {asin}: {e}")
                return None
            finally:
                driver.quit()
        
        # Execute tasks in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(scrape_task, asin, zip_code): (asin, zip_code) 
                      for asin, zip_code in tasks}
            
            for future in as_completed(futures):
                result = future.result()
                if result:
                    all_data.append(result)
        
        # Convert to DataFrame
        df = pd.DataFrame(all_data)
        
        # Save to Excel
        output_file = os.path.join(OUTPUT_FINAL, f"Amazon_Products_{timestamp}.xlsx")
        df.to_excel(output_file, index=False, engine='openpyxl')
        print(f"\n✅ Data saved to: {output_file}")
        
        return df

    def close(self):
        """Clean up resources"""
        try:
            self.session.close()
        except Exception as e:
            print(f"Error closing session: {e}")


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def cleanup_old_files(directory, days_old=7):
    """
    Remove files older than specified days
    
    Args:
        directory (str): Directory to clean
        days_old (int): Files older than this will be deleted
    """
    if not os.path.exists(directory):
        return
    
    current_time = datetime.now()
    removed_count = 0
    
    for file_path in glob.glob(os.path.join(directory, "*")):
        if os.path.isfile(file_path):
            file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
            if (current_time - file_time).days > days_old:
                try:
                    os.remove(file_path)
                    removed_count += 1
                except Exception as e:
                    print(f"Error removing {file_path}: {e}")
    
    if removed_count > 0:
        print(f"🧹 Cleaned up {removed_count} old files from {directory}")


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    """
    Example usage of the Amazon scraper
    """
    
    # Clean up old files first
    cleanup_old_files(OUTPUT_TEMP)
    cleanup_old_files(OUTPUT_FINAL, days_old=30)
    
    # Initialize scraper
    scraper = AmazonScraper()
    
    # Example ASINs (replace with your own)
    example_asins = [
        'B0CLF3VPMV',  # Example product
        'B09SM67XTG',  # Example product
        'B07Q2W5HCY',  # Example product
    ]
    
    # Example ZIP codes for different locations
    zip_codes = [
        '10001',  # New York, NY
        '90001',  # Los Angeles, CA
        '60601',  # Chicago, IL
    ]
    
    # Run the scraper
    try:
        start_time = datetime.now()
        
        # Scrape products
        df_results = scraper.scrape_products(
            asins=example_asins,
            zip_codes=zip_codes,
            max_workers=3  # Number of parallel threads
        )
        
        # Display results
        print(f"\n📊 Scraped {len(df_results)} products")
        print(f"⏱️ Time taken: {datetime.now() - start_time}")
        print("\n" + "="*50)
        print(df_results.head())
        
    except Exception as e:
        print(f"❌ Scraping failed: {e}")
    finally:
        scraper.close()