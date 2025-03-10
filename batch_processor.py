import sys
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor

# Get batch file from command-line argument
batch_file = sys.argv[1]
final_output = r"./Final_Result.xlsx"  # Directly merge into this file

# Initialize WebDriver (Headless mode for faster execution)
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--ignore-certificate-errors")
chrome_options.add_argument("--disable-web-security")

# Function to initialize and return the WebDriver
def init_driver():
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

# Function to process a single URL efficiently with retry logic
def process_url(url, driver, retries=3):
    attempt = 0
    while attempt < retries:
        try:
            driver.get(url)
            driver.set_page_load_timeout(240)  # Increased page load timeout
            WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body")))  # Increased WebDriverWait timeout
            page_source = driver.page_source.lower()
            body_text = driver.find_element(By.TAG_NAME, "body").text.lower()

            return {
                "Site URL": url,
                "Integrated Videos on Website": 'Yes' if check_for_videos(page_source) else 'No',
                "D2C Presence": 'Yes' if check_for_d2c(body_text) else 'No',
                "eCommerce Platform": 'Yes' if check_for_ecommerce(body_text) else 'No',
                "Social Media Presence": 'Yes' if check_for_social_presence(page_source) else 'No'
            }
        except TimeoutException:
            print(f"❌ Timeout error processing {url}. Retrying... {attempt + 1}/{retries}")
            attempt += 1
            time.sleep(5)
            if attempt == retries:
                return failed_result(url)
        except WebDriverException as e:
            print(f"❌ WebDriver error processing {url}: {e}")
            return failed_result(url)
        except Exception as e:
            print(f"❌ Error processing {url}: {e}")
            return failed_result(url)

# Function to return default failed result
def failed_result(url):
    return {
        "Site URL": url,
        "Integrated Videos on Website": 'No',
        "D2C Presence": 'No',
        "eCommerce Platform": 'No',
        "Social Media Presence": 'No'
    }

# Check if the page has videos (YouTube, Vimeo, HTML5 <video>)
def check_for_videos(page_source):
    return any(tag in page_source for tag in ["<video", "youtube.com", "vimeo.com"])

# Check for Direct-to-Consumer (D2C) presence
def check_for_d2c(body_text):
    keywords = ['shop', 'buy now', 'cart', 'checkout', 'products', 'store', 'order now', 'free shipping', 'add to cart', 'purchase', 'payment', 'shipping', 'delivery']
    return any(keyword in body_text for keyword in keywords)

# Check if the site uses an eCommerce platform
def check_for_ecommerce(body_text):
    keywords = ['shopify', 'woocommerce', 'bigcommerce', 'magento', 'cart', 'order', 'shop', 'checkout', 'store', 'products']
    return any(keyword in body_text for keyword in keywords)

# Check if the site has social media links
def check_for_social_presence(page_source):
    social_links = ['instagram.com', 'tiktok.com', 'facebook.com', 'twitter.com', 'linkedin.com']
    return any(link in page_source for link in social_links)

# Process each URL in the batch and merge into final file
def process_batch(batch_file):
    df = pd.read_excel(batch_file)
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(lambda row: process_url(row[1], init_driver()), df.itertuples()))

    df_results = pd.DataFrame(results)

    # Directly merge results into the final output file
    if os.path.exists(final_output):
        existing_df = pd.read_excel(final_output)
        final_df = pd.concat([existing_df, df_results], ignore_index=True)
    else:
        final_df = df_results

    final_df.to_excel(final_output, index=False)
    print(f"✅ Merged batch results into: {final_output}")

if __name__ == "__main__":
    process_batch(batch_file)
