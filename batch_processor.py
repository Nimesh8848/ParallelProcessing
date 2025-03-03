import sys
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import os

# Get batch file from command-line argument
batch_file = sys.argv[1]

# Initialize WebDriver (Headless mode for faster execution)
chrome_options = Options()
chrome_options.add_argument("--headless")
try:
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    print(f"🚀 Processing: {batch_file}")
except Exception as e:
    print(f"❌ WebDriver error: {e}")
    sys.exit(1)

# Load batch data
df = pd.read_excel(batch_file)

# Function to check for videos on the website
def check_for_videos(url):
    try:
        driver.get(url)
        time.sleep(3)
        return bool(driver.find_elements(By.CSS_SELECTOR, "video, iframe[src*='youtube.com'], iframe[src*='vimeo.com']"))
    except:
        return False

# Function to check for D2C presence
def check_for_d2c(url):
    try:
        driver.get(url)
        time.sleep(3)
        keywords = ['shop', 'buy now', 'cart', 'checkout', 'products', 'store', 'order now', 'free shipping']
        page_text = driver.find_element(By.TAG_NAME, "body").text.lower()
        return any(keyword in page_text for keyword in keywords)
    except:
        return False

# Function to check for eCommerce platform usage
def check_for_ecommerce(url):
    try:
        driver.get(url)
        time.sleep(3)
        keywords = ['shopify', 'woocommerce', 'bigcommerce', 'magento', 'cart', 'order']
        page_text = driver.find_element(By.TAG_NAME, "body").text.lower()
        return any(keyword in page_text for keyword in keywords)
    except:
        return False

# Function to check for social media presence
def check_for_social_presence(url):
    try:
        driver.get(url)
        time.sleep(3)
        social_links = ['instagram.com', 'tiktok.com', 'facebook.com', 'twitter.com', 'linkedin.com']
        return any(link in driver.page_source for link in social_links)
    except:
        return False

# Process each row in the batch
for index, row in df.iterrows():
    site_url = row['Site URL']
    df.at[index, 'Integrated Videos on Website'] = 'Yes' if check_for_videos(site_url) else 'No'
    df.at[index, 'D2C Presence'] = 'Yes' if check_for_d2c(site_url) else 'No'
    df.at[index, 'eCommerce Platform'] = 'Yes' if check_for_ecommerce(site_url) else 'No'
    df.at[index, 'Social Media Presence'] = 'Yes' if check_for_social_presence(site_url) else 'No'

# Save updated batch file
df.to_excel(batch_file, index=False)
print(f"✅ Finished processing: {batch_file}")

# Close WebDriver
driver.quit()