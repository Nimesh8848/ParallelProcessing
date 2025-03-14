import pandas as pd
import os
import time
import concurrent.futures
import multiprocessing
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

# File paths
input_file = r"./URL Checker.xlsx"
batch_folder = r"./Batches"
final_output = r"./Final_Result.xlsx"

# Ensure batch folder exists
os.makedirs(batch_folder, exist_ok=True)

# Load Excel file
df = pd.read_excel(input_file)
batch_size = 12  # Number of URLs per batch
num_batches = (len(df) // batch_size) + (1 if len(df) % batch_size > 0 else 0)  # Calculate total batches

# Split the data into batches and save them
batch_files = []
for i in range(num_batches):
    batch_data = df[i * batch_size: (i + 1) * batch_size]  # Extract batch
    batch_file = f"{batch_folder}/Batch_{i + 1}.xlsx"
    batch_data.to_excel(batch_file, index=False)
    batch_files.append(batch_file)
    print(f"✅ Saved: {batch_file}")

# Function to initialize WebDriver
def init_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("window-size=1200x600")
    options.page_load_strategy = 'eager'
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# Function to check for social media presence and extract Instagram and TikTok links
def check_for_social_presence(driver):
    try:
        social_links = driver.find_elements(By.TAG_NAME, 'a')
        instagram_links = []
        tiktok_links = []
        social_presence = False

        for link in social_links:
            href = link.get_attribute('href')
            if href:
                if 'instagram.com' in href:
                    instagram_links.append(href)
                    social_presence = True
                elif 'tiktok.com' in href:
                    tiktok_links.append(href)
                    social_presence = True

        instagram_link = instagram_links[0] if instagram_links else None
        tiktok_link = tiktok_links[0] if tiktok_links else None

        return social_presence, instagram_link, tiktok_link
    except Exception as e:
        print(f"Error checking for social presence: {e}")
        return False, None, None

# Function to check website features
def check_website_features(driver, url):
    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        page_content = driver.page_source.lower()

        has_videos = bool(driver.find_elements(By.CSS_SELECTOR, "video, iframe[src*='youtube.com'], iframe[src*='vimeo.com']"))
        
        d2c_keywords = ['shop', 'buy now', 'cart', 'checkout', 'order now', 'order online', 'free shipping', 'subscribe', 'direct buy', 'special offers', 'limited edition', 'shop now', 'store']
        has_d2c = any(keyword in page_content for keyword in d2c_keywords)
        
        ecommerce_keywords = ['shopify', 'woocommerce', 'bigcommerce', 'magento', 'shop', 'order', 'cart', 'checkout', 'buy now', 'free shipping', 'order now', 'pay now', 'sale', 'ecommerce', 'online store', 'store', 'marketplace', 'paypal', 'stripe']
        has_ecommerce = any(keyword in page_content for keyword in ecommerce_keywords)
        
        social_media_links = ['instagram.com', 'tiktok.com', 'facebook.com', 'twitter.com', 'linkedin.com']
        has_social_media = any(link in page_content for link in social_media_links)

        social_presence, instagram, tiktok = check_for_social_presence(driver)

        return has_videos, has_d2c, has_ecommerce, has_social_media, social_presence, instagram, tiktok
    except Exception as e:
        print(f"Error processing {url}: {e}")
        return False, False, False, False, False, None, None

# Function to process each batch
def process_batch(batch_file):
    driver = init_driver()
    df = pd.read_excel(batch_file)
    
    for index, row in df.iterrows():
        url = row.get('Site URL')
        if not isinstance(url, str) or not url.startswith(('http://', 'https://')):
            print(f"Skipping invalid URL: {url}")
            continue

        has_videos, has_d2c, has_ecommerce, has_social_media, social_presence, instagram, tiktok = check_website_features(driver, url)
        
        df.at[index, 'D2C Presence'] = 'Yes' if has_d2c else 'No'
        df.at[index, 'eCommerce Platform'] = 'Yes' if has_ecommerce else 'No'
        df.at[index, 'Social Media Presence'] = 'Yes' if has_social_media else 'No'
        df.at[index, 'Integrated Videos on Website'] = 'Yes' if has_videos else 'No'
        df.at[index, 'Instagram'] = instagram
        df.at[index, 'TikTok'] = tiktok
    
    df.to_excel(batch_file, index=False)
    print(f"✅ Processed: {batch_file}")
    driver.quit()

# Function to manage the worker pool
def run_worker_pool(batch_files, workers=None):
    if workers is None:
        workers = max(2, multiprocessing.cpu_count() // 2)  # Auto-set workers to half the CPU cores (at least 2)
    
    print(f"🚀 Running with {workers} workers...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        executor.map(process_batch, batch_files)

# Run batch processing
if __name__ == "__main__":
    print("\n🚀 Processing batches in parallel...\n")
    start_time = time.time()

    workers = max(2, multiprocessing.cpu_count())  # Use all available CPU cores for max speed
    run_worker_pool(batch_files, workers)

    print("\n✅ All batches processed. Merging results...\n")
    
    all_results = []
    for batch_file in batch_files:
        if os.path.exists(batch_file):
            all_results.append(pd.read_excel(batch_file))
    
    if all_results:
        final_df = pd.concat(all_results, ignore_index=True)
        final_df.to_excel(final_output, index=False)
        print(f"🎉 Final merged file saved as {final_output}")
    else:
        print("❌ No batch files found. Check if processing completed successfully.")
    
    print(f"Total Execution Time: {round(time.time() - start_time, 2)} seconds")
