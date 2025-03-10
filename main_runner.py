import pandas as pd
import os
import time
from concurrent.futures import ThreadPoolExecutor
import subprocess

# File paths
input_file = r"./URL Checker.xlsx"
batch_folder = r"./Batches"
final_output = r"./Final_Result.xlsx"

# Ensure batch folder exists
os.makedirs(batch_folder, exist_ok=True)

# Load Excel file
df = pd.read_excel(input_file)

# Define batch size
batch_size = 15  # Number of URLs per batch
num_batches = (len(df) // batch_size) + (1 if len(df) % batch_size > 0 else 0)

# Split data into batches and save them
batch_files = []
for i in range(num_batches):
    batch_data = df[i * batch_size: (i + 1) * batch_size]
    batch_file = os.path.join(batch_folder, f"Batch_{i + 1}.xlsx")
    batch_data.to_excel(batch_file, index=False)
    batch_files.append(batch_file)
    print(f"✅ Saved: {batch_file}")

# Function to process each batch using subprocess
def process_batch(batch_file):
    try:
        # Call batch_processor.py with the batch file as an argument
        subprocess.run(['python', 'batch_processor.py', batch_file], check=True)
        print(f"✅ Processed: {batch_file}")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error processing {batch_file}: {e}")

if __name__ == "__main__":
    print("\n🚀 Processing batches in parallel using multithreading...\n")
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(process_batch, batch_files)

    print("\n✅ All batches processed. Merging results...\n")

    # Merge processed batch results
    all_results = []
    for batch_file in batch_files:
        processed_file = batch_file.replace(".xlsx", "_processed.xlsx")
        if os.path.exists(processed_file):
            df_result = pd.read_excel(processed_file)
            all_results.append(df_result)
        else:
            print(f"❌ No result file found for {batch_file}. Skipping.")

    if all_results:
        final_df = pd.concat(all_results, ignore_index=True)
        final_df.to_excel(final_output, index=False)
        print(f"🎉 Final merged file saved as {final_output}")
    else:
        print("❌ No processed batch files found. Check if processing completed successfully.")

    print(f"Total Execution Time: {round(time.time() - start_time, 2)} seconds")
    print("\n👋 Done!")
