import pandas as pd
import os
import time
import multiprocessing

# File paths
input_file = r"./URL Checker.xlsx"
batch_folder = r"./Batches"
final_output = r"./Final_Result.xlsx"

# Ensure batch folder exists
os.makedirs(batch_folder, exist_ok=True)

# Load Excel file
df = pd.read_excel(input_file)
batch_size = 10 # Number of URLs per batch
num_batches = (len(df) // batch_size) + (1 if len(df) % batch_size > 0 else 0)  # Calculate total batches

# Split the data into batches and save them
batch_files = []
for i in range(num_batches):
    batch_data = df[i * batch_size: (i + 1) * batch_size]  # Extract batch
    batch_file = f"{batch_folder}/Batch_{i + 1}.xlsx"
    batch_data.to_excel(batch_file, index=False)
    batch_files.append(batch_file)
    print(f"✅ Saved: {batch_file}")

# Function to process each batch in a separate process
def process_batch(batch_file):
    os.system(f"python batch_processor.py \"{batch_file}\"")

# Run batch processing using multiprocessing
if __name__ == "__main__":
    print("\n🚀 Processing batches in parallel using multiprocessing...\n")
    start_time = time.time()

    processes = []
    for batch_file in batch_files:
        p = multiprocessing.Process(target=process_batch, args=(batch_file,))
        p.start()
        processes.append(p)

    # Wait for all processes to finish
    for p in processes:
        p.join()

    print("\n✅ All batches processed. Merging results...\n")

    # Merge all batch results
    all_results = []
    for batch_file in batch_files:
        if os.path.exists(batch_file):
            all_results.append(pd.read_excel(batch_file))

    # Concatenate all batches into final output
    if all_results:
        final_df = pd.concat(all_results, ignore_index=True)
        final_df.to_excel(final_output, index=False)
        print(f"🎉 Final merged file saved as {final_output}")
    else:
        print("❌ No batch files found. Check if processing completed successfully.")

    print(f"Total Execution Time: {round(time.time() - start_time, 2)} seconds")
