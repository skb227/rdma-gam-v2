import subprocess
import csv
import re
import os

# 1. Define the parameters you want to test
thread_counts = [1, 2, 4, 8, 16]
csv_filename = "results.csv"

# Write the CSV header if the file doesn't exist yet
if not os.path.exists(csv_filename):
    with open(csv_filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["Threads", "Duration_us", "Throughput_ops_sec"])

for threads in thread_counts:
    print(f"--- Starting experiment with {threads} threads ---")

    # 2. Dynamically create/overwrite the cl.experiment file
    # (Adjust this string to match exactly what Remus needs in cl.experiment)
    experiment_config = f"""
exefile=build/experiment/run
experiment_args="--seg-size 25 --segs-per-mn 20 --first-cn-id 1 --last-cn-id 1 --first-mn-id 0 --last-mn-id 1 --qp-lanes 4 --qp-sched-pol RR --mn-port 33330 --cn-threads {threads} --cn-ops-per-thread 4 --cn-thread-bufsz 20 --alloc-pol GLOBAL-RR"
"""
    with open("cl.experiment", "w") as f:
        f.write(experiment_config)

    # 3. Run the Remus shell script (this will block until cl.sh finishes)
    command = ["./cl.sh", "./cl.config", "run", "./cl.experiment"]
    print(f"Running command: {' '.join(command)}")
    subprocess.run(command) # We no longer care about capture_output

    # 4. Fetch the result file from the remote node!
    cn0_user = "skb227"
    cn0_ip = "apt185.apt.emulab.net"
    file_path = "/users/skb227/node1.txt" 

    fetch_cmd = ["ssh", f"{cn0_user}@{cn0_ip}", f"cat {file_path}"]
    fetch_process = subprocess.run(fetch_cmd, capture_output=True, text=True)
    
    output = fetch_process.stdout
    
    # 5. Parse the output just like before
    match = re.search(r'DUR_US:(\d+)', output)
    
    if match:
        duration_us = int(match.group(1))
        
        # Calculate throughput (assuming 20,000 ops per thread)
        total_ops = threads * 20000 
        duration_sec = duration_us / 1_000_000.0
        throughput = total_ops / duration_sec if duration_sec > 0 else 0
        
        print(f"Success! Threads: {threads} | Throughput: {throughput:.2f} ops/sec")
        
        # 6. Save the results to the CSV file
        with open(csv_filename, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([threads, duration_us, throughput])
            
    else:
        print(f"Failed to find 'DUR_US:' in the output for {threads} threads.")
        print("Raw Output:\n", output)

print("All experiments finished!")