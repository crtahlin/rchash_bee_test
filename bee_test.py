import configparser
import subprocess
import time
import datetime
import csv
import json
import os

def load_config(config_file='config.ini'):
    """
    Loads configuration parameters from a specified INI file.

    Args:
        config_file (str): The path to the configuration file.

    Returns:
        configparser.SectionProxy: A dictionary-like object containing
                                  the loaded configuration.
    """
    config = configparser.ConfigParser()
    if not os.path.exists(config_file):
        print(f"Error: Configuration file '{config_file}' not found.")
        print("Please create a 'config.ini' file with the following structure:")
        print("""
[BEE_TEST]
num_runs = 5
pause_duration = 10
storage_radius = 10
neighbourhood = 501c
log_file = bee_test_log.csv
        """)
        exit(1)
    config.read(config_file)
    if 'BEE_TEST' not in config:
        print(f"Error: Section 'BEE_TEST' not found in '{config_file}'.")
        exit(1)
    return config['BEE_TEST']

def get_bee_data(endpoint_url):
    """
    Fetches JSON data from a given Bee node endpoint using curl.

    Args:
        endpoint_url (str): The full URL of the Bee node endpoint.

    Returns:
        dict or None: Parsed JSON data if successful, None otherwise.
    """
    try:
        # Use sudo for curl to ensure it can connect if the Bee node is running
        # with elevated privileges or on a restricted port.
        # -s: Silent mode (don't show progress meter or error messages)
        # -X GET: Specify GET request method
        command = ['sudo', 'curl', '-sX', 'GET', endpoint_url]
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        return json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error executing curl for {endpoint_url}: {e}")
        print(f"Stdout: {e.stdout}")
        print(f"Stderr: {e.stderr}")
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {endpoint_url}. Raw output: {result.stdout}")
    except FileNotFoundError:
        print(f"Error: 'curl' command not found. Please ensure curl is installed and in your PATH.")
    except Exception as e:
        print(f"An unexpected error occurred while fetching data from {endpoint_url}: {e}")
    return None

def run_bee_test(config):
    """
    Executes the Bee node test sequence based on the provided configuration.

    Args:
        config (configparser.SectionProxy): The loaded configuration.
    """
    try:
        num_runs = int(config['num_runs'])
        pause_duration = int(config['pause_duration'])
        storage_radius = int(config['storage_radius'])
        neighbourhood = config['neighbourhood']
        log_file = config['log_file']
    except ValueError as e:
        print(f"Configuration error: {e}. Please ensure num_runs, pause_duration, and storage_radius are integers.")
        exit(1)
    except KeyError as e:
        print(f"Configuration error: Missing key '{e}'. Please check your config.ini file.")
        exit(1)

    # Define the headers for the CSV file
    fieldnames = [
        "timestamp_start_command",
        "command_executed",
        "rchash_duration_seconds", # This will be the curl execution time if no specific duration is returned by Bee.
        "timestamp_end_command",
        "reserveSizeWithinRadius",
        "reserveSize",
        "overlay",
        "pullsyncRate",
        "status_storageRadius",
        "connectedPeers",
        "isFullySynced",
        "isHealthy",
        "num_neighborhoods" # New field added
    ]

    # Check if log file exists to determine if header needs to be written
    file_exists = os.path.exists(log_file)

    with open(log_file, 'a', newline='') as csvfile:
        csv_writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';', quoting=csv.QUOTE_ALL)

        if not file_exists:
            csv_writer.writeheader()
            print(f"Created new log file: '{log_file}' with header.")
        else:
            print(f"Appending to existing log file: '{log_file}'.")

        for i in range(1, num_runs + 1):
            print(f"\n--- Running test iteration {i}/{num_runs} ---")

            log_entry = {}

            # Construct the rchash command URL
            rchash_url = f"http://localhost:1633/rchash/{storage_radius}/{neighbourhood}/{neighbourhood}"
            rchash_command_display = f"sudo curl -sX GET {rchash_url}"
            log_entry["command_executed"] = rchash_command_display

            # Record start timestamp for rchash command
            start_time = datetime.datetime.now()
            log_entry["timestamp_start_command"] = start_time.isoformat()

            print(f"Executing command: {rchash_command_display}")
            rchash_data = get_bee_data(rchash_url)

            end_time = datetime.datetime.now()
            log_entry["timestamp_end_command"] = end_time.isoformat()

            # Calculate duration of the curl command execution
            rchash_exec_duration = (end_time - start_time).total_seconds()
            log_entry["rchash_duration_seconds"] = rchash_exec_duration

            if rchash_data and 'duration' in rchash_data:
                # If the Bee node's rchash endpoint explicitly returns a duration, use that.
                # Otherwise, we use the curl command execution time.
                log_entry["rchash_duration_seconds"] = rchash_data['duration']
                print(f"rchash calculation duration (from Bee): {rchash_data['duration']:.2f} seconds")
            else:
                print(f"rchash command executed in {rchash_exec_duration:.2f} seconds (no specific duration returned by Bee node).")


            # Fetch status data
            status_url = "http://localhost:1633/status"
            status_data = get_bee_data(status_url)
            if status_data:
                log_entry["reserveSizeWithinRadius"] = status_data.get("reserveSizeWithinRadius", "N/A")
                log_entry["reserveSize"] = status_data.get("reserveSize", "N/A")
                log_entry["overlay"] = status_data.get("overlay", "N/A")
                log_entry["pullsyncRate"] = status_data.get("pullsyncRate", "N/A")
                log_entry["status_storageRadius"] = status_data.get("storageRadius", "N/A")
                log_entry["connectedPeers"] = status_data.get("connectedPeers", "N/A")
                print(f"Bee Status: reserveSizeWithinRadius={log_entry['reserveSizeWithinRadius']}, reserveSize={log_entry['reserveSize']}, overlay={log_entry['overlay']}, connectedPeers={log_entry['connectedPeers']}")
            else:
                print("Could not retrieve Bee status data.")
                for key in ["reserveSizeWithinRadius", "reserveSize", "overlay", "pullsyncRate", "status_storageRadius", "connectedPeers"]:
                    log_entry[key] = "ERROR"

            # Fetch redistribution state data
            redistribution_url = "http://localhost:1633/redistributionstate"
            redistribution_data = get_bee_data(redistribution_url)
            if redistribution_data:
                log_entry["isFullySynced"] = redistribution_data.get("isFullySynced", "N/A")
                log_entry["isHealthy"] = redistribution_data.get("isHealthy", "N/A")
                print(f"Redistribution State: isFullySynced={log_entry['isFullySynced']}, isHealthy={log_entry['isHealthy']}")
            else:
                print("Could not retrieve Bee redistribution state data.")
                for key in ["isFullySynced", "isHealthy"]:
                    log_entry[key] = "ERROR"

            # Fetch neighborhoods data and count entries
            neighborhoods_url = "http://localhost:1633/status/neighborhoods"
            neighborhoods_data = get_bee_data(neighborhoods_url)
            if neighborhoods_data and isinstance(neighborhoods_data, list):
                log_entry["num_neighborhoods"] = len(neighborhoods_data)
                print(f"Number of Neighborhoods: {log_entry['num_neighborhoods']}")
            else:
                print("Could not retrieve or parse Bee neighborhoods data.")
                log_entry["num_neighborhoods"] = "ERROR"

            # Write the collected data to the CSV file
            csv_writer.writerow(log_entry)
            print(f"Data logged to '{log_file}'.")

            if i < num_runs:
                print(f"Pausing for {pause_duration} seconds before next run...")
                time.sleep(pause_duration)

    print("\n--- Testing complete! ---")
    print(f"All results saved to '{log_file}'.")

if __name__ == "__main__":
    print("Starting Ethereum Bee Node Test Script.")
    print("Note: This script uses 'sudo curl'. You might be prompted for your password.")
    print("Ensure your Bee node is running and accessible at http://localhost:1633.")

    config_file_name = 'config.ini' # Default config file name
    config = load_config(config_file_name)
    run_bee_test(config)
