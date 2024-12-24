import subprocess

def list_files(source_path):
    try:
        command = f"/opt/mapr/bin/mc --insecure ls {source_path}"
        print(f"Running command: {command}")
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"Command output: {result.stdout}")
        files = []
        for line in result.stdout.splitlines():
            parts = line.split()
            if len(parts) >= 3:
                filename = parts[-1]
                files.append(filename)
        return files
    except subprocess.CalledProcessError as e:
        print("Error listing files:")
        print(f"Command: {command}")
        print(f"Error output: {e.stderr}")
        return []


def move_file(source_file, destination_path):
    try:
        command = f"/opt/mapr/bin/mc mv {source_file} {destination_path}"
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"Moved file: {source_file}")
    except subprocess.CalledProcessError as e:
        print(f"Error moving folder: {source_file}")
        # print(e.stderr)

def move_files_in_path(source_path, destination_path):
    files = list_files(source_path)
    if not files:
        print("No files found to move.")
        return

    for file in files:
        full_source_path = f"{source_path}{file}"
        move_file(full_source_path, destination_path)

if __name__ == "__main__":
    source_path = "s3/staging/DTC/ALL_DTC_TEST/"
    destination_path = "s3/staging/DTC/ALL_DTC_TEST/Archive/"
    
    move_files_in_path(source_path, destination_path)
