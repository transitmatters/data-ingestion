import requests
import json
import re
from tempfile import NamedTemporaryFile
from urllib.parse import quote


def get_sharepoint_folder_contents_anonymous(share_url):
    """
    Get contents of a SharePoint folder using anonymous access via sharing link.

    Args:
        share_url: The SharePoint 'anyone with the link' URL

    Returns:
        List of dictionaries containing file information, or None on error
    """
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
    )

    # Follow the sharing link
    response = session.get(share_url, allow_redirects=True)

    if response.status_code != 200:
        print(f"Error accessing share URL: {response.status_code}")
        return None

    html = response.text

    # Extract g_listData which contains the file list
    # Find the start of g_listData
    start_marker = "g_listData = "
    start_pos = html.find(start_marker)

    if start_pos == -1:
        print("Could not find g_listData in page")
        return None

    # Find the JSON object - look for matching braces
    json_start = start_pos + len(start_marker)
    brace_count = 0
    in_string = False
    escape_next = False
    json_end = json_start

    for i in range(json_start, len(html)):
        char = html[i]

        if escape_next:
            escape_next = False
            continue

        if char == "\\":
            escape_next = True
            continue

        if char == '"':
            in_string = not in_string
            continue

        if not in_string:
            if char == "{":
                brace_count += 1
            elif char == "}":
                brace_count -= 1
                if brace_count == 0:
                    json_end = i + 1
                    break

    json_str = html[json_start:json_end]

    try:
        list_data = json.loads(json_str)

        if "ListData" not in list_data or "Row" not in list_data["ListData"]:
            print("Unexpected g_listData structure")
            return None

        files = []
        for item in list_data["ListData"]["Row"]:
            file_info = {
                "name": item.get("FileLeafRef"),
                "url": item.get("FileRef"),
                "size": item.get("File_x0020_Size"),
                "modified": item.get("Modified"),
                "is_folder": item.get("FSObjType") == "1",
                "id": item.get("ID"),
            }
            files.append(file_info)

        return files, session  # Return session for downloads

    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return None


def download_sharepoint_file_anonymous(session, file_ref, output_path):
    """
    Download a file from SharePoint using an existing session.

    Args:
        session: requests.Session with cookies from the share URL visit
        file_ref: The FileRef path from the file list
        output_path: Local path to save the file

    Returns:
        True if successful, False otherwise
    """
    # Construct download URL
    download_url = f"https://mbta.sharepoint.com{file_ref}?download=1"

    response = session.get(download_url)

    if response.status_code == 200:
        with open(output_path, "wb") as f:
            f.write(response.content)
        print(f"Downloaded: {output_path}")
        return True
    else:
        print(f"Error downloading {file_ref}: Status code {response.status_code}")
        return False


def get_folder_by_path(session, folder_path):
    """
    Get contents of a specific folder by its server-relative path.

    Args:
        session: requests.Session with cookies
        folder_path: Server-relative path like '/sites/PublicData/Shared Documents/...'

    Returns:
        List of file info dictionaries
    """
    # Construct the URL to view that specific folder

    base_url = "https://mbta.sharepoint.com/sites/PublicData/Shared%20Documents/Forms/AllItems.aspx"
    folder_url = f"{base_url}?id={quote(folder_path)}&p=true&ga=1"

    response = session.get(folder_url)
    if response.status_code != 200:
        print(f"Error accessing folder: {response.status_code}")
        return None

    html = response.text

    # Optional to inspect the HTML file
    # with open("test.html", "w") as file:
    #     file.write(html)

    # Extract g_listData
    start_marker = "g_listData = "
    start_pos = html.find(start_marker)

    if start_pos == -1:
        return None

    json_start = start_pos + len(start_marker)
    brace_count = 0
    in_string = False
    escape_next = False
    json_end = json_start

    for i in range(json_start, len(html)):
        char = html[i]

        if escape_next:
            escape_next = False
            continue

        if char == "\\":
            escape_next = True
            continue

        if char == '"':
            in_string = not in_string
            continue

        if not in_string:
            if char == "{":
                brace_count += 1
            elif char == "}":
                brace_count -= 1
                if brace_count == 0:
                    json_end = i + 1
                    break

    json_str = html[json_start:json_end]

    try:
        list_data = json.loads(json_str)

        if "ListData" not in list_data or "Row" not in list_data["ListData"]:
            return None

        files = []
        for item in list_data["ListData"]["Row"]:
            file_info = {
                "name": item.get("FileLeafRef"),
                "url": item.get("FileRef"),
                "size": item.get("File_x0020_Size"),
                "modified": item.get("Modified"),
                "is_folder": item.get("FSObjType") == "1",
                "id": item.get("ID"),
            }
            files.append(file_info)

        return files

    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return None


def list_all_files_recursive(session, folder_path, indent=0):
    """
    Recursively list all files in a folder and its subfolders.

    Args:
        session: requests.Session with cookies
        folder_path: Server-relative path to start from
        indent: Indentation level for display

    Returns:
        List of all files (not folders) found
    """
    files = get_folder_by_path(session, folder_path)
    if not files:
        return []

    all_files = []
    prefix = "  " * indent

    for file in files:
        file_type = "Folder" if file["is_folder"] else "File"
        size = file.get("size") or 0
        # Convert size to int if it's a string
        if isinstance(size, str):
            try:
                size = int(size)
            except (ValueError, TypeError):
                size = 0

        print(f"{prefix}[{file_type}] {file['name']} - {size:,} bytes")

        if file["is_folder"]:
            # Recursively explore subfolder
            subfiles = list_all_files_recursive(session, file["url"], indent + 1)
            all_files.extend(subfiles)
        else:
            all_files.append(file)

    return all_files


def fetch_sharepoint_file(target_date=None, bus_data=True):
    """
    Downloads the ridership files from Sharepoint.

    Args:
        target_date (str): Takes format 2025.09.30, specifies which file to download. Optional for Bus data, required for Subway Data.
        bus_data (bool): Whether to download Bus Data (True) or Subway Data (False).

    Returns:
        str: Path to named Temporary File with data.
    """
    if bus_data:
        share_url = "https://mbta.sharepoint.com/:f:/s/PublicData/Eh1G_O3dog9Eh_EfCqsJZ9EBb6BIgjP-ovWMwdLpwuDnjw"
    else:
        if target_date:
            # SharePoint 'anyone with the link' URL
            share_url = "https://mbta.sharepoint.com/:f:/s/PublicData/ElfNM8viGx5Out070Rg7tTABH1wLLEdwh69nIOb4J3Nt8w"
        else:
            print("If downloading Subway data, please specify target date.")
            return None

    # print("Fetching folder contents using anonymous access...")
    # print(f"Share URL: {share_url}\n")

    result = get_sharepoint_folder_contents_anonymous(share_url)

    if result:
        files, session = result
        # print(f"Found {len(files)} items in root:\n")

        # Recursively list all files
        all_files = []
        for file in files:
            # file_type = "Folder" if file["is_folder"] else "File"
            size = file.get("size") or 0
            if isinstance(size, str):
                try:
                    size = int(size)
                except (ValueError, TypeError):
                    size = 0
            # print(f"[{file_type}] {file['name']} - {size:,} bytes")

            if file["is_folder"]:
                subfiles = list_all_files_recursive(session, file["url"], 1)
                all_files.extend(subfiles)
            else:
                all_files.append(file)

        # print("\n=== Summary ===")
        # print(f"Total files found: {len(all_files)}")

        if all_files:
            print("\n--- Download Example ---")
            output_path = NamedTemporaryFile().name
            if bus_data:
                idx = all_files.index("MBTA Bus Weekly Ridership.xlsx")
                file = all_files[idx]
                print(f"Downloading {file['name']} to {output_path}...")
                download_sharepoint_file_anonymous(session, file["url"], output_path)
                return output_path
            else:
                if target_date:
                    # Create a more specific regex that includes the date
                    date_pattern = target_date.replace(".", r"\.")  # Escape dots for regex
                    specific_regex = rf".*{date_pattern}.* MBTA Gated Station Validations by line\.csv$"  # Match files containing the date and ending in .csv

                    matching_files = [
                        file for file in all_files if re.search(specific_regex, file["name"], re.IGNORECASE)
                    ]

                    if matching_files:
                        file = matching_files[0]  # Take the first match
                        print(f"Downloading {file['name']} to {output_path}...")
                        download_sharepoint_file_anonymous(session, file["url"], output_path)
                        return output_path

    else:
        print("No files found or error occurred")
        return None


def main():
    # SharePoint 'anyone with the link' URL
    # share_url = "https://mbta.sharepoint.com/:f:/s/PublicData/ElfNM8viGx5Out070Rg7tTABH1wLLEdwh69nIOb4J3Nt8w"
    share_url = "https://mbta.sharepoint.com/:f:/s/PublicData/Eh1G_O3dog9Eh_EfCqsJZ9EBb6BIgjP-ovWMwdLpwuDnjw"

    print("Fetching folder contents using anonymous access...")
    print(f"Share URL: {share_url}\n")

    result = get_sharepoint_folder_contents_anonymous(share_url)

    if result:
        files, session = result
        print(f"Found {len(files)} items in root:\n")

        # Recursively list all files
        all_files = []
        for file in files:
            file_type = "Folder" if file["is_folder"] else "File"
            size = file.get("size") or 0
            if isinstance(size, str):
                try:
                    size = int(size)
                except (ValueError, TypeError):
                    size = 0
            print(f"[{file_type}] {file['name']} - {size:,} bytes")

            if file["is_folder"]:
                subfiles = list_all_files_recursive(session, file["url"], 1)
                all_files.extend(subfiles)
            else:
                all_files.append(file)

        print("\n=== Summary ===")
        print(f"Total files found: {len(all_files)}")

        # Example: Download the first file
        if all_files:
            print("\n--- Download Example ---")
            file = all_files[0]
            output_path = f"/tmp/{file['name']}"
            print(f"Downloading {file['name']} to {output_path}...")
            download_sharepoint_file_anonymous(session, file["url"], output_path)
    else:
        print("No files found or error occurred")


if __name__ == "__main__":
    fetch_sharepoint_file("2025.09.30", False)
