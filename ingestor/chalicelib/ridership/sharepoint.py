import requests
import json
import re
import logging
from tempfile import NamedTemporaryFile
from urllib.parse import quote

logger = logging.getLogger(__name__)

DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
BASE_URL = "https://mbta.sharepoint.com/sites/PublicData/Shared%20Documents/Forms/AllItems.aspx"
SUBWAY_SHARE_URL = "https://mbta.sharepoint.com/:f:/s/PublicData/ElfNM8viGx5Out070Rg7tTABH1wLLEdwh69nIOb4J3Nt8w"
BUS_SHARE_URL = "https://mbta.sharepoint.com/:f:/s/PublicData/Eh1G_O3dog9Eh_EfCqsJZ9EBb6BIgjP-ovWMwdLpwuDnjw"


class SharepointConnection:
    def __init__(
        self, user_agent: str = DEFAULT_USER_AGENT, base_url=BASE_URL, prefix="mbta", share_url=SUBWAY_SHARE_URL
    ) -> None:
        self.session = self.setup_session(user_agent)
        self.base_url = base_url
        self.all_files = []
        self.prefix = prefix
        self.share_url = share_url

    def setup_session(self, user_agent: str) -> requests.Session:
        session = requests.Session()
        session.headers.update({"User-Agent": user_agent})
        return session

    def get_sharepoint_folder_contents_anonymous(self, share_url):
        """
        Get contents of a SharePoint folder using anonymous access via sharing link.

        Args:
            share_url: The SharePoint 'anyone with the link' URL

        Returns:
            List of dictionaries containing file information, or None on error
        """
        # Follow the sharing link
        response = self.session.get(share_url, allow_redirects=True)

        if response.status_code != 200:
            logger.error(f"Error accessing share URL: {response.status_code}")
            return None

        html = response.text
        files = self.parse_g_data(html)

        return files

    def parse_g_data(self, html: str):
        # Extract g_listData which contains the file list
        # Find the start of g_listData
        start_marker = "g_listData = "
        start_pos = html.find(start_marker)

        if start_pos == -1:
            logger.error("Could not find g_listData in page")
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
                logger.error("Unexpected g_listData structure")
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
            logger.error(f"Error parsing JSON: {e}")
            return None

    def get_folder_by_path(self, folder_path):
        """
        Get contents of a specific folder by its server-relative path.

        Args:
            session: requests.Session with cookies
            folder_path: Server-relative path like '/sites/PublicData/Shared Documents/...'

        Returns:
            List of file info dictionaries
        """
        # Construct the URL to view that specific folder

        folder_url = f"{self.base_url}?id={quote(folder_path)}&p=true&ga=1"

        response = self.session.get(folder_url)
        if response.status_code != 200:
            logger.error(f"Error accessing folder: {response.status_code}")
            return None

        html = response.text
        files = self.parse_g_data(html)
        return files

    def list_all_files_recursive(self, folder_path, indent=0):
        """
        Recursively list all files in a folder and its subfolders.

        Args:
            folder_path: Server-relative path to start from
            indent: Indentation level for display

        Returns:
            List of all files (not folders) found
        """
        files = self.get_folder_by_path(folder_path)
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

            logger.info(f"{prefix}[{file_type}] {file['name']} - {size:,} bytes")

            if file["is_folder"]:
                # Recursively explore subfolder
                subfiles = self.list_all_files_recursive(file["url"], indent + 1)
                all_files.extend(subfiles)
            else:
                all_files.append(file)

        return all_files

    def download_sharepoint_file_anonymous(self, file_ref, output_path):
        """
        Download a file from SharePoint using an existing session.

        Args:
            file_ref: The FileRef path from the file list
            output_path: Local path to save the file

        Returns:
            True if successful, False otherwise
        """
        # Construct download URL
        download_url = f"https://{self.prefix}.sharepoint.com{file_ref}?download=1"

        response = self.session.get(download_url)

        if response.status_code == 200:
            with open(output_path, "wb") as f:
                f.write(response.content)
            logger.info(f"Downloaded: {output_path}")
            return True
        else:
            logger.error(f"Error downloading {file_ref}: Status code {response.status_code}")
            return False

    def fetch_sharepoint_file(self, file_regex=None, share_url=None, target_date=None, bus_data=True):
        """
        Downloads files from Sharepoint matching a regex pattern.

        Args:
            file_regex (str): Regular expression pattern to match against filenames. If None, uses default patterns based on bus_data.
            share_url (str): SharePoint sharing URL to download from. If None, uses default URLs based on bus_data.
            target_date (str): Takes format 2025.09.30, specifies which file to download. Used for default subway data regex. Optional for Bus data, required for Subway Data when file_regex is None.
            bus_data (bool): Whether to download Bus Data (True) or Subway Data (False). Only used when file_regex is None.

        Returns:
            str: Path to named Temporary File with data.
        """
        # Determine share URL
        if share_url is None:
            if bus_data:
                share_url = BUS_SHARE_URL
            else:
                if target_date or file_regex:
                    share_url = SUBWAY_SHARE_URL
                else:
                    logger.error("If downloading Subway data, please specify target date or file_regex.")
                    return None

        # Determine file regex pattern
        if file_regex is None:
            # Use default patterns
            if bus_data:
                file_regex = r"^MBTA Bus Weekly Ridership\.xlsx$"
            else:
                if target_date:
                    # Create a more specific regex that includes the date
                    date_pattern = target_date.replace(".", r"\.")  # Escape dots for regex
                    file_regex = rf".*{date_pattern}.* MBTA Gated Station Validations by line\.csv$"
                else:
                    logger.error("If downloading Subway data without file_regex, please specify target_date.")
                    return None

        files = self.get_sharepoint_folder_contents_anonymous(share_url)

        if files:
            # Recursively list all files
            all_files = []
            for file in files:
                size = file.get("size") or 0
                if isinstance(size, str):
                    try:
                        size = int(size)
                    except (ValueError, TypeError):
                        size = 0

                if file["is_folder"]:
                    subfiles = self.list_all_files_recursive(file["url"], 1)
                    all_files.extend(subfiles)
                else:
                    all_files.append(file)

            if all_files:
                logger.info("--- Download Example ---")
                output_path = NamedTemporaryFile().name

                # Find files matching the regex
                matching_files = [file for file in all_files if re.search(file_regex, file["name"], re.IGNORECASE)]

                if matching_files:
                    file = matching_files[0]  # Take the first match
                    logger.info(f"Downloading {file['name']} to {output_path}...")
                    self.download_sharepoint_file_anonymous(file["url"], output_path)
                    return output_path
                else:
                    logger.warning(f"No files found matching pattern: {file_regex}")
                    return None

        else:
            logger.error("No files found or error occurred")
            return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sharepoint = SharepointConnection()
    sharepoint.fetch_sharepoint_file(target_date="2025.09.30", bus_data=False)
