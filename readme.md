# Indafoto Crawler

This tool allows you to archive photos from indafoto.hu before the service shuts down.

The default setting is to save freely licensed images. Change SEARCH_URL_TEMPLATE in indafoto.py if you want to save images based on different criteria (e.g. username)

## Prerequisites

- Python 3.6 or higher
- Internet connection

## Setup Instructions

### Step 1: Download the Project

If you received this as a ZIP file, extract it to a folder on your computer.

If you need to download it from GitHub:
1. Go to the repository page
2. Click the green "Code" button
3. Select "Download ZIP"
4. Extract the ZIP file to a folder on your computer

### Step 2: Install Python

If you don't have Python installed:

1. Go to [python.org](https://www.python.org/downloads/)
2. Download the latest version for your operating system
3. Run the installer
   - On Windows: Make sure to check "Add Python to PATH" during installation
   - On Mac/Linux: The installer should handle this automatically

### Step 3: Install Required Packages

1. Open a command prompt or terminal
   - On Windows: Press Win+R, type `cmd` and press Enter
   - On Mac: Open Terminal from Applications > Utilities
   - On Linux: Open your terminal application

2. Navigate to the project folder:
   ```
   cd path/to/indafoto-crawler
   ```
   (Replace "path/to/indafoto-crawler" with the actual path to where you extracted the files)

3. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

## Running the Crawler

In the same command prompt or terminal:

1. Basic usage:
   ```
   python indafoto.py
   ```

2. Available command-line arguments:
   ```
   python indafoto.py --start-offset N  # Start crawling from page N
   python indafoto.py --enable-archive  # Enable Internet Archive submissions
   python indafoto.py --retry           # Retry previously failed pages
   python indafoto.py --test            # Run test function for album extraction
   ```

   You can combine arguments:
   ```
   python indafoto.py --start-offset 100 --enable-archive
   ```

3. The script will:
   - Create a database to track downloaded images
   - Create an "indafoto_archive" folder to store the downloaded images
   - Start downloading images from indafoto.hu
   - Show a progress bar for the current operation
   - Track failed pages for later retry
   - Skip already downloaded images when restarting
   - Optionally submit pages to the Internet Archive if --enable-archive is used

## Features

- Downloads images and their metadata (title, author, license, camera info, etc.)
- Organizes images in author-based folders
- Calculates and stores SHA-256 hashes of downloaded images
- Optional Internet Archive submission of author pages and selected image pages
- Rate limiting to avoid overloading the server
- Smart resume capability:
  - Can restart from any page using --start-offset
  - Automatically skips already downloaded images
  - Tracks failed pages for later retry
  - Limits retry attempts to prevent infinite loops
- Detailed logging of all operations

## Error Handling

The script includes several error handling features:

1. Failed Page Tracking:
   - Pages that fail to download are recorded in the database
   - Each failed page gets up to 3 retry attempts
   - Use `--retry` to attempt recovery of failed pages

2. Resume Capability:
   - Can restart from any point using `--start-offset`
   - Automatically skips already downloaded images
   - Maintains database consistency when interrupted

3. Rate Limiting:
   - Includes built-in delays between requests
   - Helps prevent server overload and blocking

## Notes

- The script includes rate limiting to avoid overloading the server
- Downloaded images are organized in folders to prevent having too many files in a single directory
- A log file "indafoto_crawler.log" will be created to track the script's progress
- Internet Archive submission is disabled by default to avoid potential errors
- Failed pages are tracked and can be retried later

## Troubleshooting

If you encounter any issues:

1. Make sure you have a stable internet connection
2. Check that you've installed all required packages
3. Review the log file for any error messages
4. If pages are failing:
   - Try running with `--retry` to attempt recovery
   - Check your internet connection stability
   - Consider increasing the RATE_LIMIT value in the script
5. If the script was interrupted:
   - Use `--start-offset` to resume from where it stopped
   - The script will automatically skip already downloaded images

If problems persist, please report the issue with the error message from the log file.
