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
   - Optionally submit pages to the Internet Archive if --enable-archive is used

4. Let the script run until completion. This may take a long time depending on how many images are available.

## Features

- Downloads images and their metadata (title, author, license, camera info, etc.)
- Organizes images in author-based folders
- Calculates and stores SHA-256 hashes of downloaded images
- Optional Internet Archive submission of author pages and selected image pages
- Rate limiting to avoid overloading the server
- Automatic resume capability using --start-offset
- Detailed logging of all operations

## Notes

- The script includes rate limiting to avoid overloading the server
- Downloaded images are organized in folders to prevent having too many files in a single directory
- A log file "indafoto_crawler.log" will be created to track the script's progress
- Internet Archive submission is disabled by default to avoid potential errors

## Troubleshooting

If you encounter any issues:

1. Make sure you have a stable internet connection
2. Check that you've installed all required packages
3. Review the log file for any error messages
4. Try running the script again

If problems persist, please report the issue with the error message from the log file.
