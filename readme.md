# Indafoto Archive Tools

This repository contains two main tools for archiving and exploring photos from indafoto.hu:
1. Indafoto Crawler - For downloading and archiving images
2. Archive Explorer - For browsing and organizing the archived content

## Indafoto Crawler

The crawler allows you to archive photos from indafoto.hu before the service shuts down.

The default setting is to save freely licensed images. Change SEARCH_URL_TEMPLATE in indafoto.py if you want to save images based on different criteria (e.g. username)

### Prerequisites

- Python 3.6 or higher
- Internet connection

### Setup Instructions

#### Step 1: Download the Project

If you received this as a ZIP file, extract it to a folder on your computer.

If you need to download it from GitHub:
1. Go to the repository page
2. Click the green "Code" button
3. Select "Download ZIP"
4. Extract the ZIP file to a folder on your computer

##### Alternative: Using Git

<details>
<summary>Click to expand: Using Git for installation and updates</summary>

If you're familiar with Git, you can clone the repository directly:

1. If you don't have Git installed, download and install it from [git-scm.com](https://git-scm.com/downloads)

2. Open a command prompt or terminal

3. Run the following command:
   ```
   git clone https://github.com/bdamokos/indafoto_archive.git
   cd indafoto_archive
   ```

This method has the advantage of making updates easier. When new features or fixes are available, you can update your local copy by running:
   ```
   git pull
   ```

The `git pull` command fetches changes from the remote repository and automatically merges them into your local copy, ensuring you always have the latest version without needing to download the entire project again.
</details>



#### Step 2: Install Python

If you don't have Python installed:

1. Go to [python.org](https://www.python.org/downloads/)
2. Download the latest version for your operating system
3. Run the installer
   - On Windows: Make sure to check "Add Python to PATH" during installation
   - On Mac/Linux: The installer should handle this automatically

#### Step 3: Install Required Packages

1. Open a command prompt or terminal
   - On Windows: Press Win+R, type `cmd` and press Enter
   - On Mac: Open Terminal from Applications > Utilities
   - On Linux: Open your terminal application

2. Navigate to the project folder:
   ```
   cd path/to/indafoto_archive
   ```
   (Replace "path/to/indafoto_archive" with the actual path to where you extracted the files)

3. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

### Running the Tools

#### Crawler Usage

1. Basic usage:
   ```
   python indafoto.py
   ```

2. Available command-line arguments:
   ```
   python indafoto.py --start-offset N  # Start crawling from page N
   python indafoto.py --enable-archive  # Enable Internet Archive / Archive.ph submissions (disabled by default)
   python indafoto.py --retry           # Retry previously failed pages
   python indafoto.py --test            # Run test function for album extraction
   ```

   You can combine arguments:
   ```
   python indafoto.py --start-offset 100 --enable-archive
   ```

#### Archive Explorer Usage

1. Start the web interface:
   ```
   python indafoto_archive_explorer.py
   ```

2. Open your web browser and navigate to:
   ```
   http://localhost:5000
   ```

### Features

#### Crawler Features

- Downloads images and their metadata (title, author, license, camera info, etc.)
- Organizes images in author-based folders
- Calculates and stores SHA-256 hashes of downloaded images
- Optional Internet Archive submission of author pages and a sample of the image pages (disabled by default). The point is to use it as future proof of provenance of the images
- Rate limiting to avoid overloading the server
- Smart resume capability:
  - Can restart from any page using --start-offset
  - Automatically skips already downloaded images
  - Tracks failed pages for later retry
  - Limits retry attempts to prevent infinite loops
- Detailed logging of all operations

#### Archive Explorer Features

- Modern, responsive web interface
- Browse and search archived images
- Filter images by:
  - Author
  - Tag
  - Collection
  - Album
- Mark important images with notes
- View detailed image metadata:
  - Title and author
  - License information
  - Camera details
  - Collections and albums
  - Tags
  - Original URLs
- Statistics dashboard showing:
  - Total images
  - Number of authors
  - Popular tags
  - Collection counts
- Pagination for large galleries
- Mobile-friendly interface

### Error Handling

The crawler includes several error handling features:

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

### Notes

- The crawler includes rate limiting to avoid overloading the server
- Downloaded images are organized in folders to prevent having too many files in a single directory
- A log file "indafoto_crawler.log" will be created to track the script's progress
- Internet Archive submission is disabled by default and can be enabled with --enable-archive flag
- Failed pages are tracked and can be retried later
- The Archive Explorer requires Flask to be installed (`pip install flask`)

### Troubleshooting

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
6. If the Archive Explorer fails to start:
   - Ensure Flask is installed (`pip install flask`)
   - Check if the database file exists
   - Verify port 5000 is not in use

If problems persist, please report the issue with the error message from the log file.
