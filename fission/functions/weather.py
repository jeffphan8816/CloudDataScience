from ftplib import FTP
import csv
from elasticsearch.helpers import parallel_bulk

# Function to print lines, skipping the first 13 lines
line_count = 0


def process_line(line):
    global line_count
    line_count += 1
    if line_count > 13:
        if line.startswith("Totals:"):
            line_count = 0
            return


# FTP server details
ftp_host = "ftp.bom.gov.au"
ftp_user = "anonymous"
ftp_passwd = ""  # Your email address as the password

# Connect to FTP server
ftp = FTP(ftp_host)
ftp.login(user=ftp_user, passwd=ftp_passwd)

# Start from the directory
ftp.cwd("/anon/gen/clim_data/IDCKWCDEA0/tables/vic/")

# List directory contents
surburbs = ftp.nlst()

subdirectories = []
# Loop through files
for surburb in surburbs:
    # print(f"Content of {surburb}:" + "\n")
    subdirectories.append(surburb)
# Loop through subdirectories
for subdirectory in subdirectories:
    # fission function to call multiple threads
    files = ftp.nlst(subdirectory)
    for file in files:
        if file.endswith('.csv'):
            print(f"Content of {file}:")
            ftp.retrlines(f"RETR {file}", process_line)
            line_count = 0
# Close connection
ftp.quit()
