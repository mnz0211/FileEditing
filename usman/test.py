import sqlite3
import numpy as np
import pandas as pd

db_file = "dev.db"

create_table_query = """
CREATE TABLE IF NOT EXISTS dev (
   file_scan TEXT,
   perm INTEGER,
   uid INTEGER,
   gid INTEGER,
   size INTEGER,
   size_on_disk INTEGER,
   blocks INTEGER,
   atime INTEGER,
   mtime INTEGER,
   ctime INTEGER,
   inode INTEGER,
   file_name TEXT
);
"""

insert_query = "INSERT INTO dev (file_scan,perm,uid,gid,size,size_on_disk,blocks,atime,mtime,ctime,inode,file_name) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"

file_paths = [
    './200AMS_Volumes_Info.dat',
    './200HOU_Volumes_Info.dat',
    './200CAL_Volumes_Info.dat'
]

LOG_FILE = 'dev-log.txt'

def log_error(file_path, line_number, line_content, error_message):
    log_content = f"File: {file_path}\nLine {line_number}: {line_content}\nError: {error_message}\n\n"
    # print(log_content)
    with open(LOG_FILE, 'a') as log_file:
        log_file.write(log_content)

def process_file(file_path, cursor, sql_exe):
    with open(file_path) as f:
        lines = f.readlines()
        for n, line in enumerate(lines, 1):
            if "\t" not in line:
                continue

            row = line.split("\t")
            if len(row) > 12:
                log_error(file_path, n, row, "The number of columns is greater than 12")
            else:
                try:
                    cursor.execute(sql_exe, row)
                except Exception as e:
                    log_error(file_path, n, row, str(e))

try:
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute(create_table_query)

    # Process each file
    for file_path in file_paths:
        process_file(file_path, cursor, insert_query)

    conn.commit()
# except Exception as e:
    # print(e)
finally:
    cursor.close()

    if conn:
        conn.close()
