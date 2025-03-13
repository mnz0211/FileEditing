import pyodbc
import numpy as np
import pandas as pd
import time

start_time = time.time()
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=HOUEPC-L-89533\\GEOEP;'
                      'Database=DSTEAM;'
                      'Trusted_Connection=yes;Pooling=False')

def log_error(file_path, line_number, line_content, error_message):
    with open('\\\\americas.shell.com\\tcs\\hou\\ua.sepco\\data.scratch\\50_scratch\\MNZ\\SQL_Listing\\AFS_Volumes_error_log.txt', 'a') as log_file:
        log_file.write(f"File: {file_path}\nLine {line_number}: {line_content}\nError: {error_message}\n\n")

def process_file(file_path, cursor, sql_exe):
    try:
        df = pd.read_csv(file_path, sep='\t', low_memory=False, on_bad_lines='skip')
        sql_data = df.replace({np.nan: None}).to_numpy().tolist()
        for line_number, row in enumerate(sql_data, 1):
            try:
                cursor.execute(sql_exe, row)
            except Exception as e:
                error_message = str(e)
                log_error(file_path, line_number, row, error_message)
    except pd.errors.ParserError as e:
        error_message = str(e)
        with open(file_path, 'r') as file:
            for line_number, line_content in enumerate(file, 1):
                if '\t' in line_content:
                    log_error(file_path, line_number, line_content.strip(), error_message)
                    break
    except Exception as e:
        error_message = str(e)
        with open(file_path, 'r') as file:
            for line_number, line_content in enumerate(file, 1):
                log_error(file_path, line_number, line_content.strip(), error_message)

try:
    cursor = conn.cursor()
    cursor.execute('''TRUNCATE TABLE [DSTEAM].[dbo].[AFS_Volumes_Info]''')

    # Define file paths
    file_paths = [
        '\\\\europe.shell.com\\tcs\\ams\\ui.siep\\data.scratch\\50_Scratch\\MNZ\\SQL_Listing\\AMS_Volumes_Info.dat',
        '\\\\americas.shell.com\\tcs\\hou\\ua.sepco\\data.scratch\\50_scratch\\MNZ\\SQL_Listing\\HOU_Volumes_Info.dat',
        '\\\\americas.shell.com\\tcs\\hou\\ua.sepco\\data.scratch\\50_scratch\\MNZ\\CAL_Volumes_Info.dat'
    ]

    # SQL execution command
    sql_exe = "INSERT INTO [DSTEAM].[dbo].[AFS_Volumes_Info] (file_scan,perm,uid,gid,size,size_on_disk,blocks,atime,mtime,ctime,inode,file_name) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"

    # Process each file
    for file_path in file_paths:
        process_file(file_path, cursor, sql_exe)

    conn.commit()
except Exception as e:
    print(f"Error: {e}")
finally:
    cursor.close()
    conn.close()

print(f"It took {round(time.time() - start_time, 2)} seconds to complete it")