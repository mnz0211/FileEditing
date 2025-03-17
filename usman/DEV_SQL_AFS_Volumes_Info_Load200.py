import pyodbc
import time

LOG_FILE = '\\\\americas.shell.com\\tcs\\hou\\ua.sepco\\data.scratch\\50_scratch\\MNZ\\200AFS_Volumes_error_log.txt'

start_time = time.time()
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=HOUEPC-L-89533\\GEOEP;'
                      'Database=DSTEAM;'
                      'Trusted_Connection=yes;Pooling=False')

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
    cursor = conn.cursor()
    cursor.execute('''TRUNCATE TABLE [DSTEAM].[dbo].[200AFS_Volumes_Info]''')

    # Define file paths
    file_paths = [
        '\\\\europe.shell.com\\tcs\\ams\\ui.siep\\data.scratch\\50_Scratch\\MNZ\\200AMS_Volumes_Info.dat',
        '\\\\americas.shell.com\\tcs\\hou\\ua.sepco\\data.scratch\\50_scratch\\MNZ\\200HOU_Volumes_Info.dat',
        '\\\\americas.shell.com\\tcs\\hou\\ua.sepco\\data.scratch\\50_scratch\\MNZ\\200CAL_Volumes_Info.dat'
    ]

    # SQL execution command
    sql_exe = "INSERT INTO [DSTEAM].[dbo].[200AFS_Volumes_Info] (file_scan,perm,uid,gid,size,size_on_disk,blocks,atime,mtime,ctime,inode,file_name) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"

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