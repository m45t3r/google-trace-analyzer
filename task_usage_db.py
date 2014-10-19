#!/usr/bin/env python

import csv
import gzip
import sqlite3
import os

PART_START = 0
PART_END = 499
TASK_USAGE_DIR = '../task_usage/'
DB_FILENAME = 'task_usage-part-' + str(PART_START).zfill(5) + '-of-' + str(PART_END).zfill(5) + '.sqlite3'
RESULT_FILE = DB_FILENAME.split('.')[0] + '-summary.csv'

def create_task_usage_db(db_filename, task_usage_dir, start, end):
    # Connect to database and initialize a cursor, so we can do operations
    # to the db.
    with sqlite3.connect(db_filename) as conn:
        cur = conn.cursor()

        # Create a sqlite database to hold task_usage data
        query = "CREATE TABLE IF NOT EXISTS task_usage (start_time INTEGER, end_time INTEGER, job_id INTEGER, task_index INTEGER, machine_id INTEGER, cpu_rate FLOAT, canonical_memory_usage FLOAT, assigned_memory_usage FLOAT, unmapped_page_cache FLOAT, total_page_cache FLOAT, maximum_memory_usage FLOAT, disk_io_time FLOAT, local_disk_space_usage FLOAT, maximum_cpu_rate FLOAT, maximum_disk_io_time FLOAT, cycles_per_instruction FLOAT, memory_accesses_per_instruction FLOAT, sample_portion FLOAT, aggregation_type BOOLEAN, part INTEGER)"
        cur.execute(query)

        # The prefix and posfix for the traces filenames is always the same
        _filename_prefix = "part-"
        _filename_posfix = "-of-00500.csv.gz"

        # Since range is actually [start, end), we need to do end + 1
        for part in range(start, end + 1):
            # str(part).zfill(5) will generate a number with 5 digits,
            # including leading zeros. Concatenate this number with prefix
            # and posfix to generate the full filename
            filename = _filename_prefix + str(part).zfill(5) + _filename_posfix
            # Inform the user about the progress, so in case of error we can now
            # which file is problematic.
            print("Processing file {}".format(filename))

            # Added directory to path
            filepath = os.path.join(task_usage_dir, filename)
            # Each task_usage file is compressed. Instead of wasting space and
            # time decompressing the files, use gzip.open(), decompressing on
            # demand
            with gzip.open(filepath, mode='rt') as csv_file:
                # Read each row from the current CSV file and add to database
                for row in csv.reader(csv_file, delimiter=','):
                    # Added original filename to database, so we can track the
                    # original file of a task.
                    row.append(part)
                    query = "INSERT OR IGNORE INTO task_usage (start_time, end_time, job_id, task_index, machine_id, cpu_rate, canonical_memory_usage, assigned_memory_usage, unmapped_page_cache, total_page_cache, maximum_memory_usage, disk_io_time, local_disk_space_usage, maximum_cpu_rate, maximum_disk_io_time, cycles_per_instruction, memory_accesses_per_instruction, sample_portion, aggregation_type, part) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                    cur.execute(query, row)
            
            # Commit the data before going to the next file, so in case of problems
            # you can just resume from the last good file (in theory at least).
            conn.commit()
        
        # Optimize search for job_id and task_index fields, since they're the
        # most used ones.
        print("Creating index for job_id and task_index")
        cur.execute("CREATE INDEX job_id_and_task_index ON task_usage(job_id,task_index)")

    # Connection to database is closed, everything should be ok.
    print("Done")

def create_data_summary(db_filename, result_file):
    with sqlite3.connect(db_filename) as conn:
        # Necessary to add STDEV and VARIANCE support to sqlite
        # See "extension-functions.c" (https://www.sqlite.org/contrib) for details.
        conn.enable_load_extension(True)
        conn.load_extension('./libsqlitefunctions.so')
        
        cur = conn.cursor()

        # Create a sqlite database to hold task_usage data
        query = "CREATE TABLE IF NOT EXISTS task_usage_summary (job_id INTEGER, task_index INTEGER, start_time FLOAT, end_time FLOAT, task_duration FLOAT, number_of_entries INTEGER, avg_cpu_rate FLOAT, avg_memory_usage FLOAT, avg_disk_io_time FLOAT, avg_disk_space_usage FLOAT, stdev_cpu_rate FLOAT, stdev_memory_usage FLOAT, stdev_disk_io_time FLOAT, stdev_disk_space_usage FLOAT, var_cpu_rate FLOAT, var_memory_usage FLOAT, var_disk_io_time FLOAT, var_disk_usage_space FLOAT, median_cpu_rate FLOAT, median_memory_usage FLOAT, median_disk_io_time FLOAT, median_disk_space_usage FLOAT, max_cpu_time FLOAT, max_memory_usage FLOAT, max_disk_io_time FLOAT, max_disk_usage_space FLOAT, PRIMARY KEY (job_id, task_index))"
        cur.execute(query)

        query = "SELECT DISTINCT job_id,task_index FROM task_usage"
        cur.execute(query)
        print("Processing distinct tasks")

        for result in cur.fetchall():
            print("Processing job_id={} and task_index={}".format(*result))
            query = "SELECT job_id, task_index, MIN(start_time), MAX(end_time), SUM(end_time - start_time), COUNT(*), AVG(cpu_rate), AVG(assigned_memory_usage), AVG(disk_io_time), AVG(local_disk_space_usage), STDEV(cpu_rate), STDEV(assigned_memory_usage), STDEV(disk_io_time), STDEV(local_disk_space_usage), VARIANCE(cpu_rate), VARIANCE(assigned_memory_usage), VARIANCE(disk_io_time), VARIANCE(local_disk_space_usage), MEDIAN(cpu_rate), MEDIAN(assigned_memory_usage), MEDIAN(disk_io_time), MEDIAN(local_disk_space_usage), MAX(maximum_cpu_rate), MAX(assigned_memory_usage), MAX(maximum_disk_io_time), MAX(local_disk_space_usage) from task_usage WHERE job_id = ? AND task_index = ?"
            cur.execute(query, result)
            res = cur.fetchone()
            query = "INSERT INTO task_usage_summary (job_id, task_index, start_time, end_time, task_duration, number_of_entries, avg_cpu_rate, avg_memory_usage, avg_disk_io_time, avg_disk_space_usage, stdev_cpu_rate, stdev_memory_usage, stdev_disk_io_time, stdev_disk_space_usage, var_cpu_rate, var_memory_usage, var_disk_io_time, var_disk_usage_space, median_cpu_rate, median_memory_usage, median_disk_io_time, median_disk_space_usage, max_cpu_time, max_memory_usage, max_disk_io_time, max_disk_usage_space) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
            cur.execute(query, res)

    print("Done")

if __name__ == '__main__':
    create_task_usage_db(DB_FILENAME, TASK_USAGE_DIR, PART_START, PART_END)
    create_data_summary(DB_FILENAME, RESULT_FILE)
