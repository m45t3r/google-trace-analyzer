#!/usr/bin/env python

from __future__ import print_function

import csv
import gzip
import subprocess
import sqlite3
import os

PART_START = 0
PART_END = 49
TASK_USAGE_DIR = '../task_usage/'
DB_FILENAME = 'task_usage-part-' + str(PART_START).zfill(5) + '-of-' + str(PART_END).zfill(5) + '.sqlite3'
EXPORT_DIR = './traces'
SUMMARY_FILE = DB_FILENAME.split('.')[0] + '-summary.csv'


class TaskUsageUtils(object):
    def __init__(self, db_filename):

        self.db_filename = db_filename
        self.conn = sqlite3.connect(self.db_filename)
        self.cur = self.conn.cursor()

    def import_data(self, task_usage_dir, start, end):
        # Create a sqlite database to hold task_usage data
        query = "CREATE TABLE IF NOT EXISTS task_usage (start_time INTEGER, end_time INTEGER, job_id INTEGER, task_index INTEGER, machine_id INTEGER, cpu_rate FLOAT, canonical_memory_usage FLOAT, assigned_memory_usage FLOAT, unmapped_page_cache FLOAT, total_page_cache FLOAT, maximum_memory_usage FLOAT, disk_io_time FLOAT, local_disk_space_usage FLOAT, maximum_cpu_rate FLOAT, maximum_disk_io_time FLOAT, cycles_per_instruction FLOAT, memory_accesses_per_instruction FLOAT, sample_portion FLOAT, aggregation_type BOOLEAN, part INTEGER)"
        self.cur.execute(query)

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
                    self.cur.execute(query, row)
            
            # Commit the data before going to the next file, so in case of problems
            # you can just resume from the last good file (in theory at least).
            self.conn.commit()
        
        # Optimize search for job_id and task_index fields, since they're the
        # most used ones.
        print("Creating index for job_id and task_index")
        self.cur.execute("CREATE INDEX job_id_and_task_index ON task_usage(job_id,task_index)")

        # Connection to database is closed, everything should be ok.
        print("Done")

    def create_data_summary(self):
        # Necessary to add STDEV and VARIANCE support to sqlite
        # See "extension-functions.c" (https://www.sqlite.org/contrib) for details.
        try:
            self.conn.enable_load_extension(True)
            self.conn.load_extension('./libsqlitefunctions.so')
        except sqlite3.OperationalError:
            sys.exit("libsqlitefunctions.so not found OR "
                     "your SQLite setup doesn't support external extensions!")
        
        # Create a sqlite database to hold task_usage data
        query = "CREATE TABLE IF NOT EXISTS task_usage_summary (job_id INTEGER, task_index INTEGER, start_time FLOAT, end_time FLOAT, task_duration FLOAT, number_of_entries INTEGER, avg_cpu_rate FLOAT, avg_memory_usage FLOAT, avg_disk_io_time FLOAT, avg_disk_space_usage FLOAT, stdev_cpu_rate FLOAT, stdev_memory_usage FLOAT, stdev_disk_io_time FLOAT, stdev_disk_space_usage FLOAT, var_cpu_rate FLOAT, var_memory_usage FLOAT, var_disk_io_time FLOAT, var_disk_usage_space FLOAT, median_cpu_rate FLOAT, median_memory_usage FLOAT, median_disk_io_time FLOAT, median_disk_space_usage FLOAT, max_cpu_time FLOAT, max_memory_usage FLOAT, max_disk_io_time FLOAT, max_disk_usage_space FLOAT, PRIMARY KEY (job_id, task_index))"
        self.cur.execute(query)

        query = "SELECT DISTINCT job_id,task_index FROM task_usage WHERE cpu_rate != '' AND disk_io_time != '' AND assigned_memory_usage != ''"
        self.cur.execute(query)
        print("Processing distinct tasks")

        for result in self.cur.fetchall():
            print("Processing job_id={} and task_index={}".format(*result))
            query = "SELECT job_id, task_index, MIN(start_time), MAX(end_time), SUM(end_time - start_time), COUNT(*), AVG(cpu_rate), AVG(assigned_memory_usage), AVG(disk_io_time), AVG(local_disk_space_usage), STDEV(cpu_rate), STDEV(assigned_memory_usage), STDEV(disk_io_time), STDEV(local_disk_space_usage), VARIANCE(cpu_rate), VARIANCE(assigned_memory_usage), VARIANCE(disk_io_time), VARIANCE(local_disk_space_usage), MEDIAN(cpu_rate), MEDIAN(assigned_memory_usage), MEDIAN(disk_io_time), MEDIAN(local_disk_space_usage), MAX(maximum_cpu_rate), MAX(assigned_memory_usage), MAX(maximum_disk_io_time), MAX(local_disk_space_usage) from task_usage WHERE job_id = ? AND task_index = ?"
            self.cur.execute(query, result)
            res = self.cur.fetchone()
            query = "INSERT INTO task_usage_summary (job_id, task_index, start_time, end_time, task_duration, number_of_entries, avg_cpu_rate, avg_memory_usage, avg_disk_io_time, avg_disk_space_usage, stdev_cpu_rate, stdev_memory_usage, stdev_disk_io_time, stdev_disk_space_usage, var_cpu_rate, var_memory_usage, var_disk_io_time, var_disk_usage_space, median_cpu_rate, median_memory_usage, median_disk_io_time, median_disk_space_usage, max_cpu_time, max_memory_usage, max_disk_io_time, max_disk_usage_space) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
            self.cur.execute(query, res)

        print("Done")

    def export_trace(self, job_id, task_index, output_dir='', limit_entries = None):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        trace_filename = 'task_usage-job_id_{}-task_index_{}_'.format(job_id, task_index)
        trace_filename = os.path.join(output_dir, trace_filename)
        extension = '.txt'

        print("Exporting job_id={} and task_index={} to pyCloudSim's .txt file".format(job_id, task_index))
        if limit_entries:
            self.cur.execute("SELECT cpu_rate, assigned_memory_usage, disk_io_time FROM task_usage WHERE job_id = ? AND task_index = ? LIMIT ?", (job_id, task_index, limit_entries))
        else:
            self.cur.execute("select cpu_rate, assigned_memory_usage, disk_io_time from task_usage where job_id = ? and task_index = ?", (job_id, task_index))

        with open(trace_filename + 'cpu' + extension, 'w') as cpu, open(trace_filename + 'mem' + extension, 'w') as mem, open(trace_filename + 'disk' + extension, 'w') as disk, open(trace_filename + 'net' + extension, 'w') as net:
            for row in self.cur.fetchall():
                # pyCloudSim uses % int, while Google's traces use a normalized
                # float value, so we fix it here
                # If there is no value (empty string or ''), we set it to 0
                try:
                    cpu.write(str(float(row[0] * 100)) + '\n')
                except ValueError:
                    print("Invalid CPU entry for task with job_id={} and task_index={}, using 0.0!".format(job_id, task_index), file=sys.stderr)
                    cpu.write("0.0\n")
                try:
                    mem.write(str(float(row[1] * 100)) + '\n')
                except ValueError:
                    print("Invalid MEMORY entry for task with job_id={} and task_index={}, using 0.0!".format(job_id, task_index), file=sys.stderr)
                    mem.write("0.0\n")
                try:
                    disk.write(str(float(row[2] * 100)) + '\n')
                except ValueError:
                    print("Invalid DISK entry for task with job_id={} and task_index={}, using 0.0!".format(job_id, task_index), file=sys.stderr)
                    disk.write("0.0\n")
		net.write("0.0\n")
        print("Done")

    def export_traces_from_csv_r(self, csv_file, output_dir='', limit_entries = None):
        with open(csv_file, mode='rt') as f:
            csv_reader = csv.reader(f, delimiter=',')
            # Skip header
            next(csv_reader)
            for row in csv_reader:
                self.export_trace(row[1], row[2], output_dir, limit_entries)

    def is_entry_valid(self, job_id, task_index):
        self.cur.execute("select cpu_rate, assigned_memory_usage, disk_io_time from task_usage where job_id = ? and task_index = ?", (job_id, task_index))

        is_valid = True
        for row in self.cur.fetchall():
            try:
                float(row[0])
                float(row[1])
                float(row[2])
            except ValueError:
                is_valid = False
                print("Task with job_id {} and task_index {} contain invalid entries!".format(job_id, task_index), file=sys.stderr)

        return is_valid

    def return_valid_tasks(self, csv_file, result_file):
        print("Checking if tasks from {} are all valid".format(csv_file))
        with open(csv_file, mode='rt') as f, open(result_file, mode='w') as r:
            r.write("job_id,task_index\n")
            csv_reader = csv.reader(f, delimiter=',')
            # Skip header
            next(csv_reader)
            for row in csv_reader:
                is_valid = self.is_entry_valid(row[1], row[2])
                if is_valid:
                    r.write("{},{}\n".format(row[1], row[2]))
        print("Done")


    def export_summary_to_csv(self, summary_file):
        print("Exporting task_usage_summary to .csv")
        with open(summary_file, mode='w') as f:
            subprocess.check_call(['sqlite3', '-csv', '-header', self.db_filename, "select * from task_usage_summary;"], stdout=f)
        print("Done")

    def analyze_summary_with_r(self, rscript_filename):
        print("Running {} script".format(rscript_filename))
        subprocess.check_call(['Rscript', rscript_filename])
        print("Done")

    def close_con(self):
        self.conn.commit()
        self.cur.close()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close_con()

if __name__ == '__main__':
    with TaskUsageUtils(DB_FILENAME) as task_usage:
        #task_usage.import_data(TASK_USAGE_DIR, PART_START, PART_END)
        #task_usage.create_data_summary()
        #task_usage.export_summary_to_csv(SUMMARY_FILE)
        #task_usage.analyze_summary_with_r('analyze-traces.r')
        task_usage.return_valid_tasks('filtered-cpu-29-40.csv', 'valid-entries.csv')
        #task_usage.export_traces_from_csv_r('filtered-cpu-29-40.csv', EXPORT_DIR)
