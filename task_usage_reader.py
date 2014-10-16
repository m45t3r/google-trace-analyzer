#!/usr/bin/env python

import csv
import gzip

# See schema.csv file for details
TRACE_USAGE_VALUES = ['start_time',
                      'end_time',
                      'job_id',
                      'task_index',
                      'machine_id',
                      'cpu_rate',
                      'canonical_memory_usage',
                      'assigned_memory_usage',
                      'unmapped_page_cache',
                      'total_page_cache',
                      'maximum_memory_usage',
                      'disk_io_time',
                      'local_disk_space_usage',
                      'maximum_cpu_rate',
                      'maximum_disk_io_time',
                      'cycles_per_instruction',
                      'memory_access_per_instruction',
                      'sample_portion',
                      'aggregation_type',]

def google_trace_usage_reader(fname):
    with gzip.open(fname, mode='rt') as f:
        csv_reader = csv.reader(f, delimiter=',')
        for row in csv_reader:
            yield dict(zip(TRACE_USAGE_VALUES, row))

def print_task(task_list, job_id, task_index):
    print("Job ID: {}; Task Index: {}".format(job_id, task_index))
    for i in task_list:
        if job_id != i['job_id'] or task_index != i['task_index']:
          continue
        print("cpu: {}, mem: {}, disk: {}".format(i['cpu_rate'], i['canonical_memory_usage'], i['disk_io_time']))

def get_job_list(task_list):
    job_list = []
    for i in task_list:
        if i['job_id'] not in job_list:
            job_list.append(i['job_id'])
            print(i['job_id'])

def get_highest_cpu(task_list):
    highest_cpu = 0.0
    job = None
    for i in task_list:
        if float(i['cpu_rate']) > highest_cpu:
            highest_cpu = float(i['cpu_rate'])
            job = i
    print(job)

if __name__ == '__main__':
    task_list = google_trace_usage_reader('task_usage/part-00001-of-00500.csv.gz')
    #get_job_list(task_list)
    print_task(task_list, '3418309', '0')
    #print_task(task_list, '5911025686', '0')
    #print_task(task_list, '6119300167', '322')
    #get_highest_cpu(task_list)
