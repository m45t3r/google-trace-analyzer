Analyze Google trace data
=========================

This combination of scripts downloads, filters and analyzes the contents of [Google trace data](https://code.google.com/p/googleclusterdata/wiki/ClusterData2011_2).

How to run
----------

Install ``gcc`` (``build-essential`` on Ubuntu), ``python2``, ``python2-pip``, ``sqlite3`` (including ``-dev`` packages) and R (``rbase`` on Ubuntu) and run the included script:

    $ ./run.sh

Note however that this process is **really slow** (in a reasonable recent CPU it may takes two or three days), and the script does no checks if some task is already completed. You may comment already completed tasks on ``TaskUsageUtils.py`` (just go in the ``main`` function and comment functions that already done their job) to speed-up the process.

The result will be available in the directory ``traces``. You may change the included R script (``analyze-traces.r``) to filter out other ranges of data. 
