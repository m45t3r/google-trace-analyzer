#!/usr/bin/env Rscript

calculate_thresholds <- function(value, threshold) {
    # http://stackoverflow.com/questions/1826519/function-returning-more-than-one-value
    threshold <- 0.00010
    lb <- value*(1-threshold)
    ub <- value*(1+threshold)
    result <- data.frame(lower=lb, upper=ub)
    return(result)
}

filter_between <- function(dataframe, column, lb, ub) {
    # Dataframe is the dataframe
    # Column is the column to filter
    # lb is the lower bound
    # ub is the upper bound
    condition_filter <- column >= lb & column <= ub
    result <- subset(dataframe, condition_filter)
    return(result)
}

plot_all <- function(column, column_title) {
    fn <- paste("plot-density-", column_title, ".png", sep = "")
    png(filename=fn)
    plot(density(column), main='')
    dev.off()
    
    fn <- paste("plot-", column_title, ".png", sep = "")
    png(filename=fn)
    plot(column)
    dev.off()
    
    fn <- paste("plot-bar-", column_title, ".png", sep = "")
    png(filename=fn)
    barplot(column)
    dev.off()
    
    fn <- paste("plot-hist-", column_title, ".png", sep = "")
    png(filename=fn)
    hist(column, main='')
    dev.off()
    
    #pie(column)
}

filename <- "task_usage-part-00000-of-00049-summary.csv"
data.raw <- read.csv(filename)
#colnames(data) <- c("job_id", "task_index", "start_time", "end_time", "task_duration", "number_of_entries", "a")
#states <- unique(data$State)

column <- data.raw$avg_cpu_rate
plot_all(column, "cpu")

# Q1, Q3, sigma1, sigma2
data.cpu.summary = summary(column)
#mean_avg_cpu_rate <- mean(column, na.rm = T)
#min_avg_cpu_rate <- min(column, na.rm = T)
#max_avg_cpu_time <- max(column, na.rm = T)
#data.cpu.summary['1st Qu.']
m <- data.cpu.summary['Mean']
b <- calculate_thresholds(m, t)
data.avg_cpu_rate_mean_range <- filter_between(data.raw, column, 0.29, 0.40)
#data.avg_cpu_rate_mean_range <- filter_between(data.raw, column, b$lower, b$upper)
column <- data.avg_cpu_rate_mean_range$avg_cpu_rate
plot_all(column, "cpu-29-40")

fn <- paste("filtered-", "cpu-29-40", ".csv", sep = "")
write.csv(file=fn, x=data.avg_cpu_rate_mean_range)

column <- data.raw$max_cpu_time
mean_max_cpu_time <- mean(column, na.rm = T)
min_max_cpu_time <- min(column, na.rm = T)
max_max_cpu_time <- max(column, na.rm = T)

#data.cpu$job_id
#data.cpu$task_index

column <- data.raw$avg_memory_usage
plot_all(column, "mem-usage")

column <- data.raw$avg_disk_io_time
plot_all(column, "disk-time")

column <- data.raw$avg_disk_space_usage
plot_all(column, "disk-usage")

plot(density(data.raw$stdev_cpu_rate))
plot(density(data.raw$var_cpu_rate))

#Number_of_entries??

# Resources:
# Plots: 
# http://www.stat.berkeley.edu/~s133/R-3a.html
# https://www.stat.auckland.ac.nz/~paul/RGraphics/rgraphics.html
