rm(list=ls())

library("rmr2")
rmr.options(backend="local") # comment to run in hadoop cluster

source(file="/home/creggian/R/RHadoop/lib/lib.R")

## create the dataset
N <- 10   # number of rows
n <- 100  # number of columns

m <- array(rnorm(N*n),c(N,n))
m.dfs <- to.dfs(m)

## mapreduce
sum <- mr.sum(data=m.dfs) # looking for column's means

## if the dataset is a csv file stored in HDFS, then run this code instead
# hdfs.path <- "path/to/dataset.csv"
# input.format <- make.input.format(format="csv", sep=",")
# sum <- mr.sum(data=hdfs.path, input.format=input.format)

## post-processing
identical(colSums(m), sum$sum)
sum(abs(sum$mean - colMeans(m))) < 0.0000000001