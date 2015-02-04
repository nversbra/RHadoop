rm(list=ls())

library("rmr2")
rmr.options(backend="local") # comment to run in hadoop cluster

source(file="/home/creggian/R/RHadoop/lib/lib.R")

## create the dataset
# this version of MI in mapreduce works only with discrete values
m <- create_discrete_dataset()
m.dfs <- to.dfs(m)

## mapreduce
# class is column 1, features are in column 2-9
# feature selection of 4 variables
mrmr <- mr.mrmr(data=m.dfs, target.idx=c(1), variable.idx=c(2:9), k=4)

## if the dataset is a csv file stored in HDFS, then run this code instead
# hdfs.path <- "path/to/dataset.csv"
# input.format <- make.input.format(format="csv", sep=",")
# mrmr <- mr.mrmr(data=hdfs.path, target.idx=c(1), variable.idx=c(2:9), k=4, input.format=input.format)

sapply(mrmr, function(x) x$selected) # feature selected idx
sapply(mrmr, function(x) x$selected.mrmr)

# standard approach
library("mRMRe")
dd <- mRMR.data(data=as.data.frame(m[,c(1,2:9)]))
mrmr.classic <- mRMR.classic(data=dd, target_indices=c(1), feature_count=4)
idx <- solutions(mrmr.classic)[[1]][,1]

# Note: mapreduce and standard results are different