rm(list=ls())

library("rmr2")
rmr.options(backend="local") # comment to run in hadoop cluster

source(file="/home/creggian/R/RHadoop/lib/lib.R")

## create the dataset
# this version of MI in mapreduce works only with discrete values
m <- create_discrete_dataset()
m.dfs <- to.dfs(m)

## mapreduce
# calculate mutual information between var 1 and 2,6,8,4,9
mi <- mr.mi(data=m.dfs, target.idx=c(1), variable.idx=c(2,6,8,4,9))

## if the dataset is a csv file stored in HDFS, then run this code instead
# hdfs.path <- "path/to/dataset.csv"
# input.format <- make.input.format(format="csv", sep=",")
# mi <- mr.mi(data=hdfs.path, target.idx=c(1), variable.idx=c(2,6,8,4,9), input.format=input.format)

## post-processing
library("reshape2")
mr.m.melt <- data.frame(do.call("rbind", lapply(mi$key, function(x) {unlist(strsplit(x, "[.]"))})),
                        value=mi$val)
mr.m <- dcast(mr.m.melt, X1 ~ X2, value.var='value')
rownames <- mr.m[,1]
mr.m <- mr.m[,-1]
rownames(mr.m) <- rownames

mr.m # mi with mapreduce

# standard approach
library("infotheo")
mutinformation(m[,8], m[,1])