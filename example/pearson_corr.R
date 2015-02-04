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
idx <- c(2,30,41,45,90) # calculate corr between a subset of columns
pcor <- mr.pcor(data=m.dfs, variable.idx=idx, mean=sum$mean[idx])

## if the dataset is a csv file stored in HDFS, then run this code instead
# hdfs.path <- "path/to/dataset.csv"
# input.format <- make.input.format(format="csv", sep=",")
# sum <- mr.sum(data=hdfs.path, input.format=input.format)
# idx <- c(2,30,41,45,90)
# pcor <- mr.pcor(data=hdfs.path, variable.idx=idx, mean=sum$mean[idx], input.format=input.format)

## post-processing
library("reshape2")
mr.m.melt <- data.frame(do.call("rbind", lapply(pcor$key, function(x) {unlist(strsplit(x, "[.]"))})),
                     value=pcor$val)
mr.m <- dcast(mr.m.melt, X1 ~ X2, value.var='value')
rownames <- mr.m[,1]
mr.m <- mr.m[,-1]
rownames(mr.m) <- rownames

mr.m # correlation matrix with mapreduce

# standard approach
cor(m[,idx])