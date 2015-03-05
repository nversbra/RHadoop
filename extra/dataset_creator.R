rm(list=ls())
# source(file="/home/jpoullet/Rscript/testing/config.R") # config hadoop

## Create a dataset with the following dimension:
##  - nrow: 5,000,000
##  - ncol: 100,000
## for a total size of about 1T
##
## Properties:
##  - all columns are integer (discrete)
##  - columns are not scaled

nrow <- 750000
ncol <- 10000

batch <- 5000

set.seed(0)
for (i in 1:(nrow/batch)) {
  cat("Writing row: ", i, "\n")
  base <- sample(0:9, ncol/10, replace=TRUE)
  
  lines.list <- lapply(1:batch, function(idx) {
    columns <- lapply(0:9, function(x) {
      n <- ncol/10
      group <- (base + x + sample(c(0,1,2), n, replace=TRUE)) %% 10
      t(group)
    })
    do.call("cbind", columns)
  })
  lines <- do.call("rbind", lines.list)
  
  write.table(lines, file=pipe("hdfs dfs -appendToFile - dataset/artificial_20150224_750Kx10k_discrete.csv"),
              sep=",", col.names=FALSE, row.names=FALSE)
}

#mm <- read.table(file=pipe("hdfs dfs -cat dataset/artificial_20150116_5Mx100k_discrete.csv"), sep=",", header=FALSE)