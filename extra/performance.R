rm(list=ls())

library("rmr2")
rmr.options(backend="hadoop") # comment to run in hadoop cluster

#source(file="/home/jpoullet/Rscript/testing/config.R") # config hadoop
source(file="/home/creggian/R/RHadoop/lib/lib.R")

hdfs.paths <- c("dataset/artificial_20150224_50Kx10k_discrete.csv",
                "dataset/artificial_20150224_250Kx10k_discrete.csv",
                "dataset/artificial_20150224_500Kx10k_discrete.csv",
                "dataset/artificial_20150224_750Kx10k_discrete.csv",
                "dataset/artificial_20150224_1Mx10k_discrete.csv",
                "dataset/artificial_20150224_2Mx10k_discrete.csv")

nvar <- 750

perf.res <- lapply(hdfs.paths, function(hdfs.path) {
  input.format <- make.input.format(format="csv", sep=",")
  cat("Testing dataset: ", hdfs.path, "\n", sep="")
  pt <- system.time(mr.mi(data=hdfs.path, target.idx=c(1:nvar), variable.idx=c(1:nvar), input.format=input.format))
  out <- list(path=hdfs.path, pt=pt)
  save(out, file=paste("performance_f", nvar, "_", format(Sys.time(), "%Y.%b.%d.%X"), ".RData", sep=""))
})