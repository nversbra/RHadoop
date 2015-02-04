##### Utils #####

merge_table <- function(t1, t2) {
  col <- union(colnames(t1), colnames(t2))
  row <- union(rownames(t1), rownames(t2))
  m <- matrix(0, nrow=length(row), ncol=length(col))
  colnames(m) <- col
  rownames(m) <- row
  m[rownames(t1), colnames(t1)] <- m[rownames(t1), colnames(t1)] + t1
  m[rownames(t2), colnames(t2)] <- m[rownames(t2), colnames(t2)] + t2
  m
}

## @param   vars    vector of string
mr.param.backup <- function(vars, envir=globalenv()) {
  vars <- as.list(vars)
  lapply(vars, function(x) {
    update <- FALSE
    backup <- NULL
    if (exists(x, envir=envir)) {
      update <- TRUE
      backup <- get(x, envir=envir)
    }
    list(var=x, update=update, backup=backup)
  })
}

## @param   param   list returned from 'mr.param.backup' function
mr.param.restore <- function(param, envir=globalenv()) {
  lapply(param, function(x) {
    if (x$update) {
      assign(x$var, x$backup, envir=envir)
    } else {
      rm(list=c(x$var), envir=envir)
    }
  })
  NULL
}

create_discrete_dataset <- function() {
  nrow <- 1000
  levels <- c(1,2,3,4,5,6,7,8,9)
  x1  <- sample(levels, size=nrow, replace=TRUE)
  x2  <- x1 + round(runif(nrow, min=-2, max=2))
  x3  <- x1 + round(runif(nrow, min=-4, max=4))
  x4  <- x1 + round(runif(nrow, min=-6, max=6))
  x5  <- x1 + round(runif(nrow, min=-0, max=6))
  x6  <- x4 + round(runif(nrow, min=-1, max=1))
  x7  <- x4 + x2
  x8  <- x1 + x2
  x9  <- sample(levels, size=nrow, replace=TRUE)
  x10 <- sample(levels, size=nrow, replace=TRUE)
  m <- cbind(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10)
  colnames(m) <- NULL
  m
}

##### MapReduce high-level functions #####

mr.sum <- function(data, ...) {
  from.dfs.sum <- from.dfs(
    mapreduce(
      input = data,
      map = sum.map,
      reduce = sum.reduce,
      ...
    )
  )
  mean <- from.dfs.sum$val / from.dfs.sum$key
  list(sum=from.dfs.sum$val, mean=mean)
}

mr.mi <- function(data, target.idx, variable.idx, ...) {
  opar <- mr.param.backup(vars=c("target.idx", "variable.idx"), envir=globalenv())
  
  assign("target.idx", target.idx, envir=globalenv())
  assign("variable.idx", variable.idx, envir=globalenv())

  from.dfs.res <- from.dfs(
    mapreduce(
      input = data,
      map = mi.map,
      reduce = mi.reducer,
      ...
    )
  )
  
  mr.param.restore(param=opar, envir=globalenv())
  
  from.dfs.res
}

mr.pcor <- function(data, variable.idx, mean, ...) {
  opar <- mr.param.backup(vars=c("mean", "variable.idx"), envir=globalenv())
  
  assign("mean", mean, envir=globalenv())
  assign("variable.idx", variable.idx, envir=globalenv())
  
  from.dfs.res <- from.dfs(
    mapreduce(
      input = data,
      map = pcor.map,
      reduce = pcor.reducer,
      ...
    )
  )
  
  mr.param.restore(param=opar, envir=globalenv())
  
  from.dfs.res
}

mr.mrmr <- function(data, target.idx, variable.idx, k, ...) {
  opar <- mr.param.backup(vars=c("mrmr.selected.idx", "mrmr.candidate.idx"), envir=globalenv())
  
  assign("mrmr.selected.idx", target.idx, envir=globalenv())
  assign("mrmr.candidate.idx", variable.idx, envir=globalenv())
  
  res <- list()
  for(i in 1:k) {
    from.dfs.res <- from.dfs(
      mapreduce(
        input = data,
        map = mrmr.map,
        reduce = mrmr.reducer,
        ...
      )
    )
    candidate <- keys(from.dfs.res)
    mrmr <- values(from.dfs.res)
    
    selected <- candidate[which.max(mrmr)]
    mrmr.selected.idx <- c(mrmr.selected.idx, selected)
    mrmr.candidate.idx <- mrmr.candidate.idx[mrmr.candidate.idx != selected]
    
    assign("mrmr.selected.idx", mrmr.selected.idx, envir=globalenv())
    assign("mrmr.candidate.idx", mrmr.candidate.idx, envir=globalenv())
    
    res[[length(res)+1]] <- list(candidate=candidate,
                                 mrmr=mrmr,
                                 selected=selected,
                                 selected.mrmr=max(mrmr))
  }
  
  mr.param.restore(param=opar, envir=globalenv())
  
  res
}

##### Mapper and Reducer functions #####

## @param   variable.idx
## @param   mean           column means vector
pcor.map <- function(key, Xi) {
  subset <- Xi[,variable.idx]
  err <- t(apply(subset, 1, function(x) {x-mean}))
  
  squared.err <- colSums(err^2, na.rm=TRUE)
  c.keyval(
    lapply(seq_along(variable.idx), function(i) {
      ncov.i <- sapply(seq_along(variable.idx), function(j) {
        #if (i != j) sum(err[,i] * err[,j])}
        sum(err[,i] * err[,j], na.rm=TRUE)
      })
      keyval(key=i,
             val=list(list(squared.err=squared.err,
                           ncov.i=ncov.i)))
    })
  )
}

## @param   pcor.variable.idx
pcor.reducer <- function(key, YY) {
  YY.squared.err <- lapply(YY, function(x) x$squared.err)
  YY.ncov.i <- lapply(YY, function(x) x$ncov.i)
  
  squared.err <- Reduce("+", YY.squared.err)
  ncov.i <- Reduce("+", YY.ncov.i)
  
  c.keyval(
    lapply(seq_along(variable.idx), function(i) {
      rho <- ncov.i[i] / ( (sqrt(squared.err[key])) * (sqrt(squared.err[i])) )
      keyval(key=paste(variable.idx[key], variable.idx[i], sep="."), val=rho)
    })
  )
}

sum.map <- function(key, Xi) {
  nrow <- nrow(Xi)
  sum <- apply(m, 2, sum, na.rm=TRUE)
  keyval(key=1, val=list(list(n=nrow, sum=sum)))
}

sum.reduce <- function(key, YY) {
  YY.nrow <- lapply(YY, function(x) x$n)
  YY.sum <- lapply(YY, function(x) x$sum)
  
  nrow.total <- Reduce("+", YY.nrow)
  sum.total <- Reduce("+", YY.sum)
  
  keyval(nrow.total, sum.total)
}

mi.map <- function(key, Xi) {
  subset <- Xi[,c(target.idx, variable.idx)]
  f <- lapply(data.frame(subset), factor)
  names(f) <- NULL
  df <- do.call("cbind.data.frame", f)
  colnames(df) <- paste("V", c(target.idx, variable.idx), sep="")
  
  c.keyval(
    unlist(lapply(seq_along(target.idx), function(i) {
      lapply(seq_along(variable.idx), function(j) {
        keyval(paste(target.idx[i], ".", variable.idx[j], sep=""),
               list(table(df[,c(i,j+length(target.idx))])))
      })
    }), recursive=FALSE, use.names=FALSE)
  )
}

mi.reducer <- function(key, YY) {
  sum <- Reduce("merge_table", YY)
  freq <- sum/sum(sum, na.rm=TRUE)
  v1.marginal <- rowSums(freq)
  v2.marginal <- colSums(freq)
  mi <- sum(freq*log(t(t(freq/v1.marginal)/v2.marginal)), na.rm=TRUE)
  
  keyval(key, mi)
}

mrmr.map <- function(key, Xi) {
  subset <- Xi[,c(mrmr.selected.idx, mrmr.candidate.idx)]
  f <- lapply(data.frame(subset), factor)
  names(f) <- NULL
  df <- do.call("cbind.data.frame", f)
  colnames(df) <- paste("V", c(mrmr.selected.idx, mrmr.candidate.idx), sep="")
  
  c.keyval(
    unlist(lapply(seq_along(mrmr.selected.idx), function(i) {
      lapply(seq_along(mrmr.candidate.idx), function(j) {
        keyval(key=mrmr.candidate.idx[j],
               val=list(list(target=mrmr.selected.idx[i],
                             table=table(df[,c(i,j+length(mrmr.selected.idx))]))))
      })
    }), recursive=FALSE, use.names=FALSE)
  )
}

mrmr.reducer <- function(key, YY) {
  YY.tables <- lapply(YY, function(x) {x$table})
  s.idx <- sapply(YY, function(x) {x$target})
  s.idx.u <- unique(s.idx)
  
  l.s.idx <- lapply(s.idx.u, function(i) {which(s.idx == i)})
  names(l.s.idx) <- NULL
  
  mi.target <- values(mi.reducer(NULL, YY.tables[unlist(l.s.idx[s.idx.u == mrmr.selected.idx[1]])]))
  mi.variables <- sapply(l.s.idx[s.idx.u != mrmr.selected.idx[1]], function(idx) {
    values(mi.reducer(NULL, YY.tables[idx]))
  })
  
  m <- length(mi.variables)
  if (m < 1) {
    mrmr <- mi.target
  } else {
    mrmr <- mi.target - (1/m) * sum(mi.variables)
  }

  keyval(key, mrmr)
}
