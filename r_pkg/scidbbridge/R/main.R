# BEGIN_COPYRIGHT
#
# Copyright (C) 2021 Paradigm4 Inc.
# All Rights Reserved.
#
# scidbbridge is a plugin for SciDB, an Open Source Array DBMS
# maintained by Paradigm4. See http://www.paradigm4.com/
#
# scidbbridge is free software: you can redistribute it and/or modify
# it under the terms of the AFFERO GNU General Public License as
# published by the Free Software Foundation.
#
# scidbbridge is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY
# KIND, INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See the
# AFFERO GNU General Public License for the complete license terms.
#
# You should have received a copy of the AFFERO GNU General Public
# License along with scidbbridge. If not, see
# <http://www.gnu.org/licenses/agpl-3.0.html>
#
# END_COPYRIGHT

#   Install Package:           'Ctrl + Shift + B'
#   Check Package:             'Ctrl + Shift + E'
#   Test Package:              'Ctrl + Shift + T'
library(R6)

#' Reads a table from S3 in arrow format and returns a dataframe

parse_url = function(url) {
  if (grepl(url, " ")) stop(paste0("Invalid URL '", url, "' - contains spaces"))

  url_parts <- scan(
    text=gsub("([^:]+)://([^/]*)(.*)", "\\1 \\2 \\3", url), what = "", sep = " ", quiet = TRUE)
  if (length(url_parts) != 3) stop(paste0("Invalid URL '", url, "' - cannot be parsed"))

  scheme <- url_parts[1]
  if (!(scheme %in% c("file", "s3"))) stop(paste0("Unsupported scheme '", scheme, "' in URL '", url, "'"))

  bucket <- url_parts[2]  # Empty for "file://" URLs
  path <- url_parts[3]
  if (scheme == "s3")
    path <- sub(".", "", path)  # Strip starting "/"

  return(list("scheme" = scheme,
              "bucket" = bucket,
              "path"   = path))
}

#' An R6 class representing an externally stored SciDB array
#'
Array <- R6Class(
  "Array",
  clone = F, portable = F,
  public = list(
    #' @field The URL for the bridge array
    url = NA,

    #' @description Create a new Array object
    #' @param url The URL for the Bridge Array
    initialize = function(url) {
      self$url = url
    },

    #' @description Get the Array's metadata
    metadata = function() {
      if(is.null(private$.metadata)) {
        private$.metadata = private$read_metadata()
      }

      return(private$.metadata)
    },

    #' @description Get the index for the bridge array
    read_index = function() {
      parts <- parse_url(self$url)

      keys = list()

      if (parts$scheme == "s3") {
        # Need to get all of the index arrays
        truncated = TRUE
        contToken = character(0)

        # Get the set of keys to read
        while(truncated) {
          objects = private$s3$list_objects_v2(Bucket = parts$bucket, Prefix = paste0(parts$path, "/index/"))

          truncated = objects$IsTruncated
          contToken = objects$NextContinuationToken

          keys = append(keys, lapply(objects$Contents, function(x) unlist(x[c('Key')])))
        }

        frames = lapply(keys, function(x) read_table_s3(bucket = parts$bucket, key = x))
      } else if (parts$scheme == "file") {
        idx_files = list.files(file.path(parts$path, "index"))

        frames = lapply(idx_files, function(x) read_table_fs(path = file.path(parts$path, "index", x)))
      }

      return(do.call(rbind, frames))
    },

    #' @description Gets a data.frame for a specified chunk
    #' @param ... One parameter for each column in the index file.
    get_chunk = function(...) {

      chunk_file = private$get_chunk_prefix(...)
      parts = parse_url(self$url)

      chunk_key = glue::glue("{parts$path}/chunks/{chunk_file}")
      gzipped = self$metadata()$compression == "gzip"

      if(parts$scheme == "s3") {
        return(private$read_table_s3(bucket = parts$bucket, key = chunk_key, gzipped = gzipped))
      } else {
        return(private$read_table_fs(path = chunk_key, gzipped = gzipped))
      }

    },

    #' @description Gets the schema string for the Bridge Array
    schema = function() {
      return(self$metadata()$schema)
    },

    #' @description Gets the dimensions of the array as a data.frame
    get_dimensions = function() {

      # Get the schema here for the object
      x = self$metadata()$schema

      x = gsub("\\t", " ", x)
      x = gsub("\\n", " ", x)
      tokenize = function(s, token)
      {
        x = strsplit(s, token)[[1]]
        x = as.vector(rbind(x, rep(token, length(x))))
        x[- length(x)]
      }

      diagram = function(tokens, labels=c())
      {
        if(length(tokens) == 0) return(labels)
        last = tail(labels, 1)
        prev = tail(labels, 2)[1]
        if(is.null(last))              labels = c(labels, "name")
        else if(tokens[1] == "=")      labels = c(labels, "equals")
        else if(tokens[1] == ";")      labels = c(labels, "semicolon")
        else if(tokens[1] == ":")      labels = c(labels, "colon")
        else if(tokens[1] == ",")      labels = c(labels, "comma")
        else
        {
          if(last == "semicolon")      labels = c(labels, "name")
          else if(last == "equals")    labels = c(labels, "start")
          else if(last == "colon")
          {
            if(is.null(prev))          stop("invalid : character")
            else if(prev == "start")   labels = c(labels, "end")
            else if(prev == "end")     labels = c(labels, "overlap")
            else if(prev == "overlap") labels = c(labels, "chunk")
          }
          else if(last == "comma")
          {
            if(is.null(prev))          stop("invalid , character")
            else if(prev == "name")    labels = c(labels, "name")
            else if(prev == "start")   labels = c(labels, "end")
            else if(prev == "end")     labels = c(labels, "chunk")
            else if(prev == "chunk")   labels = c(labels, "overlap")
            else if(prev == "overlap") labels = c(labels, "name")
          }
        }
        diagram(tokens[-1], labels)
      }
      form = function(x)
      {
        c(name=x["name"], start=x["start"], end=x["end"], chunk=x["chunk"], overlap=x["overlap"])
      }

      s = tryCatch(gsub("]", "", strsplit(x, "\\[")[[1]][[2]]), error=function(e) NULL)
      if(is.null(s) || nchar(s) == 0) return(NULL)
      tokens = Reduce(c, lapply(Reduce(c, lapply(Reduce(c, lapply(tokenize(s, "="), tokenize, ":")), tokenize, ";")), tokenize, ","))
      names(tokens) = diagram(tokens)
      tokens[!(names(tokens) %in% c("equals", "colon", "semicolon", "comma"))]
      i = which(names(tokens) %in% "name")
      j = c((i - 1)[-1], length(tokens))
      ans = Reduce(rbind, lapply(1:length(i), function(k) form(tokens[i[k]:j[k]])))
      if(length(i) == 1) {
        ans = data.frame(as.list(ans), stringsAsFactors=FALSE, row.names=NULL)
      } else ans = data.frame(ans, stringsAsFactors=FALSE, row.names=c())
      names(ans) = c("name", "start", "end", "chunk", "overlap")
      ans$name = gsub(" ", "", ans$name)
      return(ans)
    },

    #' Internal function for processing SciDB attribute schema
    #' @param x a scidb object or schema string
    #' @return a data frame with parsed attribute data
    .attsplitter = function(x)
    {
      if (is.character(x)) s = x
      else
      {
        if (!(inherits(x, "scidb"))) return(NULL)
        s = schema(x)
      }
      s = gsub("\\t", " ", s)
      s = gsub("\\n", " ", s)
      s = gsub("default[^,]*", "", s, ignore.case=TRUE)
      s = strsplit(strsplit(strsplit(strsplit(s, ">")[[1]][1], "<")[[1]][2], ",")[[1]], ":")
      # SciDB schema syntax changed in 15.12
      null = if (at_least(attr(x@meta$db, "connection")$scidb.version, "15.12"))
        ! grepl("NOT NULL", s)
      else grepl(" NULL", s)
      type = gsub(" ", "", gsub("null", "", gsub("not null", "", gsub("compression '.*'", "", vapply(s, function(x) x[2], ""), ignore.case=TRUE), ignore.case=TRUE), ignore.case=TRUE))
      data.frame(name=gsub("[ \\\t\\\n]", "", vapply(s, function(x) x[1], "")),
                 type=type,
                 nullable=null, stringsAsFactors=FALSE)
    }
  ),
  private = list(
    .metadata = NULL,
    s3 = paws::s3(),
    read_metadata = function() {
      parts <- parse_url(self$url)
      parts$path <- paste0(parts$path, "/metadata")

      if (parts$scheme == "s3") {
        req <- private$s3$get_object(Bucket = parts$bucket, Key = parts$path)
        df <- utils::read.table(text = rawToChar(req$Body), sep = "\t")
      }
      else if (parts$scheme == "file")
        df <- utils::read.table(parts$path, sep = "\t")

      schema <- split(df$V2, df$V1)
      return(schema)
    },
    read_table_s3 = function(bucket, key, gzipped = TRUE) {

      s3_obj = private$s3$get_object(Bucket = bucket, Key = key)

      if(gzipped) {
        stream = arrow::CompressedInputStream$create(arrow::BufferReader$create(s3_obj$Body), arrow::Codec$create("gzip"))
      } else {
        stream = arrow::BufferReader$create(s3_obj$Body)
      }

      reader = arrow::RecordBatchStreamReader$create(stream)
      return(as.data.frame(reader$read_table()))
    },

    read_table_fs = function(path, gzipped = TRUE) {
      if (gzipped) {
        stream = arrow::CompressedInputStream$create(path, arrow::Codec$create("gzip"))
      } else {
        stream = arrow::ReadableFile$create(path)
      }

      reader = arrow::RecordBatchStreamReader$create(stream)
      return(as.data.frame(reader$read_table()))
    },


    get_chunk_prefix = function(...) {
      # Get the coordinates
      coords = list(...)

      schema_dims = self$get_dimensions()

      all_match = all(names(coords) == schema_dims$name)
      stopifnot(names(coords) == schema_dims$name)

      dims = schema_dims
      dims$parts = unlist(coords)

      # TODO: The input needs to be checked to ensure it is valid.
      dims$parts = dims$parts - as.integer(dims$start)
      dims$parts = floor(dims$parts/as.integer(dims$chunk))

      chunk_str = paste(c("c", dims$parts), sep="", collapse="_")
      return(chunk_str)
    }
  )
)
