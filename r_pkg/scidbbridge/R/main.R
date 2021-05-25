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

#' A Reference Class to represent an externally stored SciDB array.
#'
#' @field url URL to the SciDB array
Array <- setRefClass(
  "Array",
  fields = c(
    "url",
    ".metadata"),

  methods = list(
    initialize = function(url) {
      url <<- url
      .metadata <<- NULL
    },

    metadata = function() {
      if (is.null(.metadata)) {
        print("Reading metadata")
        .metadata <<- .read_metadata(url)
      }
      return(.metadata)
    },

    schema = function() {
      return(metadata()$schema)
    },

    index = function() {
      return(.read_index(url), .metadata)
    }
  )
)

.scidbbridge <- new.env()

.s3 = function() {
  if (!exists("s3", .scidbbridge)) {
    print("Create S3 client")
    assign("s3", paws::s3(), .scidbbridge)
  }
  return(.scidbbridge$s3)
}

.parse_url = function(url) {
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

.read_metadata = function(url) {
  parts <- .parse_url(url)
  parts$path <- paste0(parts$path, "/metadata")

  if (parts$scheme == "s3") {
    req <- .s3()$get_object(Bucket = parts$bucket, Key = parts$path)
    df <- utils::read.table(text = rawToChar(req$Body), sep = "\t")
  }
  else if (parts$scheme == "file")
    df <- utils::read.table(parts$path, sep = "\t")

  schema <- split(df$V2, df$V1)
  return(schema)
}

# Extracts a table from a bucket/key combo
.read_table = function(bucket, key, compression = FALSE) {
  s3_obj = s3$get_object(Bucket = "bmsrd-ngs-64223", Key = "incoming/VariantWarehouse/bridge-test/chr22/index/0")

  # Set the compression if we need to (only using gzip for now)
  if(compression) {
    stream = arrow::CompressedInputStream$create(arrow::BufferReader$create(s3_obj$Body), arrow::Codec$create("gzip"))
  } else {
    stream = arrow::CompressedInputStream$create(arrow::BufferReader$create(s3_obj$Body))
  }

  reader = arrow::RecordBatchStreamReader$create(stream)
}

.read_index = function(url, metadata) {

  parts <- .parse_url(url)

  keys = list()

  if (parts$scheme == "s3") {
    # Need to get all of the index arrays
    truncated = TRUE
    contToken = character(0)

    # Get the set of keys to read
    while(truncated) {
      objects = s3$list_objects_v2(Bucket = parts$bucket, Prefix = parts$path, ContinuationToken = contToken)
      truncated = objects$IsTruncated
      contToken = objects$NextContinuationToken

      append(keys, lapply(objects$Contents, function(x) unlist(x[c('Key')])))
    }

    return(rbind(lapply(function(x) .read_table_s3(bucket, x), keys)))

  } else if (parts$scheme == "file") {
    stop("Local files are yet to be implemented.")
  }

}

# Read in a table in Arrow format, returning a data.frame
.read_table_s3 <- function(bucket, key, gzipped = FALSE) {

  s3_obj = .s3$get_object(Bucket = bucket, Key = key)
  if(gzipped) {
    stream = arrow::CompressedInputStream(arrow::BufferReader$create(s3_obj$body), arrow::Codec$create("gzip"))
  } else {
    stream = arrow::InputStream(arrow::BufferReader$create(s3_obj$body))
  }

  reader = arrow::RecordBatchStreamReader$create(stream)
  return(as.data.frame(reader$read_table()))
}
