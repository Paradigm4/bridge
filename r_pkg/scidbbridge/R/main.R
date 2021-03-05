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
