test_that("parsing URLs works", {
  url_1 = "s3://p4-test/test_path"
  url_2 = "file:///path/to/test_path"

  parts_1 = parse_url(url_1)
  parts_2 = parse_url(url_2)

  # Schemes
  expect_equal(parts_1$scheme, "s3")
  expect_equal(parts_2$scheme, "file")

  # Buckets (file should be empty string)
  expect_equal(parts_1$bucket, "p4-test")
  expect_equal(parts_2$bucket, "")

  # Path
  expect_equal(parts_1$path, "test_path")
  expect_equal(parts_2$path, "/path/to/test_path")

})
