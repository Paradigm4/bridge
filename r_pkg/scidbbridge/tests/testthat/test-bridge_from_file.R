test_that("Reading index from file works", {

  ar = Array$new("file:///tmp/test_bridge")
  index = ar$read_index()

  expected_output = data.frame(
    i = c(5L, 5L),
    j = c(10L, 15L)
  )

  # Not sure why as.data.frame has to be called (AGAIN)
  expect_equal(as.data.frame(index), expected_output)

})

test_that("Reading chunk from file works.", {
  ar = Array$new("file:///tmp/test_bridge")
  chunk_frame = ar$get_chunk(5, 15)[1:3,]

  expected_output = data.frame(
    v = c(20, 22, 24),
    w = c(400.0, 484.0, 576.0),
    i = c(5, 5, 5),
    j = c(15, 17, 19)
  )

  expect_equal(as.data.frame(chunk_frame), expected_output)

})
