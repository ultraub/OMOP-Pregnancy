# Offline regression tests: Rscript tests/run_all.R  (from the repository root)
files <- c("tests/test_hip.R", "tests/test_pps_merge.R", "tests/test_esd_evidence.R", "tests/test_backend.R")
status <- 0
for (f in files) {
  cat("==", f, "\n")
  rc <- system2("Rscript", c("--vanilla", f), stdout = TRUE, stderr = TRUE)
  cat(paste0(grep("\\[(PASS|FAIL)\\]|error:", rc, value = TRUE), collapse = "\n"), "\n")
  if (any(grepl("\\[FAIL\\]|error:", rc)) || !is.null(attr(rc, "status"))) status <- 1
}
cat(if (status == 0) "ALL TESTS PASSED\n" else "SOME TESTS FAILED\n"); quit(status = status)
