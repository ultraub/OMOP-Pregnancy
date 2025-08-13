#!/usr/bin/env Rscript

# Check for undefined exports in NAMESPACE

# Read NAMESPACE
namespace_lines <- readLines("NAMESPACE")

# Extract exports
exports <- grep("^export\\(", namespace_lines, value = TRUE)
exports <- gsub("export\\((.*)\\)", "\\1", exports)

cat("Checking", length(exports), "exports...\n\n")

# Source all R files to get defined functions
r_files <- list.files("R", pattern = "\\.R$", full.names = TRUE)

# Get all defined functions
all_functions <- character()
for (file in r_files) {
  content <- readLines(file)
  # Look for function definitions
  func_defs <- grep("^[a-zA-Z_][a-zA-Z0-9_]*\\s*<-\\s*function", content, value = TRUE)
  func_names <- gsub("^([a-zA-Z_][a-zA-Z0-9_]*)\\s*<-.*", "\\1", func_defs)
  all_functions <- c(all_functions, func_names)
}

# Find undefined exports
undefined <- setdiff(exports, all_functions)

if (length(undefined) > 0) {
  cat("❌ UNDEFINED EXPORTS:\n")
  for (func in undefined) {
    cat("  -", func, "\n")
  }
} else {
  cat("✅ All exports are defined!\n")
}

# Find defined but not exported
not_exported <- setdiff(all_functions, exports)
cat("\n📝 Functions defined but not exported:\n")
if (length(not_exported) > 0) {
  for (func in not_exported) {
    cat("  -", func, "\n")
  }
} else {
  cat("  (none)\n")
}

cat("\n✅ Defined functions that ARE exported:\n")
exported_and_defined <- intersect(exports, all_functions)
cat("Total:", length(exported_and_defined), "\n")