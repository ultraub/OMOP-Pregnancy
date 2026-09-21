# Convert a Quarto document into Databricks notebook source format
#
# Databricks cannot render .qmd files, but a .r file that starts with
# "# Databricks notebook source" opens as a notebook. This converter keeps
# the .qmd as the single source of truth:
#   - markdown between chunks becomes "%md" cells
#   - each R chunk becomes a code cell titled with its label
#   - `#| eval: !expr <cond>` wraps the cell body in if (<cond>) { }
#   - `eval=FALSE` chunks are emitted commented out
#   - the YAML `params:` block becomes a `params <- list(...)` cell, with
#     values replaced by `overrides`; `preamble` cells are inserted after it
#
# Usage (from the repository root):
#   source("Databricks/qmd_to_notebook.r")
#   qmd_to_notebook("Evaluation/validation_report.qmd",
#                   "Evaluation/validation_report_notebook.r",
#                   overrides = list(connection_type = "spark"),
#                   preamble = c("setwd('/Workspace/.../Evaluation')"))

qmd_to_notebook <- function(qmd_path, out_path, overrides = list(), preamble = character(0),
                            title = NULL) {
  lines <- readLines(qmd_path, warn = FALSE)

  # ---- YAML front matter -------------------------------------------------
  yaml_end <- which(lines == "---")[2]
  yaml <- lines[2:(yaml_end - 1)]
  body <- lines[(yaml_end + 1):length(lines)]

  if (is.null(title)) {
    t <- grep("^title:", yaml, value = TRUE)
    title <- if (length(t)) gsub('^title:\\s*"?|"?\\s*$', "", t[1]) else basename(qmd_path)
  }

  # params: simple "  key: value" lines under "params:" (comments ignored)
  p_start <- grep("^params:", yaml)
  params <- list()
  if (length(p_start)) {
    for (l in yaml[(p_start + 1):length(yaml)]) {
      if (!grepl("^  ", l)) break
      if (grepl("^\\s*#", l) || !grepl(":", l)) next
      key <- trimws(sub(":.*$", "", l))
      val <- trimws(sub("^[^:]*:", "", l))
      val <- sub("\\s+#.*$", "", val)
      params[[key]] <- val
    }
  }
  for (k in names(overrides)) {
    v <- overrides[[k]]
    params[[k]] <- if (is.character(v)) sprintf('"%s"', v) else as.character(v)
  }
  params_cell <- c(
    "# Parameters (from the .qmd YAML, with Databricks overrides applied)",
    "params <- list(",
    paste0("  ", names(params), " = ", unlist(params), c(rep(",", length(params) - 1), "")),
    ")"
  )

  # ---- cells --------------------------------------------------------------
  cells <- list()
  add_md <- function(txt) {
    txt <- sub("^\\s+$", "", txt)
    while (length(txt) && txt[1] == "") txt <- txt[-1]
    while (length(txt) && txt[length(txt)] == "") txt <- txt[-length(txt)]
    if (length(txt)) cells[[length(cells) + 1]] <<- c("# MAGIC %md", ifelse(txt == "", "# MAGIC", paste("# MAGIC", txt)))
  }
  add_code <- function(label, code, eval_cond = NULL, disabled = FALSE) {
    while (length(code) && code[length(code)] == "") code <- code[-length(code)]
    if (disabled) {
      code <- c("# (eval=FALSE in the source document; run by hand if needed)", paste("#", code))
    } else if (!is.null(eval_cond)) {
      code <- c(sprintf("if (%s) {", eval_cond), paste0("  ", code), "}")
    }
    cells[[length(cells) + 1]] <<- c(sprintf("# DBTITLE 1,%s", label), code)
  }

  i <- 1; md_buf <- character(0)
  while (i <= length(body)) {
    l <- body[i]
    if (grepl("^```\\{r", l)) {
      add_md(md_buf); md_buf <- character(0)
      header <- sub("^```\\{r\\s*", "", sub("\\}\\s*$", "", l))
      label <- trimws(strsplit(header, ",")[[1]][1]); if (label == "") label <- sprintf("chunk-%d", length(cells) + 1)
      disabled <- grepl("eval\\s*=\\s*FALSE", header)
      j <- i + 1; code <- character(0); eval_cond <- NULL
      while (j <= length(body) && !grepl("^```\\s*$", body[j])) {
        cl <- body[j]
        if (grepl("^#\\|", cl)) {
          m <- regmatches(cl, regexec("^#\\|\\s*eval:\\s*!expr\\s*(.*)$", cl))[[1]]
          if (length(m) == 2) eval_cond <- trimws(m[2])
          m2 <- regmatches(cl, regexec("^#\\|\\s*eval:\\s*(false|FALSE)\\s*$", cl))[[1]]
          if (length(m2) == 2) disabled <- TRUE
        } else {
          code <- c(code, cl)
        }
        j <- j + 1
      }
      add_code(label, code, eval_cond, disabled)
      i <- j + 1
    } else {
      md_buf <- c(md_buf, l); i <- i + 1
    }
  }
  add_md(md_buf)

  # ---- assemble -----------------------------------------------------------
  out <- c("# Databricks notebook source",
           "# MAGIC %md", paste("# MAGIC #", title),
           "# MAGIC", paste("# MAGIC Generated from", basename(qmd_path), "by Databricks/qmd_to_notebook.r; edit the .qmd, not this file."))
  sep <- c("", "# COMMAND ----------", "")
  out <- c(out, sep, params_cell)
  if (length(preamble)) out <- c(out, sep, c("# DBTITLE 1,Databricks setup", preamble))
  for (cell in cells) out <- c(out, sep, cell)
  writeLines(out, out_path)
  invisible(out_path)
}

