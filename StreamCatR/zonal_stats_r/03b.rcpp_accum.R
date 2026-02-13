# 03b.rcpp_accum.R
ensure_rcpp_accum <- function(path, env = parent.frame()) {
  if (!file.exists(path)) stop("C++ file not found: ", path)
  # Compile into the requested environment (important in packages/workers)
  Rcpp::sourceCpp(file = path, env = env, rebuild = FALSE, verbose = FALSE)
  if (!exists("acc_sum_n_idx1K", envir = env, mode = "function"))
    stop("acc_sum_n_idx1K not found after compile")
  if (!exists("acc_sum_n_map_centers1K", envir = env, mode = "function"))
    stop("acc_sum_n_map_centers1K not found after compile")
  invisible(TRUE)
}