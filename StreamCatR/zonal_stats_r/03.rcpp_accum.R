# install.packages("Rcpp")  # if needed
library(Rcpp)

Rcpp::sourceCpp(code = '
#include <Rcpp.h>
using namespace Rcpp;

// [[Rcpp::export]]
void acc_sum_n_idx1K(IntegerVector idx,
                     NumericVector v,
                     NumericVector sumv,
                     IntegerVector n) {

  const R_xlen_t N = idx.size();
  const int K = sumv.size();

  // idx is assumed 1..K (with NA), v is numeric (with NA).
  for (R_xlen_t i = 0; i < N; ++i) {
    int g = idx[i];
    if (g == NA_INTEGER) continue;

    // guard against unexpected values
    if (g < 1 || g > K) continue;

    double x = v[i];
    if (NumericVector::is_na(x)) continue;

    const int j = g - 1;      // 0-based for C++
    sumv[j] += x;
    n[j] += 1;
  }
}
')
