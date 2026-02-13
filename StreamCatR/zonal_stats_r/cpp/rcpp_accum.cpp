#include <Rcpp.h>
using namespace Rcpp;

// [[Rcpp::export]]
void acc_sum_n_idx1K(IntegerVector idx,
                     NumericVector v,
                     NumericVector sumv,
                     IntegerVector n) {
  const R_xlen_t N = idx.size();
  const int K = sumv.size();
  for (R_xlen_t i = 0; i < N; ++i) {
    int g = idx[i];
    if (g == NA_INTEGER) continue;
    if (g < 1 || g > K) continue;
    double x = v[i];
    if (NumericVector::is_na(x)) continue;
    const int j = g - 1;
    sumv[j] += x;
    n[j] += 1;
  }
}

// [[Rcpp::export]]
void acc_sum_n_map_centers1K(IntegerVector idx,
                             const double xminZ,
                             const double ymaxZ,
                             const double rxZ,
                             const double ryZ,
                             const int nx,
                             const int ny,
                             NumericVector valsP,
                             const double xminP,
                             const double ymaxP,
                             const double rxP,
                             const double ryP,
                             const int ncP,
                             const int nrP,
                             NumericVector sumv,
                             IntegerVector n) {
  
  const R_xlen_t N = idx.size();
  if ((R_xlen_t)nx * (R_xlen_t)ny != N) {
    stop("acc_sum_n_map_centers1K: idx length does not match nx*ny");
  }
  const int K = sumv.size();
  
  std::vector<int> cols(nx, -1);
  for (int ix = 0; ix < nx; ++ix) {
    const double x = xminZ + (ix + 0.5) * rxZ;
    const double fx = (x - xminP) / rxP;
    int col = (int)std::floor(fx);
    cols[ix] = (col >= 0 && col < ncP) ? col : -1;
  }
  
  for (int iy = 0; iy < ny; ++iy) {
    const double y = ymaxZ - (iy + 0.5) * ryZ;
    const double fy = (ymaxP - y) / ryP;
    const int row = (int)std::floor(fy);
    const bool row_ok = (row >= 0 && row < nrP);
    
    const int baseV = iy * nx;
    const int baseP = row * ncP;
    
    for (int ix = 0; ix < nx; ++ix) {
      const int t = baseV + ix;
      
      int g = idx[t];
      if (g == NA_INTEGER || g < 1 || g > K) continue;
      
      const int col = cols[ix];
      if (!row_ok || col < 0) continue;
      
      const double val = valsP[baseP + col];
      if (NumericVector::is_na(val)) continue;
      
      const int j = g - 1;
      sumv[j] += val;
      n[j] += 1;
    }
  }
}