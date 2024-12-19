#pragma once

// @generated by torchgen/gen.py from NativeFunction.h

#include <c10/core/Scalar.h>
#include <c10/core/Storage.h>
#include <c10/core/TensorOptions.h>
#include <c10/util/Deprecated.h>
#include <c10/util/Optional.h>
#include <c10/core/QScheme.h>
#include <ATen/core/Reduction.h>
#include <ATen/core/Tensor.h>
#include <tuple>
#include <vector>
#include <ATen/ops/round_meta.h>

namespace at {
namespace native {
struct TORCH_API structured_round_out : public at::meta::structured_round {
void impl(const at::Tensor & self, const at::Tensor & out);
};
TORCH_API at::Tensor round_sparse(const at::Tensor & self);
TORCH_API at::Tensor & round_sparse_out(const at::Tensor & self, at::Tensor & out);
TORCH_API at::Tensor & round_sparse_(at::Tensor & self);
TORCH_API at::Tensor round_sparse_csr(const at::Tensor & self);
TORCH_API at::Tensor & round_sparse_csr_out(const at::Tensor & self, at::Tensor & out);
TORCH_API at::Tensor & round_sparse_csr_(at::Tensor & self);
struct TORCH_API structured_round_decimals_out : public at::meta::structured_round_decimals {
void impl(const at::Tensor & self, int64_t decimals, const at::Tensor & out);
};
} // namespace native
} // namespace at
