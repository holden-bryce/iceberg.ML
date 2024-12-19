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


namespace at {
namespace native {
TORCH_API at::Tensor sparse_resize(const at::Tensor & self, at::IntArrayRef size, int64_t sparse_dim, int64_t dense_dim);
TORCH_API const at::Tensor & sparse_resize_out(const at::Tensor & self, at::IntArrayRef size, int64_t sparse_dim, int64_t dense_dim, const at::Tensor & out);
TORCH_API const at::Tensor & sparse_resize_(const at::Tensor & self, at::IntArrayRef size, int64_t sparse_dim, int64_t dense_dim);
} // namespace native
} // namespace at
