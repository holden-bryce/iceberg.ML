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
#include <ATen/ops/special_xlog1py_meta.h>

namespace at {
namespace native {
struct TORCH_API structured_special_xlog1py_out : public at::meta::structured_special_xlog1py {
void impl(const at::Tensor & self, const at::Tensor & other, const at::Tensor & out);
};
TORCH_API at::Tensor special_xlog1py(const at::Scalar & self, const at::Tensor & other);
TORCH_API at::Tensor & special_xlog1py_out(const at::Scalar & self, const at::Tensor & other, at::Tensor & out);
TORCH_API at::Tensor special_xlog1py(const at::Tensor & self, const at::Scalar & other);
TORCH_API at::Tensor & special_xlog1py_out(const at::Tensor & self, const at::Scalar & other, at::Tensor & out);
} // namespace native
} // namespace at
