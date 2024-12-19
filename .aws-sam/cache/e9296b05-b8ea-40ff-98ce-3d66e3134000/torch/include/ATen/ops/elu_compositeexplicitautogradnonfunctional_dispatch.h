#pragma once
// @generated by torchgen/gen.py from DispatchKeyFunction.h

// NB: The implementing C++ file is RegisterDispatchKey.cpp

// The only #includes we need are for custom classes that have defaults in the C++ API
#include <c10/core/MemoryFormat.h>
#include <c10/core/Scalar.h>
#include <ATen/core/Reduction.h>

// Forward declarations of any types needed in the operator signatures.
// We can't directly include these classes because it will cause circular include dependencies.
// This file is included by TensorBody.h, which defines the Tensor class.
#include <ATen/core/ATen_fwd.h>

namespace at {

namespace compositeexplicitautogradnonfunctional {

TORCH_API at::Tensor elu(const at::Tensor & self, const at::Scalar & alpha=1, const at::Scalar & scale=1, const at::Scalar & input_scale=1);
TORCH_API at::Tensor & elu_(at::Tensor & self, const at::Scalar & alpha=1, const at::Scalar & scale=1, const at::Scalar & input_scale=1);

} // namespace compositeexplicitautogradnonfunctional
} // namespace at
