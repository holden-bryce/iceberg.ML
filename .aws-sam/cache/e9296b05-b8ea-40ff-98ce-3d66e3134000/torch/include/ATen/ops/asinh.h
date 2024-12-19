#pragma once

// @generated by torchgen/gen.py from Function.h

#include <ATen/Context.h>
#include <ATen/DeviceGuard.h>
#include <ATen/TensorUtils.h>
#include <ATen/TracerMode.h>
#include <ATen/core/Generator.h>
#include <ATen/core/Reduction.h>
#include <ATen/core/Tensor.h>
#include <c10/core/Scalar.h>
#include <c10/core/Storage.h>
#include <c10/core/TensorOptions.h>
#include <c10/util/Deprecated.h>
#include <c10/util/Optional.h>



#include <ATen/ops/asinh_ops.h>

namespace at {


// aten::asinh(Tensor self) -> Tensor
inline at::Tensor asinh(const at::Tensor & self) {
    return at::_ops::asinh::call(self);
}

// aten::asinh_(Tensor(a!) self) -> Tensor(a!)
inline at::Tensor & asinh_(at::Tensor & self) {
    return at::_ops::asinh_::call(self);
}

// aten::asinh.out(Tensor self, *, Tensor(a!) out) -> Tensor(a!)
inline at::Tensor & asinh_out(at::Tensor & out, const at::Tensor & self) {
    return at::_ops::asinh_out::call(self, out);
}
// aten::asinh.out(Tensor self, *, Tensor(a!) out) -> Tensor(a!)
inline at::Tensor & asinh_outf(const at::Tensor & self, at::Tensor & out) {
    return at::_ops::asinh_out::call(self, out);
}

}
