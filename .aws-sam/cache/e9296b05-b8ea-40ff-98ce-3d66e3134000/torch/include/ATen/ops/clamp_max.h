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



#include <ATen/ops/clamp_max_ops.h>

namespace at {


// aten::clamp_max(Tensor self, Scalar max) -> Tensor
inline at::Tensor clamp_max(const at::Tensor & self, const at::Scalar & max) {
    return at::_ops::clamp_max::call(self, max);
}

// aten::clamp_max.Tensor(Tensor self, Tensor max) -> Tensor
inline at::Tensor clamp_max(const at::Tensor & self, const at::Tensor & max) {
    return at::_ops::clamp_max_Tensor::call(self, max);
}

// aten::clamp_max_(Tensor(a!) self, Scalar max) -> Tensor(a!)
inline at::Tensor & clamp_max_(at::Tensor & self, const at::Scalar & max) {
    return at::_ops::clamp_max_::call(self, max);
}

// aten::clamp_max_.Tensor(Tensor(a!) self, Tensor max) -> Tensor(a!)
inline at::Tensor & clamp_max_(at::Tensor & self, const at::Tensor & max) {
    return at::_ops::clamp_max__Tensor::call(self, max);
}

// aten::clamp_max.out(Tensor self, Scalar max, *, Tensor(a!) out) -> Tensor(a!)
inline at::Tensor & clamp_max_out(at::Tensor & out, const at::Tensor & self, const at::Scalar & max) {
    return at::_ops::clamp_max_out::call(self, max, out);
}
// aten::clamp_max.out(Tensor self, Scalar max, *, Tensor(a!) out) -> Tensor(a!)
inline at::Tensor & clamp_max_outf(const at::Tensor & self, const at::Scalar & max, at::Tensor & out) {
    return at::_ops::clamp_max_out::call(self, max, out);
}

// aten::clamp_max.Tensor_out(Tensor self, Tensor max, *, Tensor(a!) out) -> Tensor(a!)
inline at::Tensor & clamp_max_out(at::Tensor & out, const at::Tensor & self, const at::Tensor & max) {
    return at::_ops::clamp_max_Tensor_out::call(self, max, out);
}
// aten::clamp_max.Tensor_out(Tensor self, Tensor max, *, Tensor(a!) out) -> Tensor(a!)
inline at::Tensor & clamp_max_outf(const at::Tensor & self, const at::Tensor & max, at::Tensor & out) {
    return at::_ops::clamp_max_Tensor_out::call(self, max, out);
}

}
