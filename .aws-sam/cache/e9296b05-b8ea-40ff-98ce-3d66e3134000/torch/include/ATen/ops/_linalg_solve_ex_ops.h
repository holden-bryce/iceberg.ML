#pragma once

// @generated by torchgen/gen.py from Operator.h

#include <tuple>
#include <vector>

// Forward declarations of any types needed in the operator signatures.
// We can't directly include these classes because it will cause circular include dependencies.
// This file is included by TensorBody.h, which defines the Tensor class.
#include <ATen/core/ATen_fwd.h>

namespace at {
namespace _ops {


struct TORCH_API _linalg_solve_ex {
  using schema = ::std::tuple<at::Tensor,at::Tensor,at::Tensor,at::Tensor> (const at::Tensor &, const at::Tensor &, bool, bool);
  using ptr_schema = schema*;
  // See Note [static constexpr char* members for windows NVCC]
  STATIC_CONSTEXPR_STR_INL_EXCEPT_WIN_CUDA(name, "aten::_linalg_solve_ex")
  STATIC_CONSTEXPR_STR_INL_EXCEPT_WIN_CUDA(overload_name, "")
  STATIC_CONSTEXPR_STR_INL_EXCEPT_WIN_CUDA(schema_str, "_linalg_solve_ex(Tensor A, Tensor B, *, bool left=True, bool check_errors=False) -> (Tensor result, Tensor LU, Tensor pivots, Tensor info)")
  static ::std::tuple<at::Tensor,at::Tensor,at::Tensor,at::Tensor> call(const at::Tensor & A, const at::Tensor & B, bool left, bool check_errors);
  static ::std::tuple<at::Tensor,at::Tensor,at::Tensor,at::Tensor> redispatch(c10::DispatchKeySet dispatchKeySet, const at::Tensor & A, const at::Tensor & B, bool left, bool check_errors);
};

struct TORCH_API _linalg_solve_ex_result {
  using schema = ::std::tuple<at::Tensor &,at::Tensor &,at::Tensor &,at::Tensor &> (const at::Tensor &, const at::Tensor &, bool, bool, at::Tensor &, at::Tensor &, at::Tensor &, at::Tensor &);
  using ptr_schema = schema*;
  // See Note [static constexpr char* members for windows NVCC]
  STATIC_CONSTEXPR_STR_INL_EXCEPT_WIN_CUDA(name, "aten::_linalg_solve_ex")
  STATIC_CONSTEXPR_STR_INL_EXCEPT_WIN_CUDA(overload_name, "result")
  STATIC_CONSTEXPR_STR_INL_EXCEPT_WIN_CUDA(schema_str, "_linalg_solve_ex.result(Tensor A, Tensor B, *, bool left=True, bool check_errors=False, Tensor(a!) result, Tensor(b!) LU, Tensor(c!) pivots, Tensor(d!) info) -> (Tensor(a!) result, Tensor(b!) LU, Tensor(c!) pivots, Tensor(d!) info)")
  static ::std::tuple<at::Tensor &,at::Tensor &,at::Tensor &,at::Tensor &> call(const at::Tensor & A, const at::Tensor & B, bool left, bool check_errors, at::Tensor & result, at::Tensor & LU, at::Tensor & pivots, at::Tensor & info);
  static ::std::tuple<at::Tensor &,at::Tensor &,at::Tensor &,at::Tensor &> redispatch(c10::DispatchKeySet dispatchKeySet, const at::Tensor & A, const at::Tensor & B, bool left, bool check_errors, at::Tensor & result, at::Tensor & LU, at::Tensor & pivots, at::Tensor & info);
};

}} // namespace at::_ops
