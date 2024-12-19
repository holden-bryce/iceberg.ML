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
TORCH_API ::std::tuple<at::Tensor,at::Tensor> _efficient_attention_forward(const at::Tensor & query, const at::Tensor & key, const at::Tensor & value, const c10::optional<at::Tensor> & cu_seqlens_q, const c10::optional<at::Tensor> & cu_seqlens_k, c10::optional<int64_t> max_seqlen_q, bool compute_log_sumexp=false, bool causal=false);
} // namespace native
} // namespace at
