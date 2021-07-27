/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cudf/detail/utilities/assert.cuh>
#include <cudf/types.hpp>

// Note: The <cuda/std/*> versions are used in order for Jitify to work with our fixed_point type.
//       Jitify is needed for several algorithms (binaryop, rolling, etc)
#include <cuda/std/limits>
#include <cuda/std/type_traits>  // add cuda namespace

#include <algorithm>
#include <cassert>
#include <cmath>
#include <string>

//! `fixed_point` and supporting types
namespace numeric {
namespace detail {
namespace numeric_limits {

template <typename T>
auto max() -> T
{
  if constexpr (std::is_same_v<T, __int128_t>) {
    // −170,141,183,460,469,231,731,687,303,715,884,105,728
    __int128_t max = 1;
    for (int i = 0; i < 126; ++i) {
      max *= 2;
    }
    return max + (max - 1);
  }

  return std::numeric_limits<T>::max();
}

template <typename T>
auto lowest() -> T
{
  if constexpr (std::is_same_v<T, __int128_t>) {
    // 170,141,183,460,469,231,731,687,303,715,884,105,728
    __int128_t lowest = -1;
    for (int i = 0; i < 127; ++i) {
      lowest *= 2;
    }
    return lowest;
  }

  return std::numeric_limits<T>::lowest();
}

}  // namespace numeric_limits

template <typename T>
auto to_string(T value) -> std::string
{
  if constexpr (cuda::std::is_same<T, __int128_t>::value) {
    auto s          = std::string{};
    auto const sign = value < 0;
    if (sign) {
      value += 1;  // avoid overflowing if value == _int128_t lowest
      value *= -1;
      if (value == detail::numeric_limits::max<__int128_t>())
        return "-170141183460469231731687303715884105728";
      value += 1;  // can add back the one, not need to avoid overflow anymore
    }
    while (value) {
      s.push_back("0123456789"[value % 10]);
      value /= 10;
    }
    if (sign) s.push_back('-');
    std::reverse(s.begin(), s.end());
    return s;
  } else {
    return std::to_string(value);
  }
}

template <typename T>
CUDA_HOST_DEVICE_CALLABLE constexpr auto abs(T value)
{
  return value >= 0 ? value : -value;
}

template <typename T>
CUDA_HOST_DEVICE_CALLABLE constexpr auto is_signed()
{
  return std::is_signed<T>::value || std::is_same_v<T, __int128_t>;
}

}  // namespace detail

/** @} */  // end of group
}  // namespace numeric
