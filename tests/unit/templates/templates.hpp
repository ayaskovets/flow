#pragma once

#define __TEST_CONCAT_IMPL(a, b) a##_##b
#define __TEST_CONCAT(a, b) __TEST_CONCAT_IMPL(a, b)

#define TEST_T(test_suite_name, test_name, type, ...)         \
  TEST(test_suite_name, __TEST_CONCAT(test_name, __LINE__)) { \
    templates::test_name<type>(__VA_ARGS__);                  \
  }

#define TEST_P_T(test_suite_name, test_name, type, ...)                \
  TEST_P(test_suite_name, __TEST_CONCAT(test_name, __LINE__)) {        \
    templates::test_name<type>(GetParam() __VA_OPT__(, ) __VA_ARGS__); \
  }
