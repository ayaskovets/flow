#pragma once

#define TEST_T(test_suite_name, test_name, type, ...) \
  TEST(test_suite_name, test_name) { templates::test_name<type>(__VA_ARGS__); }

#define TEST_P_T(test_suite_name, test_name, type, ...)                \
  TEST_P(test_suite_name, test_name) {                                 \
    templates::test_name<type>(GetParam() __VA_OPT__(, ) __VA_ARGS__); \
  }
