# Copyright 2016 <chaishushan{AT}gmail.com>. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

project(LRUCache)

add_library(lru-cache-lib
  ./cache.cc
  ./cache.h
)

add_library(cc-test-lib
  ./test.h
  ./test.cc
  ./test_main.cc
)

add_executable(lru-cache-test
  ./cache_test.cc
)

target_link_libraries(lru-cache-test
  lru-cache-lib
  cc-test-lib
)

