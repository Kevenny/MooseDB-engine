# TideFlow storage engine — build integrated with the MariaDB source tree.
#
# Copyright (c) 2026 Kevenny / Rech Informática
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# The Rust core is built by cargo as a static library (libtideflow.a) and
# linked into the ha_tideflow.so plugin.

FIND_PROGRAM(CARGO_EXECUTABLE cargo)
IF(NOT CARGO_EXECUTABLE)
  MESSAGE(STATUS "TideFlow: cargo not found, storage engine will not be built")
  RETURN()
ENDIF()

SET(TIDEFLOW_RUST_DIR ${CMAKE_CURRENT_SOURCE_DIR}/rust)
SET(TIDEFLOW_CARGO_TARGET_DIR ${CMAKE_CURRENT_BINARY_DIR}/cargo-target)
SET(TIDEFLOW_RUST_LIB ${TIDEFLOW_CARGO_TARGET_DIR}/release/libtideflow.a)

FILE(GLOB_RECURSE TIDEFLOW_RUST_SOURCES CONFIGURE_DEPENDS
  ${TIDEFLOW_RUST_DIR}/tideflow-core/*.rs
  ${TIDEFLOW_RUST_DIR}/tideflow-ffi/src/*.rs
  ${TIDEFLOW_RUST_DIR}/tideflow-core/src/*/*.rs
  ${TIDEFLOW_RUST_DIR}/*/Cargo.toml)

ADD_CUSTOM_COMMAND(
  OUTPUT ${TIDEFLOW_RUST_LIB}
  COMMAND ${CMAKE_COMMAND} -E env CARGO_TARGET_DIR=${TIDEFLOW_CARGO_TARGET_DIR}
          ${CARGO_EXECUTABLE} build --release --locked -p tideflow-ffi
          --manifest-path ${TIDEFLOW_RUST_DIR}/Cargo.toml
  DEPENDS ${TIDEFLOW_RUST_SOURCES}
          ${TIDEFLOW_RUST_DIR}/Cargo.toml ${TIDEFLOW_RUST_DIR}/Cargo.lock
  COMMENT "Building TideFlow Rust core (cargo)"
  VERBATIM)
ADD_CUSTOM_TARGET(tideflow_rust DEPENDS ${TIDEFLOW_RUST_LIB})

# tideflow_ffi.h is committed so the C++ side builds without cbindgen.
# When cbindgen is available, fail the build if the header is stale.
FIND_PROGRAM(CBINDGEN_EXECUTABLE cbindgen)
IF(CBINDGEN_EXECUTABLE)
  ADD_CUSTOM_TARGET(tideflow_ffi_header_check
    COMMAND ${CBINDGEN_EXECUTABLE} --verify
            --config ${TIDEFLOW_RUST_DIR}/cbindgen.toml
            --output ${CMAKE_CURRENT_SOURCE_DIR}/tideflow_ffi.h
            ${TIDEFLOW_RUST_DIR}/tideflow-ffi
    COMMENT "Checking that tideflow_ffi.h matches the Rust FFI"
    VERBATIM)
ENDIF()

MYSQL_ADD_PLUGIN(tideflow ha_tideflow.cc
  STORAGE_ENGINE MODULE_ONLY
  LINK_LIBRARIES ${TIDEFLOW_RUST_LIB} pthread dl m)

IF(TARGET tideflow)
  ADD_DEPENDENCIES(tideflow tideflow_rust)
  IF(TARGET tideflow_ffi_header_check)
    ADD_DEPENDENCIES(tideflow tideflow_ffi_header_check)
  ENDIF()
  SET_TARGET_PROPERTIES(tideflow PROPERTIES
    CXX_STANDARD 20 CXX_STANDARD_REQUIRED ON CXX_EXTENSIONS OFF)
  # Keep the symbols of the static Rust library (std, zstd, ...) private to
  # the plugin, so they never interpose with other libraries in mysqld.
  # Only that library: the server patches the plugin-service pointers from
  # libmysqlservices.a (my_print_error_service, ...) through dlsym().
  IF(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    TARGET_LINK_OPTIONS(tideflow PRIVATE "LINKER:--exclude-libs,libtideflow.a")
  ENDIF()
ENDIF()
