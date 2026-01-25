---
name: cpp-header-template
description: Generate C++ header files following Google Style Guide. Use when creating new header files or asking for a header template.
disable-model-invocation: true
---

# C++ Header File Template Generator

Generate header files following Google C++ Style Guide conventions.

## Usage

Provide: class name, namespace, and file path to generate a compliant header.

## Template

```cpp
// Copyright [year] [copyright holder]
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#ifndef [PROJECT]_[PATH]_[FILENAME]_H_
#define [PROJECT]_[PATH]_[FILENAME]_H_

// C system headers (if needed)
// #include <sys/types.h>

// C++ standard library headers
#include <memory>
#include <string>
#include <vector>

// Other library headers
// #include "other/library.h"

// Project headers
// #include "project/base.h"

namespace [namespace] {

// Forward declarations (avoid when possible)

/// Brief description of the class.
///
/// Detailed description if needed. Include:
/// - Purpose and responsibilities
/// - Thread safety guarantees
/// - Example usage (for complex classes)
class [ClassName] {
 public:
  // Types and type aliases
  using ValueType = int;

  // Static constants
  static constexpr int kDefaultSize = 100;

  // Factory functions (if needed)
  static std::unique_ptr<[ClassName]> Create();

  // Constructors
  [ClassName]();
  explicit [ClassName](int value);

  // Rule of five - be explicit about copy/move
  [ClassName](const [ClassName]&) = default;
  [ClassName]& operator=([const ClassName]&) = default;
  [ClassName]([ClassName]&&) noexcept = default;
  [ClassName]& operator=([ClassName]&&) noexcept = default;

  // Destructor
  ~[ClassName]();

  // Public methods
  void Process();

  // Accessors and mutators
  int value() const { return value_; }
  void set_value(int value) { value_ = value; }

 protected:
  // Protected members (only if needed for inheritance)

 private:
  // Private methods
  void InternalHelper();

  // Data members (always private for classes)
  int value_ = 0;
  std::string name_;
};

}  // namespace [namespace]

#endif  // [PROJECT]_[PATH]_[FILENAME]_H_
```

## Include Guard Format

Convert path to uppercase with underscores:

| File Path | Guard |
|-----------|-------|
| `foo/bar/baz.h` | `FOO_BAR_BAZ_H_` |
| `src/http/parser.h` | `SRC_HTTP_PARSER_H_` |
| `my_project/utils.h` | `MY_PROJECT_UTILS_H_` |

## Struct Template

For passive data with public members and no invariants:

```cpp
namespace [namespace] {

/// Brief description.
struct [StructName] {
  int field_one;           // No trailing underscore for structs
  std::string field_two;
  float field_three = 0.0f;  // Default values OK
};

}  // namespace [namespace]
```

## Include Order

1. Related header (for `.cc` files)
2. Blank line
3. C system headers (`<unistd.h>`)
4. Blank line
5. C++ standard library (`<string>`, `<vector>`)
6. Blank line
7. Other libraries
8. Blank line
9. Project headers

Each section sorted alphabetically.
