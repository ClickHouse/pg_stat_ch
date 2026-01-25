---
name: google-cpp-style
description: Google C++ Style Guide rules for writing clean, maintainable C++ code. Use when writing C++, reviewing code, discussing naming conventions, formatting, class design, or any C++ best practices. Covers headers, scoping, classes, functions, naming, comments, and formatting.
---

# Google C++ Style Guide

This skill provides the complete Google C++ Style Guide for C++20. Apply these rules when writing or reviewing C++ code.

## Quick Reference

| Topic | Key Rules |
|-------|-----------|
| **Naming** | Types: `PascalCase`, functions: `PascalCase`, variables: `snake_case`, constants: `kPascalCase`, macros: `UPPER_CASE` |
| **Formatting** | 80 char lines, 2-space indent, spaces (not tabs), `{` on same line |
| **Headers** | Self-contained, `#define` guards, include what you use |
| **Classes** | Prefer composition over inheritance, explicit constructors, `private` data members |

## Core Principles

1. **Optimize for the reader** - Code is read more than written
2. **Be consistent** - Follow existing patterns in the codebase
3. **Avoid surprising constructs** - Prefer clear over clever

## Target Version

Code should target **C++20**. Do not use C++23 features or non-standard extensions.

## Detailed References

For complete rules on specific topics:

- [Header Files](headers.md) - Include guards, ordering, forward declarations
- [Scoping](scoping.md) - Namespaces, linkage, static/global variables
- [Classes](classes.md) - Constructors, inheritance, operator overloading
- [Functions](functions.md) - Parameters, overloading, default arguments
- [C++ Features](features.md) - Smart pointers, exceptions, RTTI, lambdas
- [Naming](naming.md) - All naming conventions with examples
- [Comments](comments.md) - Documentation style and requirements
- [Formatting](formatting.md) - Whitespace, braces, line length

## Common Patterns

### Header File Template

```cpp
#ifndef PROJECT_PATH_FILE_H_
#define PROJECT_PATH_FILE_H_

#include "project/public/header.h"

#include <sys/types.h>

#include <string>
#include <vector>

#include "other/library.h"
#include "project/internal.h"

namespace project {

class MyClass {
 public:
  MyClass();
  explicit MyClass(int value);

  void DoSomething();
  int count() const { return count_; }
  void set_count(int count) { count_ = count; }

 private:
  int count_;
};

}  // namespace project

#endif  // PROJECT_PATH_FILE_H_
```

### Class Declaration Order

```cpp
class MyClass {
 public:
  // Types and type aliases
  using ValueType = int;

  // Static constants
  static constexpr int kMaxSize = 100;

  // Constructors and assignment
  MyClass();
  MyClass(const MyClass&) = default;
  MyClass& operator=(const MyClass&) = default;

  // Destructor
  ~MyClass();

  // All other methods
  void Process();

 protected:
  // Protected members (if needed)

 private:
  // Private methods
  void InternalHelper();

  // Data members
  int value_;
};
```

### Function Comments

```cpp
// Returns an iterator positioned at the first entry >= `start_word`.
// Returns nullptr if no such entry exists.
// The client must not use the iterator after the table is destroyed.
std::unique_ptr<Iterator> GetIterator(absl::string_view start_word) const;
```

## Critical Rules Summary

### Always Do

- Use `#define` guards in all headers
- Make single-argument constructors `explicit`
- Use `nullptr` (not `NULL` or `0`) for pointers
- Use `override` or `final` for virtual function overrides
- Declare data members `private`
- Initialize variables at declaration

### Never Do

- Use `using namespace` directives
- Use C-style casts (use `static_cast`, etc.)
- Use exceptions (in Google code)
- Use non-standard extensions
- Define macros in headers (when possible)
- Use `std::auto_ptr` (use `std::unique_ptr`)
