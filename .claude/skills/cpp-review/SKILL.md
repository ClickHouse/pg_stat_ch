---
name: cpp-review
description: Review C++ code against Google C++ Style Guide. Use when reviewing C++ code, pull requests, or when asked to check code style compliance.
---

# C++ Code Review (Google Style)

Review C++ code for compliance with the Google C++ Style Guide.

## Review Checklist

### Naming
- [ ] Types use `PascalCase`
- [ ] Functions use `PascalCase` (accessors use `snake_case`)
- [ ] Variables use `snake_case`
- [ ] Class members have trailing underscore: `member_`
- [ ] Constants use `kPascalCase`
- [ ] Macros use `UPPER_CASE` with project prefix

### Headers
- [ ] Has `#define` guard: `PROJECT_PATH_FILE_H_`
- [ ] Self-contained (includes all dependencies)
- [ ] Includes ordered: related header, C system, C++ stdlib, other libs, project
- [ ] No forward declarations unless necessary

### Classes
- [ ] Single-argument constructors are `explicit`
- [ ] Data members are `private`
- [ ] Copy/move semantics explicit (= default, = delete)
- [ ] No virtual calls in constructors
- [ ] Uses composition over inheritance when appropriate

### Functions
- [ ] Returns values instead of output parameters when possible
- [ ] Parameters ordered: inputs before outputs
- [ ] Functions are ≤40 lines (prefer smaller)
- [ ] Uses `override`/`final` for virtual overrides

### Modern C++
- [ ] Uses `nullptr` (not `NULL` or `0`)
- [ ] Uses C++ casts (not C-style)
- [ ] Uses range-based for loops where appropriate
- [ ] Uses `auto` appropriately (not excessively)
- [ ] Smart pointers for ownership (`unique_ptr`, `shared_ptr`)

### Formatting
- [ ] 80 character line limit
- [ ] 2-space indent
- [ ] Braces on same line as control structures
- [ ] Spaces around binary operators

## Feedback Format

Use severity levels:

- 🔴 **MUST FIX**: Style violations or bugs that must be fixed
- 🟡 **SHOULD FIX**: Strong recommendations for improvement
- 🟢 **CONSIDER**: Optional enhancements or suggestions

## Example Review Comment

```
🔴 **MUST FIX**: Missing `explicit` on single-argument constructor
Line 45: `Foo(int value)` should be `explicit Foo(int value)` to prevent
implicit conversions.

🟡 **SHOULD FIX**: Function too long
Lines 78-145: `ProcessData()` is 67 lines. Consider breaking into smaller
functions for readability and testability.

🟢 **CONSIDER**: Use structured bindings
Line 23: `auto [iter, success] = map.insert({key, value});` would be clearer
than separate `.first` and `.second` access.
```
