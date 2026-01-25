# Naming Conventions

## General Principles

- Names should be descriptive and proportional to scope
- Minimize abbreviations (OK if in Wikipedia or universally known)
- Capitalize abbreviations as single words: `StartRpc()` not `StartRPC()`

## Quick Reference

| Entity | Style | Example |
|--------|-------|---------|
| Files | `snake_case` | `my_useful_class.cc` |
| Types (class, struct, enum, alias) | `PascalCase` | `MyExcitingClass` |
| Variables | `snake_case` | `table_name` |
| Class data members | `snake_case_` | `table_name_` |
| Struct data members | `snake_case` | `num_entries` |
| Constants | `kPascalCase` | `kDaysInAWeek` |
| Functions | `PascalCase` | `AddTableEntry()` |
| Accessors/mutators | `snake_case` | `count()`, `set_count()` |
| Namespaces | `snake_case` | `my_project` |
| Enumerators | `kPascalCase` | `kErrorOutOfMemory` |
| Macros | `UPPER_CASE` | `MY_MACRO_NAME` |

## File Names

- All lowercase with underscores or dashes
- C++ source: `.cc`, headers: `.h`
- Be specific: `http_server_logs.h` not `logs.h`

```
my_useful_class.cc
my_useful_class.h
my_useful_class_test.cc
```

## Type Names

PascalCase for all types:

```cpp
class UrlTable { ... };
struct UrlTableProperties { ... };
typedef hash_map<...> PropertiesMap;
using PropertiesMap = hash_map<...>;
enum class UrlTableError { ... };
template <typename T> class MyTemplate { ... };
```

## Variable Names

### Local and parameter variables

```cpp
std::string table_name;  // Good
std::string tableName;   // Bad - mixed case
```

### Class data members

Trailing underscore:

```cpp
class TableInfo {
 private:
  std::string table_name_;
  static Pool* pool_;
};
```

### Struct data members

No trailing underscore:

```cpp
struct UrlTableProperties {
  std::string name;
  int num_entries;
};
```

## Constant Names

`k` prefix + PascalCase for static/global constants:

```cpp
const int kDaysInAWeek = 7;
const int kAndroid8_0_0 = 24;  // Underscores OK for versions
constexpr float kPi = 3.14159f;

void Compute() {
  // Local constants - either style OK
  const int kLocalMax = 100;
  const int local_max = 100;
}
```

## Function Names

PascalCase:

```cpp
AddTableEntry();
DeleteUrl();
OpenFileOrDie();
```

Accessors and mutators use `snake_case`:

```cpp
int count() const { return count_; }
void set_count(int count) { count_ = count; }
```

## Namespace Names

All lowercase with underscores:

```cpp
namespace my_project {
namespace internal {
// ...
}  // namespace internal
}  // namespace my_project

// Single-line nested OK
namespace my_project::internal {
// ...
}
```

## Enumerator Names

`k` prefix + PascalCase:

```cpp
enum class UrlTableError {
  kOk = 0,
  kOutOfMemory,
  kMalformedInput,
};

// Old MACRO_STYLE is deprecated
enum class AlternateError {
  OK = 0,           // Deprecated style
  OUT_OF_MEMORY,    // Deprecated style
};
```

## Template Parameter Names

Follow type or variable rules:

```cpp
template <typename T>           // Type parameter
template <typename InputIterator>
template <int N>                // Non-type uses variable rules
template <int buffer_size>
```

## Macro Names

All caps with underscores, project-specific prefix:

```cpp
#define MYPROJECT_ROUND(x) ...
#define MY_MACRO_THAT_DOES_SOMETHING ...
```

## Choosing Good Names

```cpp
// Good - clear in context
class MyClass {
 public:
  int CountFooErrors(const std::vector<Foo>& foos) {
    int n = 0;  // Clear given limited scope
    for (const auto& foo : foos) {
      // ...
      ++n;
    }
    return n;
  }

 private:
  const int kMaxAllowedConnections = ...;  // Clear in class context
};

// Bad - overly verbose or unclear
int total_number_of_foo_errors = 0;  // Too verbose for local scope
int cstmr_id = ...;                   // Don't delete letters
const int kNum = ...;                 // Too vague for wide scope
```

## Exceptions to Naming Rules

When mimicking existing C/C++ entities:

```cpp
bigopen()           // Matches open()
uint                // typedef, matches C style
sparse_hash_map     // STL-like, follows STL conventions
LONGLONG_MAX        // Matches INT_MAX
```
