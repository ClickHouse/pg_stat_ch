---
name: cpp-naming-check
description: Check C++ naming conventions against Google Style Guide. Use when checking if names follow conventions or when renaming identifiers.
---

# C++ Naming Convention Checker

Verify names follow Google C++ Style Guide conventions.

## Naming Rules Quick Check

| Entity | Convention | Pattern | Examples |
|--------|------------|---------|----------|
| Files | lowercase + `_` or `-` | `snake_case.cc` | `my_class.cc`, `http-parser.h` |
| Classes | PascalCase | `[A-Z][a-zA-Z0-9]*` | `MyClass`, `UrlParser` |
| Structs | PascalCase | `[A-Z][a-zA-Z0-9]*` | `Point`, `HttpRequest` |
| Type aliases | PascalCase | `[A-Z][a-zA-Z0-9]*` | `DataMap`, `StringList` |
| Enums | PascalCase | `[A-Z][a-zA-Z0-9]*` | `ErrorCode`, `State` |
| Enumerators | kPascalCase | `k[A-Z][a-zA-Z0-9]*` | `kOk`, `kNotFound` |
| Functions | PascalCase | `[A-Z][a-zA-Z0-9]*` | `ProcessData()`, `GetValue()` |
| Accessors | snake_case | `[a-z][a-z0-9_]*` | `count()`, `set_count()` |
| Variables | snake_case | `[a-z][a-z0-9_]*` | `table_name`, `num_items` |
| Class members | snake_case_ | `[a-z][a-z0-9_]*_` | `value_`, `data_map_` |
| Struct members | snake_case | `[a-z][a-z0-9_]*` | `width`, `height` |
| Constants | kPascalCase | `k[A-Z][a-zA-Z0-9]*` | `kMaxSize`, `kPi` |
| Macros | UPPER_CASE | `[A-Z][A-Z0-9_]*` | `MY_MACRO`, `DEBUG_LOG` |
| Namespaces | snake_case | `[a-z][a-z0-9_]*` | `my_project`, `internal` |
| Template params | Varies | Type=`T`, value=`n` | `T`, `InputIterator`, `N` |

## Common Mistakes

### ❌ Wrong → ✅ Correct

```cpp
// Classes
class myClass;        → class MyClass;
class HTTPParser;     → class HttpParser;  // Treat acronyms as words

// Variables
int TableName;        → int table_name;
int tablename;        → int table_name;    // Use underscores

// Class members
int value;            → int value_;        // Trailing underscore
int m_value;          → int value_;        // No Hungarian notation

// Functions
void process_data();  → void ProcessData();
void getValue();      → void GetValue();   // Or get_value() if accessor

// Constants
const int MAX_SIZE;   → const int kMaxSize;
const int maxSize;    → const int kMaxSize;

// Enumerators
enum { ERROR_OK };    → enum { kErrorOk };
enum { Ok };          → enum { kOk };

// Macros (must be UPPER_CASE with prefix)
#define DEBUG         → #define MYPROJECT_DEBUG
```

## Abbreviations

- **OK**: Listed in Wikipedia or universally known (`i`, `n`, `id`, `url`)
- **Capitalize as word**: `StartRpc()` not `StartRPC()`
- **Don't delete letters**: `customer` not `cstmr`

## Check Process

1. Identify the entity type (class, function, variable, etc.)
2. Apply the corresponding naming rule
3. Check for consistency with surrounding code
4. Flag any Hungarian notation or other outdated styles
