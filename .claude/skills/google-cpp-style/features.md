# Other C++ Features

## Smart Pointers and Ownership

- Prefer single, fixed ownership
- Use `std::unique_ptr` for exclusive ownership
- Use `std::shared_ptr` only when truly needed (immutable shared data)
- Never use `std::auto_ptr`

```cpp
// Good - clear ownership transfer
std::unique_ptr<Foo> CreateFoo();
void ConsumeFoo(std::unique_ptr<Foo> foo);

// Shared ownership - use sparingly
std::shared_ptr<const Config> GetConfig();
```

## Exceptions

**Do not use C++ exceptions** (in Google code).

Also avoid: `std::exception_ptr`, `std::nested_exception`.

Use error codes, `absl::Status`, or assertions instead.

## noexcept

Use `noexcept` when:
- It accurately reflects the function won't throw
- There's meaningful performance benefit (especially move constructors)

```cpp
// Good - move operations benefit from noexcept
MyClass(MyClass&&) noexcept;
MyClass& operator=(MyClass&&) noexcept;

// Unconditional noexcept when exceptions are disabled
void Process() noexcept;
```

## RTTI (Run-Time Type Information)

Avoid `typeid` and `dynamic_cast`. Prefer:
- Virtual methods
- Visitor pattern
- Redesigning the class hierarchy

```cpp
// Bad - type-based decision tree
if (typeid(*obj) == typeid(Derived1)) { ... }
else if (typeid(*obj) == typeid(Derived2)) { ... }

// Good - virtual method
obj->Process();  // Each derived class implements appropriately
```

`dynamic_cast` is OK when logic guarantees the type, but prefer `static_cast` in those cases.

## Casting

Use C++ casts, never C-style casts:

| Cast | Use For |
|------|---------|
| `static_cast<T>` | Value conversions, explicit upcasts, known downcasts |
| `const_cast<T>` | Removing `const` (use sparingly) |
| `reinterpret_cast<T>` | Pointer/integer conversions (dangerous) |
| `std::bit_cast<T>` | Type punning (reinterpret bits) |
| Brace initialization | Safe arithmetic conversion: `int64_t{x}` |

```cpp
// Good
double d = static_cast<double>(i);
auto* derived = static_cast<Derived*>(base_ptr);

// Bad
double d = (double)i;  // C-style cast
```

## Streams

Use streams for:
- Debug logging
- Ad-hoc, developer-facing output
- Simple I/O

Avoid streams for:
- User-facing output (use templating libraries)
- Performance-critical code
- Untrusted input

```cpp
// OK for debug output
std::cerr << "Debug: value = " << value << "\n";

// Prefer explicit formatting
std::string result = absl::StrFormat("Value: %d", value);
```

Do not use stateful stream features (`imbue()`, `xalloc()`, manipulators for formatting).

## Preincrement and Predecrement

Prefer prefix (`++i`) over postfix (`i++`) when the value isn't used:

```cpp
for (int i = 0; i < n; ++i) {  // Good
  // ...
}
```

## const

Use `const` wherever meaningful:
- Function parameters passed by reference/pointer
- Methods that don't modify state
- Local variables that don't change

```cpp
void Process(const std::string& input);  // Won't modify input
int GetValue() const;                     // Doesn't modify object
const int kBufferSize = 1024;             // Constant
```

Put `const` first: `const int*` preferred over `int const*`.

## constexpr, constinit, consteval

- `constexpr`: True compile-time constants and functions
- `constinit`: Ensure constant initialization for non-const variables
- `consteval`: Functions that must run at compile time

```cpp
constexpr int kMaxSize = 100;
constexpr int Square(int x) { return x * x; }
constinit thread_local int counter = 0;
```

## Integer Types

- Use `int` for general integers
- Use `<stdint.h>` types (`int32_t`, `int64_t`) when size matters
- Avoid unsigned types except for bit patterns

```cpp
int count;                    // General use
int64_t large_value;          // When > 2^31 possible
uint32_t flags;               // Bit patterns OK
size_t index;                 // Container sizes OK
```

## Lambda Expressions

```cpp
// Capture by reference when lambda's lifetime < captures
std::sort(v.begin(), v.end(), [&](int a, int b) { return a < b; });

// Explicit captures for escaping lambdas
executor->Schedule([&foo] { Process(foo); });  // Make captures explicit

// Init captures for move-only types
[ptr = std::move(unique_ptr)] { ... }
```

**Rules:**
- Prefer explicit captures for lambdas that escape current scope
- Use default capture `[&]` only for short-lived lambdas
- Avoid default `[=]` capturing `this` implicitly

## Template Metaprogramming

Use sparingly. Prefer:
- Clear, simple implementations
- Well-commented complex templates
- Hiding metaprogramming as implementation details

## Concepts and Constraints (C++20)

- Use standard library concepts over type traits
- Avoid defining new public concepts
- Prefer `requires(Concept<T>)` syntax

```cpp
// Good - use standard concepts
template <typename T>
  requires std::integral<T>
T Add(T a, T b);

// Avoid - custom concept at API boundary
template <MyConcept T>  // Don't expose custom concepts
void Process(T value);
```

## Boost

Only use approved Boost libraries. See style guide for current list.

## Disallowed Features

- `<ratio>` header
- `<cfenv>` / `<fenv.h>` headers  
- `<filesystem>` header
- C++20 modules
- Coroutines (without approved library)
- Non-standard extensions (`__attribute__`, inline assembly, etc.)
