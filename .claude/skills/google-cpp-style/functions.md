# Functions

## Inputs and Outputs

**Prefer return values over output parameters.**

| Parameter Type | Use For |
|---------------|---------|
| Value or `const T&` | Non-optional inputs |
| `std::optional<T>` | Optional by-value inputs |
| `const T*` | Optional inputs (when reference would be used) |
| Reference `T&` | Non-optional outputs/in-out |
| Pointer `T*` | Optional outputs/in-out |

```cpp
// Good - return value
std::string ProcessData(const Input& input);

// Good - reference for non-optional output
void ProcessData(const Input& input, Output& output);

// Good - pointer for optional output
void ProcessData(const Input& input, Output* output);  // output may be nullptr
```

**Parameter order**: inputs before outputs.

Avoid functions requiring reference parameters to outlive the call (dangling reference risk).

## Write Short Functions

- Prefer functions ≤40 lines
- Break up long functions when it improves readability
- Small functions are easier to test and understand

## Function Overloading

Use overloading when behavior is obvious without knowing which overload is called:

```cpp
// Good - obvious what happens
class MyClass {
 public:
  void Analyze(const std::string& text);
  void Analyze(const char* text, size_t len);
};

// Consider string_view instead of multiple overloads
void Analyze(std::string_view text);
```

For overload sets, provide a single umbrella comment before the first declaration.

## Default Arguments

Allowed on non-virtual functions when:
- Default always has the same value
- Readability benefit outweighs downsides

```cpp
// Good
void SetTimeout(int timeout_ms = 1000);

// Bad - default on virtual function
class Base {
  virtual void Process(int mode = 0);  // Don't do this
};

// Bad - default varies
void Log(int level = GetDefaultLevel());  // Re-evaluated each call
```

When in doubt, use overloads instead of default arguments.

## Trailing Return Type Syntax

Use only when required or when it significantly improves readability:

```cpp
// Required for lambdas with explicit return type
auto lambda = [](int x) -> double { return x * 1.5; };

// Helpful for complex template returns
template <typename T, typename U>
auto Add(T t, U u) -> decltype(t + u);

// Not needed for simple cases - prefer traditional style
int Foo(int x);  // Good
auto Foo(int x) -> int;  // Unnecessary
```

## Rvalue References

Use only for:

1. **Move constructors and assignment**:
```cpp
class MyClass {
 public:
  MyClass(MyClass&& other) = default;
  MyClass& operator=(MyClass&& other) = default;
};
```

2. **`&&`-qualified methods** that consume `*this`:
```cpp
class Builder {
 public:
  Result Build() &&;  // Consumes the builder
};
```

3. **Perfect forwarding**:
```cpp
template <typename... Args>
void Forward(Args&&... args) {
  target(std::forward<Args>(args)...);
}
```

4. **Performance-critical overload pairs** (when justified):
```cpp
void Process(const std::string& s);
void Process(std::string&& s);  // Only if measurably faster
```

## Friends

- Define friends in the same file when possible
- Use for factory classes, test classes, or tightly coupled helpers

```cpp
class Foo {
 public:
  // ...
 private:
  friend class FooBuilder;  // Can access Foo's private members
  int internal_state_;
};
```
