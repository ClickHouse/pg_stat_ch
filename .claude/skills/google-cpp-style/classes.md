# Classes

## Doing Work in Constructors

- Never call virtual functions in constructors
- If initialization can fail, use factory functions or `Init()` methods
- Constructors should not do complex work that could fail

```cpp
// Bad - virtual call in constructor
class Base {
 public:
  Base() { DoWork(); }  // Bad if DoWork is virtual
  virtual void DoWork();
};

// Good - factory function for fallible initialization
class Widget {
 public:
  static std::unique_ptr<Widget> Create(Config config);
 private:
  Widget() = default;
};
```

## Implicit Conversions

Mark all single-argument constructors and conversion operators `explicit`:

```cpp
class Foo {
 public:
  explicit Foo(int x);                    // Required
  explicit Foo(int x, int y = 0);         // Required (can be called with 1 arg)
  Foo(int x, int y);                      // OK - requires 2 args
  explicit operator bool() const;          // Required

  // Copy/move constructors are exceptions - don't mark explicit
  Foo(const Foo&) = default;
  Foo(Foo&&) = default;
};
```

**Exception**: `std::initializer_list` constructors may omit `explicit`.

## Copyable and Movable Types

Make copy/move semantics explicit in the public interface:

```cpp
// Copyable class
class Copyable {
 public:
  Copyable(const Copyable&) = default;
  Copyable& operator=(const Copyable&) = default;
};

// Move-only class
class MoveOnly {
 public:
  MoveOnly(MoveOnly&&) = default;
  MoveOnly& operator=(MoveOnly&&) = default;
  MoveOnly(const MoveOnly&) = delete;
  MoveOnly& operator=(const MoveOnly&) = delete;
};

// Non-copyable, non-movable
class NotCopyableOrMovable {
 public:
  NotCopyableOrMovable(const NotCopyableOrMovable&) = delete;
  NotCopyableOrMovable& operator=(const NotCopyableOrMovable&) = delete;
};
```

## Structs vs. Classes

Use `struct` for passive data carriers with public fields and no invariants:

```cpp
// Good use of struct
struct Point {
  float x;
  float y;
};

// Should be a class - has invariants
class Rectangle {
 public:
  void SetDimensions(int w, int h);
 private:
  int width_;   // Must be positive
  int height_;  // Must be positive
};
```

**Note**: Struct members use `snake_case`, class members use `snake_case_` (trailing underscore).

## Structs vs. Pairs and Tuples

Prefer structs with named fields over `std::pair` and `std::tuple`:

```cpp
// Bad - unclear what .first and .second mean
std::pair<int, std::string> GetUserInfo();

// Good - self-documenting
struct UserInfo {
  int user_id;
  std::string username;
};
UserInfo GetUserInfo();
```

## Inheritance

- Use `public` inheritance only
- Prefer composition over inheritance
- Use `final` when not designing for inheritance
- Mark base class destructors `protected` or classes `abstract` to prevent slicing

```cpp
// Good - explicitly marks override
class Derived : public Base {
 public:
  void Method() override;  // Use override, not virtual
  void Other() final;      // Cannot be overridden further
};

// Prefer composition
class Car {
 private:
  Engine engine_;  // Has-a, not is-a
};
```

Multiple inheritance is allowed but multiple *implementation* inheritance is discouraged.

## Operator Overloading

- Only overload operators when meaning is obvious
- Define operators for your own types only
- Prefer non-member binary operators

```cpp
// Good - obvious meaning
bool operator==(const Point& a, const Point& b);
bool operator<(const Point& a, const Point& b);
Point operator+(const Point& a, const Point& b);

// Bad - non-obvious meaning
void operator|(const Config& a, const Config& b);  // What does this do?
```

**Do not overload**: `&&`, `||`, `,`, unary `&`, or `operator""` (user-defined literals).

## Access Control

- Make data members `private`
- Use accessors when needed

```cpp
class MyClass {
 public:
  int value() const { return value_; }
  void set_value(int v) { value_ = v; }

 private:
  int value_;
};
```

## Declaration Order

Within each access specifier section, order as:

1. Types and type aliases
2. (structs only) Non-static data members
3. Static constants
4. Factory functions
5. Constructors and assignment operators
6. Destructor
7. All other functions
8. All other data members

```cpp
class MyClass {
 public:
  using ValueType = int;
  static constexpr int kMax = 100;

  static std::unique_ptr<MyClass> Create();

  MyClass();
  ~MyClass();

  void DoSomething();
  int value() const;

 private:
  void Helper();
  int value_;
};
```
