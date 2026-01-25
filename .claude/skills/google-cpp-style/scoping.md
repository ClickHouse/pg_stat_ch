# Scoping

## Namespaces

Place code in a namespace with a unique name based on project name/path.

```cpp
// In .h file
namespace mynamespace {

class MyClass {
 public:
  void Foo();
};

}  // namespace mynamespace

// In .cc file
namespace mynamespace {

void MyClass::Foo() {
  // ...
}

}  // namespace mynamespace
```

**Rules:**
- Do not indent namespace contents
- Terminate with `// namespace name` comment
- Single-line nested namespaces allowed: `namespace my_project::my_component {`
- Do not use `using namespace foo;` (pollutes namespace)
- Do not use inline namespaces
- Do not declare anything in `namespace std`

**Namespace aliases** - allowed in `.cc` files and internal namespaces, not in public headers:

```cpp
// In .cc file - OK
namespace sidetable = ::pipeline_diagnostics::sidetable;

// In .h file - only in internal namespaces
namespace internal {
namespace sidetable = ::pipeline_diagnostics::sidetable;
}
```

## Internal Linkage

Give definitions in `.cc` files internal linkage when not referenced elsewhere:

```cpp
// In .cc file
namespace {

void HelperFunction() {
  // Only used in this file
}

}  // namespace

// Or use static
static void AnotherHelper() {
  // Only used in this file
}
```

Do not use unnamed namespaces or `static` in `.h` files.

## Nonmember, Static Member, and Global Functions

- Prefer nonmember functions in a namespace over global functions
- Do not create classes just to group static members
- Use internal linkage for `.cc`-only functions

```cpp
// Good - nonmember in namespace
namespace myproject {
void UtilityFunction();
}

// Bad - class just for static grouping
class MyUtilities {
 public:
  static void Function1();
  static void Function2();
};
```

## Local Variables

- Declare in the narrowest scope possible
- Initialize at declaration
- Declare loop variables in the loop statement

```cpp
// Good
int jobs = NumJobs();
f(jobs);

std::vector<int> v = {1, 2};

while (const char* p = strchr(str, '/')) {
  str = p + 1;
}

// Bad
int i;
i = f();  // Separate declaration and initialization

std::vector<int> v;
v.push_back(1);
v.push_back(2);
```

**Exception**: Move expensive object construction outside loops when performance matters:

```cpp
Foo f;  // Construct once
for (int i = 0; i < 1000000; ++i) {
  f.DoSomething(i);
}
```

## Static and Global Variables

Objects with static storage duration must be **trivially destructible**.

```cpp
// Allowed - trivially destructible
const int kNum = 10;
struct X { int n; };
const X kX[] = {{1}, {2}, {3}};
constexpr std::array<int, 3> kArray = {1, 2, 3};

void foo() {
  static const char* const kMessages[] = {"hello", "world"};  // OK
}

// Bad - non-trivial destructor
const std::string kFoo = "foo";  // Bad
static std::map<int, int> kData = {{1, 0}};  // Bad
```

**Constant initialization** - use `constexpr` or `constinit`:

```cpp
struct Foo { constexpr Foo(int) {} };

constexpr int n = 5;        // OK
constexpr Foo x(2);         // OK
constinit int p = getpid(); // OK if no ordering dependencies
```

**Common patterns for static data:**
- Use `constexpr` `string_view` or char arrays for strings
- Use arrays of trivial types instead of `std::map`/`std::set`
- Use function-local static pointer: `static const auto& impl = *new T(args...);`

## thread_local Variables

Must be initialized with a true compile-time constant at namespace/class scope:

```cpp
// OK - compile-time constant
constinit thread_local Foo foo = ...;

// OK - function scope (no constinit needed)
Foo& MyThreadLocalFoo() {
  thread_local Foo result = ComplicatedInitialization();
  return result;
}
```

Prefer `thread_local` over other thread-local mechanisms. Prefer trivial types to avoid destruction-order issues.
