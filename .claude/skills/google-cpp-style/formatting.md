# Formatting

## Line Length

**80 characters maximum.**

Exceptions:
- Comment lines with URLs or long commands
- Include statements
- Header guards
- Using-declarations

## Non-ASCII Characters

- Use UTF-8 encoding
- Use non-ASCII characters rarely (unit tests, data parsing)
- Prefer hex encoding for clarity: `"\xEF\xBB\xBF"` or `"\uFEFF"`
- Don't use `u8` prefix, `wchar_t`, `char16_t`, `char32_t`

## Spaces vs. Tabs

**Use only spaces. 2-space indent.**

## Function Declarations

```cpp
// Fits on one line
ReturnType ClassName::FunctionName(Type par1, Type par2) {

// Wrap at parenthesis
ReturnType ClassName::ReallyLongFunctionName(Type par_name1, Type par_name2,
                                             Type par_name3) {

// 4-space indent if first param doesn't fit
ReturnType LongClassName::ReallyReallyReallyLongFunctionName(
    Type par_name1,  // 4 space indent
    Type par_name2,
    Type par_name3) {
  DoSomething();  // 2 space indent
}
```

**Rules:**
- Return type on same line as function name (or break before function name)
- No space between function name and `(`
- Open brace on same line as declaration
- Align parameters when wrapping

## Lambda Expressions

```cpp
int x = 0;
auto x_plus_n = [&x](int n) -> int { return x + n; };

// Short lambdas inline
digits.erase(std::remove_if(digits.begin(), digits.end(), [&to_remove](int i) {
               return to_remove.contains(i);
             }),
             digits.end());
```

## Floating-Point Literals

Always include radix point with digits on both sides:

```cpp
// Good
float f = 1.0f;
double d = 1248.0e6;
long double ld = -0.5L;

// Bad
float f = 1.f;        // Missing leading digit
double d = 1248e6;    // Missing radix point
```

## Function Calls

```cpp
// Fits on one line
bool result = DoSomething(argument1, argument2, argument3);

// Align at parenthesis
bool result = DoSomething(averyveryveryverylongargument1,
                          argument2, argument3);

// 4-space indent
if (...) {
  bool result = DoSomething(
      argument1, argument2,
      argument3, argument4);
}
```

## Braced Initializer Lists

Format like function calls:

```cpp
return {foo, bar};
std::pair<int, int> p{foo, bar};

SomeType variable{
    "This is too long to fit all in one line"};
```

## Looping and Branching

```cpp
if (condition) {                   // Space after keyword, before brace
  DoOneThing();
  DoAnotherThing();
} else if (int a = f(); a != 3) {  // Closing brace, else on same line
  DoAThirdThing(a);
} else {
  DoNothing();
}

while (condition) {
  RepeatAThing();
}

for (int i = 0; i < 10; ++i) {
  RepeatAThing();
}

switch (var) {
  case 0: {
    Foo();
    break;
  }
  default: {
    Bar();
  }
}
```

**Always use braces**, except for single-line statements:

```cpp
// OK - fits on one line
if (x == kFoo) return new Foo();

// OK - condition and body each fit on one line
if (x == kBar)
  Bar(arg1, arg2, arg3);
```

**Fall-through** requires `[[fallthrough]]`:

```cpp
switch (x) {
  case 41:
  case 43:  // No annotation needed for empty cases
    if (condition) {
      [[fallthrough]];
    }
    break;
  case 42:
    DoSomething();
    [[fallthrough]];
  default:
    DoDefault();
    break;
}
```

## Pointer and Reference Expressions

```cpp
x = *p;
p = &x;
x = r.y;
x = r->y;

// Type declarations - no space between type and */&
char* c;
const std::string& str;
std::vector<char*> v;

// Multiple declarations with pointers - disallowed
int x, *y;   // Bad
int* x, *y;  // Bad
```

## Boolean Expressions

Break after operator, keep operators at end of line:

```cpp
if (this_one_thing > this_other_thing &&
    a_third_thing == a_fourth_thing &&
    yet_another && last_one) {
```

Use `&&` and `||`, not `and` and `or`.

## Return Values

No parentheses unless needed for readability:

```cpp
return result;

return (some_long_condition &&
        another_condition);

// Bad
return (value);
return(result);
```

## Variable Initialization

All forms are acceptable:

```cpp
int x = 3;
int x(3);
int x{3};  // Prevents narrowing

std::vector<int> v(100, 1);  // 100 ones
std::vector<int> v{100, 1};  // Two elements: 100, 1
```

## Preprocessor Directives

Always at beginning of line:

```cpp
  if (lopsided_score) {
#if DISASTER_PENDING      // Correct - at line start
    DropEverything();
#endif
    BackToNormal();
  }
```

## Class Format

```cpp
class MyClass : public OtherClass {
 public:      // 1 space indent
  MyClass();  // 2 space indent
  explicit MyClass(int var);
  ~MyClass() {}

  void SomeFunction();
  void set_some_var(int var) { some_var_ = var; }
  int some_var() const { return some_var_; }

 private:
  bool SomeInternalFunction();

  int some_var_;
  int some_other_var_;
};
```

## Constructor Initializer Lists

```cpp
// All on one line
MyClass::MyClass(int var) : some_var_(var) {

// Wrap before colon, 4-space indent
MyClass::MyClass(int var)
    : some_var_(var), some_other_var_(var + 1) {

// One per line when it spans multiple lines
MyClass::MyClass(int var)
    : some_var_(var),
      some_other_var_(var + 1) {
```

## Namespace Formatting

No indentation inside namespaces:

```cpp
namespace outer {
namespace inner {

void foo() {  // No extra indentation
  // ...
}

}  // namespace inner
}  // namespace outer
```

## Horizontal Whitespace

```cpp
int i = 0;  // Two spaces before end-of-line comments
void f(bool b) {  // Space before {
int x[] = {0};    // Spaces inside braces optional, be consistent

// Operators
x = 0;            // Spaces around assignment
v = w * x + y/z;  // OK to omit around factors

// Loops and conditionals
if (condition) {  // Space after keyword
while (test) {}   // Usually no space inside ()
for (int i = 0; i < 5; ++i) {
for (auto x : counts) {  // Space around : in range-for
```

## Vertical Whitespace

Use sparingly. Don't add blank lines:
- At start/end of code blocks
- Where indentation shows structure

Use blank lines to separate logical sections (like paragraph breaks).
