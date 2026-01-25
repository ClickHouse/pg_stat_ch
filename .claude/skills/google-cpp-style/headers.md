# Header Files

## Self-Contained Headers

Headers must be self-contained (compile on their own) and end in `.h`. All headers should:
- Have header guards
- Include all headers they need
- Not rely on transitive inclusions

## The #define Guard

Format: `<PROJECT>_<PATH>_<FILE>_H_`

```cpp
// File: foo/src/bar/baz.h
#ifndef FOO_BAR_BAZ_H_
#define FOO_BAR_BAZ_H_

// ... contents ...

#endif  // FOO_BAR_BAZ_H_
```

## Include What You Use

If a file refers to a symbol, it must directly include the header that provides it. Do not rely on transitive inclusions.

```cpp
// foo.cc uses string and vector
#include <string>   // Required - don't rely on bar.h including it
#include <vector>   // Required
#include "bar.h"
```

## Forward Declarations

**Avoid forward declarations.** Include the headers you need instead.

Forward declarations:
- Can hide dependencies
- May break with library changes
- Make tooling harder
- Can change code behavior subtly

```cpp
// Bad - forward declaration
class B;
void FuncInB();

// Good - include the header
#include "b.h"
```

**Exception**: Forward declaring symbols from `namespace std` is undefined behavior.

## Inline Functions

Only define functions inline in headers when:
- The function is short (≤10 lines)
- Performance requires it

Put longer definitions in `.cc` files or in an internal section of the header.

```cpp
class Foo {
 public:
  int bar() { return bar_; }  // OK - short accessor

  void MethodWithHugeBody();  // Declaration only

 private:
  int bar_;
};

// Below public API or in .cc file
void Foo::MethodWithHugeBody() {
  // Long implementation...
}
```

## Names and Order of Includes

Order includes as follows, with blank lines between groups:

1. Related header (`dir2/foo2.h` for `dir/foo.cc`)
2. C system headers (`<unistd.h>`, `<stdlib.h>`)
3. C++ standard library headers (`<algorithm>`, `<string>`)
4. Other libraries' headers
5. Your project's headers

```cpp
#include "foo/server/fooserver.h"

#include <sys/types.h>
#include <unistd.h>

#include <string>
#include <vector>

#include "base/basictypes.h"
#include "foo/server/bar.h"
#include "third_party/absl/flags/flag.h"
```

**Rules:**
- Use angle brackets only for system/standard headers
- Sort alphabetically within each group
- No UNIX directory aliases (`.` or `..`)

**Conditional includes** (platform-specific) go after other includes:

```cpp
#include "foo/public/fooserver.h"

#ifdef _WIN32
#include <windows.h>
#endif
```
