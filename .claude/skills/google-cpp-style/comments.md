# Comments

## Comment Style

Use `//` or `/* */` consistently. `//` is more common.

## File Comments

Start with license boilerplate. Include a file comment only if declaring multiple abstractions:

```cpp
// Copyright 2024 Google LLC
// 
// Licensed under the Apache License, Version 2.0 ...

// This file contains utilities for processing network requests.
// It provides the HttpClient class and related helper functions.
```

Don't duplicate file comments in `.h` and `.cc` - put it where the interface is defined.

## Class Comments

Document what the class is for and how to use it:

```cpp
// Iterates over the contents of a GargantuanTable.
// Example:
//    std::unique_ptr<GargantuanTableIterator> iter = table->NewIterator();
//    for (iter->Seek("foo"); !iter->done(); iter->Next()) {
//      process(iter->key(), iter->value());
//    }
class GargantuanTableIterator {
  // ...
};
```

Include:
- Purpose and usage
- Thread safety requirements
- Small example code when helpful

## Function Comments

### Declaration comments

Document what the function does and how to use it:

```cpp
// Returns an iterator for this table, positioned at the first entry
// lexically greater than or equal to `start_word`. If there is no
// such entry, returns a null pointer. The client must not use the
// iterator after the underlying GargantuanTable has been destroyed.
//
// This method is equivalent to:
//    std::unique_ptr<Iterator> iter = table->NewIterator();
//    iter->Seek(start_word);
//    return iter;
std::unique_ptr<Iterator> GetIterator(absl::string_view start_word) const;
```

Document:
- What inputs and outputs are (use backticks for parameter names)
- Ownership semantics for pointers
- Whether null is allowed
- Performance implications

### Definition comments

Explain how the function works (tricky parts, algorithms used):

```cpp
void MyClass::Process() {
  // We use a two-phase approach here because...
  // First, collect all items that need processing.
  // ...
}
```

Don't repeat the declaration comment.

### Overrides

Focus on specifics of the override, not the base class behavior:

```cpp
// Overrides base class to also log the operation.
void LoggingWidget::DoOperation() override;
```

## Variable Comments

### Class data members

Comment if the purpose isn't obvious:

```cpp
private:
 // Used to bounds-check table accesses. -1 means
 // that we don't yet know how many entries the table has.
 int num_total_entries_;
```

### Global variables

Always comment:

```cpp
// The total number of test cases that we run through in this regression test.
const int kNumTestCases = 6;
```

## Implementation Comments

### Tricky code

Explain non-obvious code before it:

```cpp
// Divide result by two, rounding down, using a bit shift because
// the standard division operator rounds toward zero for negative numbers.
result >>= 1;
```

### Function argument comments

Use when arguments aren't self-documenting:

```cpp
// Prefer named variables or enums
ProductOptions options;
options.set_precision_decimals(7);
const DecimalNumber product = CalculateProduct(values, options);

// Or use comments for clarity
const DecimalNumber product =
    CalculateProduct(values, options, /*completion_callback=*/nullptr);
```

### Don'ts

Don't state the obvious:

```cpp
// Bad - obvious
// Find the element in the vector.
if (std::find(v.begin(), v.end(), element) != v.end()) {

// Good - explains why
// Process "element" unless it was already processed.
if (std::find(v.begin(), v.end(), element) != v.end()) {

// Better - self-documenting
if (!IsAlreadyProcessed(element)) {
```

## Punctuation, Spelling, and Grammar

Write comments as readable prose with proper punctuation. Complete sentences are more readable than fragments.

## TODO Comments

Format: `TODO: context - explanation`

```cpp
// TODO: bug 12345678 - Remove this after the 2047q4 compatibility window expires.
// TODO: example.com/my-design-doc - Manually fix up this code the next time it's touched.
// TODO(bug 12345678): Update this list after the Foo service is turned down.
// TODO(John): Use a "*" here for concatenation operator.
```

Include either:
- A bug number
- A username
- A specific date/event ("Fix by November 2025")
