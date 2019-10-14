Checklist:

* Review TODO comments in code.
* Review check for ``dead_code`` attributes.
* Check for unwrap() calls. It can be security breach if unwrap
  is called on values from external sources, outside the `rdms` library.
* check fo ok() calls on Result type. It can be security breach if
  errors are ignored.
* Trim trait constraits for exported types, exported functions and
  type/methods/functions defined in core.rs
