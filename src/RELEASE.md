Checklist:

* Review TODO comments in code.
* Review check for ``dead_code`` attributes.
* Check for unwrap() calls. It can be security breach if unwrap
  is called on values from external sources, outside the `rdms` library.
