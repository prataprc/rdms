use super::*;

#[test]
fn test_name() {
    let name = Name("somename-0-dgmlevel-0".to_string());
    let parts: Option<(String, usize)> = name.clone().into();
    assert!(parts.is_some());
    let (s, n) = parts.unwrap();
    assert_eq!(s, "somename-0".to_string());
    assert_eq!(n, 0);

    let name1: Name = (s, n).into();
    assert_eq!(name.0, name1.0);

    assert_eq!(name1.next().0, "somename-0-dgmlevel-1".to_string());
}
