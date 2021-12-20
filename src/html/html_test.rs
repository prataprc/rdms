use crate::html::{Doctype, Dom};
use crate::parsec::Lex;

use super::*;

#[test]
fn test_html1() {
    let text = r"<!DOCTYPE html>";

    let mut lex = Lex::new(text.to_string());

    let parser = new_parser().unwrap();
    let node = parser.parse(&mut lex).unwrap().unwrap();
    assert_eq!(node.to_string(), text);
    let dom = Dom::from_node(node).unwrap();

    let ref_dom = Dom::Doc {
        doc_type: Some(Doctype { legacy: None }),
        root_elements: Vec::default(),
    };
    assert_eq!(dom, ref_dom);
}

const TEST_HTML2_TEXT: &'static str = r#"
<p>
 <svg>
  <metadata>
   <!-- this is comment -->
  </metadata>
 </svg>
</p>"#;
#[test]
fn test_html2() {
    let mut lex = Lex::new(TEST_HTML2_TEXT.to_string());

    let parser = new_parser().unwrap();
    let node = parser.parse(&mut lex).unwrap().unwrap();
    assert_eq!(node.to_string(), TEST_HTML2_TEXT);
    let dom = Dom::from_node(node).unwrap();

    let ref_dom = Dom::Doc {
        doc_type: None,
        root_elements: vec![Dom::Tag {
            tag_name: "p".to_string(),
            attrs: vec![],
            tag_children: vec![
                Dom::Text {
                    text: "\n ".to_string(),
                },
                Dom::Tag {
                    tag_name: "svg".to_string(),
                    attrs: vec![],
                    tag_children: vec![
                        Dom::Text {
                            text: "\n  ".to_string(),
                        },
                        Dom::Tag {
                            tag_name: "metadata".to_string(),
                            attrs: vec![],
                            tag_children: vec![
                                Dom::Text {
                                    text: "\n   ".to_string(),
                                },
                                Dom::Comment {
                                    text: "<!-- this is comment -->".to_string(),
                                },
                                Dom::Text {
                                    text: "\n  ".to_string(),
                                },
                            ],
                        },
                        Dom::Text {
                            text: "\n ".to_string(),
                        },
                    ],
                },
                Dom::Text {
                    text: "\n".to_string(),
                },
            ],
        }],
    };
    assert_eq!(dom, ref_dom);
}

#[test]
fn test_html3() {
    let text = r"<input disabled/>";

    let mut lex = Lex::new(text.to_string());

    let parser = new_parser().unwrap();
    let node = parser.parse(&mut lex).unwrap().unwrap();
    assert_eq!(node.to_string(), text);
    let dom = Dom::from_node(node).unwrap();

    let ref_dom = Dom::Doc {
        doc_type: None,
        root_elements: vec![Dom::Tag {
            tag_name: "input".to_string(),
            attrs: vec![Attribute {
                key: "disabled".to_string(),
                value: None,
            }],
            tag_children: vec![],
        }],
    };

    assert_eq!(dom, ref_dom);
}
