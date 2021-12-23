use std::{
    cell::RefCell,
    rc::{Rc, Weak},
};

use crate::parsec::Lex;
use crate::web::html::{Doctype, Dom};

use super::*;

#[test]
fn test_html1() {
    let text = r"<!DOCTYPE html>";

    let mut lex = Lex::new(text.to_string());

    let parser = new_parser().unwrap();
    let node = parser.parse(&mut lex).unwrap().unwrap();
    assert_eq!(node.to_string(), text);
    let dom = Dom::from_node(node).unwrap();

    let ref_dom = Rc::new(Dom::Doc {
        doc_type: Some(Doctype { legacy: None }),
        root_elements: Vec::default(),
    });
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

    let ref_dom = Rc::new(Dom::Doc {
        doc_type: None,
        root_elements: vec![
            Rc::new(Dom::Text {
                text: "\n".to_string(),
                parent: RefCell::new(Weak::new()),
            }),
            Rc::new(Dom::Tag {
                tag_name: "p".to_string(),
                attrs: vec![],
                tag_children: vec![
                    Rc::new(Dom::Text {
                        text: "\n ".to_string(),
                        parent: RefCell::new(Weak::new()),
                    }),
                    Rc::new(Dom::Tag {
                        tag_name: "svg".to_string(),
                        attrs: vec![],
                        tag_children: vec![
                            Rc::new(Dom::Text {
                                text: "\n  ".to_string(),
                                parent: RefCell::new(Weak::new()),
                            }),
                            Rc::new(Dom::Tag {
                                tag_name: "metadata".to_string(),
                                attrs: vec![],
                                tag_children: vec![
                                    Rc::new(Dom::Text {
                                        text: "\n   ".to_string(),
                                        parent: RefCell::new(Weak::new()),
                                    }),
                                    Rc::new(Dom::Comment {
                                        text: "<!-- this is comment -->".to_string(),
                                        parent: RefCell::new(Weak::new()),
                                    }),
                                    Rc::new(Dom::Text {
                                        text: "\n  ".to_string(),
                                        parent: RefCell::new(Weak::new()),
                                    }),
                                ],
                                parent: RefCell::new(Weak::new()),
                            }),
                            Rc::new(Dom::Text {
                                text: "\n ".to_string(),
                                parent: RefCell::new(Weak::new()),
                            }),
                        ],
                        parent: RefCell::new(Weak::new()),
                    }),
                    Rc::new(Dom::Text {
                        text: "\n".to_string(),
                        parent: RefCell::new(Weak::new()),
                    }),
                ],
                parent: RefCell::new(Weak::new()),
            }),
        ],
    });

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

    let ref_dom = Rc::new(Dom::Doc {
        doc_type: None,
        root_elements: vec![Rc::new(Dom::Tag {
            tag_name: "input".to_string(),
            attrs: vec![Attribute {
                key: "disabled".to_string(),
                value: None,
            }],
            tag_children: vec![],
            parent: RefCell::new(Weak::new()),
        })],
    });

    assert_eq!(dom, ref_dom);
}

#[test]
fn test_html4() {
    let text = r"<input value=yes></input>";

    let mut lex = Lex::new(text.to_string());

    let parser = new_parser().unwrap();
    let node = parser.parse(&mut lex).unwrap().unwrap();
    assert_eq!(node.to_string(), text);
    let dom = Dom::from_node(node).unwrap();

    let ref_dom = Rc::new(Dom::Doc {
        doc_type: None,
        root_elements: vec![Rc::new(Dom::Tag {
            tag_name: "input".to_string(),
            attrs: vec![Attribute {
                key: "value".to_string(),
                value: Some("yes".to_string()),
            }],
            tag_children: vec![],
            parent: RefCell::new(Weak::new()),
        })],
    });

    assert_eq!(dom, ref_dom);
}

#[test]
fn test_html5() {
    let text = r"<input type='checkbox'/>";

    let mut lex = Lex::new(text.to_string());

    let parser = new_parser().unwrap();
    let node = parser.parse(&mut lex).unwrap().unwrap();
    assert_eq!(node.to_string(), text);
    let dom = Dom::from_node(node).unwrap();

    let ref_dom = Rc::new(Dom::Doc {
        doc_type: None,
        root_elements: vec![Rc::new(Dom::Tag {
            tag_name: "input".to_string(),
            attrs: vec![Attribute {
                key: "type".to_string(),
                value: Some("'checkbox'".to_string()),
            }],
            tag_children: vec![],
            parent: RefCell::new(Weak::new()),
        })],
    });

    assert_eq!(dom, ref_dom);
}

#[test]
fn test_html6() {
    let text = r#"<input name="be evil"/>"#;

    let mut lex = Lex::new(text.to_string());

    let parser = new_parser().unwrap();
    let node = parser.parse(&mut lex).unwrap().unwrap();
    assert_eq!(node.to_string(), text);
    let dom = Dom::from_node(node).unwrap();

    let ref_dom = Rc::new(Dom::Doc {
        doc_type: None,
        root_elements: vec![Rc::new(Dom::Tag {
            tag_name: "input".to_string(),
            attrs: vec![Attribute {
                key: "name".to_string(),
                value: Some(r#""be evil""#.to_string()),
            }],
            tag_children: vec![],
            parent: RefCell::new(Weak::new()),
        })],
    });

    assert_eq!(dom, ref_dom);
}
