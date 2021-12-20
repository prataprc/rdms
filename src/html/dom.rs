use crate::parsec::Node;

#[derive(Clone, Debug, PartialEq)]
pub enum Dom {
    Doc {
        doc_type: Option<Doctype>,
        root_elements: Vec<Dom>,
    },
    Tag {
        tag_name: String,
        attrs: Vec<Attribute>,
        tag_children: Vec<Dom>,
    },
    Comment {
        text: String,
    },
    Text {
        text: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct Doctype {
    pub legacy: Option<String>,
}

impl ToString for Doctype {
    fn to_string(&self) -> String {
        format!(
            "{{ legacy: {} }}",
            self.legacy.as_ref().map(|x| x.as_str()).unwrap_or("")
        )
    }
}

impl Doctype {
    fn from_node(node: Node) -> Doctype {
        #[cfg(feature = "debug")]
        println!("Doctype::from_node {}", node.to_name());

        let val = Doctype {
            legacy: match node.into_children().remove(3).into_child() {
                Some(node) => Some(node.into_text()),
                None => None,
            },
        };
        val
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Attribute {
    pub key: String,
    pub value: Option<String>,
}

impl ToString for Attribute {
    fn to_string(&self) -> String {
        match &self.value {
            Some(value) => format!("{}={}", self.key, value),
            None => format!("{}", self.key),
        }
    }
}

impl Attribute {
    fn from_node(node: Node) -> Attribute {
        match node.to_name().as_str() {
            "WS_ATTRIBUTE" => Attribute::from_node(node.into_children().remove(1)),
            "ATTR_KEY_TOK" => {
                let attr = Attribute {
                    key: node.into_text(),
                    value: None,
                };
                attr
            }
            "ATTR_KEY_VALUE" => {
                let mut children = node.into_children();
                let attr = Attribute {
                    key: children.remove(0).into_text(),
                    value: Some(children.remove(3).into_text()),
                };
                attr
            }
            _ => unreachable!(),
        }
    }

    pub fn unwrap(self) -> (String, Option<String>) {
        let value = match self.value {
            Some(value) => {
                let (a, z) = (0, value.len().saturating_sub(1));
                let chars: Vec<char> = value.clone().chars().collect();
                let value = match (chars.first(), chars.last()) {
                    (Some('"'), Some('"')) => value[(a + 1)..z].to_string(),
                    (Some('\''), Some('\'')) => value[(a + 1)..z].to_string(),
                    (_, _) => value,
                };
                Some(value)
            }
            None => None,
        };
        (self.key, value)
    }
}

impl Dom {
    pub fn from_node(node: Node) -> Option<Dom> {
        #[cfg(feature = "debug")]
        println!("Dom for node {}", node.to_name());

        match node.to_name().as_str() {
            "DOC" => {
                let mut children = node.into_children();
                let doc_type = children.remove(0).into_child().map(Doctype::from_node);

                let dom = Dom::Doc {
                    doc_type,
                    root_elements: children
                        .remove(0)
                        .into_children()
                        .into_iter()
                        .filter_map(|cs| Dom::from_node(cs.into_children().remove(1)))
                        .collect(),
                };
                Some(dom)
            }
            "ELEMENT_INLINE" => {
                let dom = Dom::Tag {
                    tag_name: node.into_children().remove(1).into_text(),
                    attrs: Vec::default(),
                    tag_children: Vec::default(),
                };
                Some(dom)
            }
            "ELEMENT_INLINE_TAG_ATTRS" => {
                let mut children = node.into_children();

                let tag_name = children.remove(1).into_text();
                let attrs: Vec<Attribute> = children
                    .remove(1)
                    .into_children()
                    .into_iter()
                    .filter_map(|n| Some(Attribute::from_node(n)))
                    .collect();

                let dom = Dom::Tag {
                    tag_name,
                    attrs,
                    tag_children: Vec::default(),
                };
                Some(dom)
            }
            "ELEMENT" => {
                let mut children = node.into_children();
                let mut tag = Dom::from_node(children.remove(0))?;

                let mut tag_children: Vec<Dom> = children
                    .remove(0)
                    .into_children()
                    .into_iter()
                    .filter_map(|n| {
                        Some(n.into_children().into_iter().filter_map(Dom::from_node))
                    })
                    .flatten()
                    .collect();

                Dom::from_node(children.remove(0)).map(|n| tag_children.push(n));

                let end_tag = Dom::from_node(children.remove(0)).unwrap();

                assert_eq!(end_tag.to_tag_name(), tag.to_tag_name());

                tag.set_tag_chilren(tag_children);
                Some(tag)
            }
            "START_TAG" => {
                let dom = Dom::Tag {
                    tag_name: node.into_children().remove(1).into_text(),
                    attrs: Vec::default(),
                    tag_children: Vec::default(),
                };
                Some(dom)
            }
            "START_TAG_ATTRS" => {
                let mut children = node.into_children();
                let tag_name = children.remove(1).into_text();
                let attrs: Vec<Attribute> = children
                    .remove(1)
                    .into_children()
                    .into_iter()
                    .filter_map(|n| Some(Attribute::from_node(n)))
                    .collect();

                let dom = Dom::Tag {
                    tag_name,
                    attrs,
                    tag_children: Vec::default(),
                };
                Some(dom)
            }
            "END_TAG" => {
                let dom = Dom::Tag {
                    tag_name: node.into_children().remove(1).into_text(),
                    attrs: Vec::default(),
                    tag_children: Vec::default(),
                };
                Some(dom)
            }
            "TEXT" => node.into_child().map(|n| Dom::Text {
                text: n.into_text(),
            }),
            "COMMENT" => {
                let dom = Dom::Comment {
                    text: node.into_text(),
                };
                Some(dom)
            }
            name => panic!("{}", name),
        }
    }

    fn to_tag_name(&self) -> String {
        match self {
            Dom::Tag { tag_name, .. } => tag_name.to_string(),
            _ => unreachable!(),
        }
    }

    fn set_tag_chilren(&mut self, children: Vec<Dom>) {
        match self {
            Dom::Tag { tag_children, .. } => *tag_children = children,
            _ => unreachable!(),
        }
    }

    pub fn pretty_print(&self, prefix: &str) {
        match self {
            Dom::Doc {
                doc_type,
                root_elements,
            } => {
                match doc_type {
                    Some(dt) => println!("{}Doctype {}", prefix, dt.to_string()),
                    None => (),
                }
                root_elements
                    .iter()
                    .for_each(|dom| dom.pretty_print(prefix));
            }
            Dom::Tag {
                tag_name,
                attrs,
                tag_children,
            } => {
                let ss: Vec<String> = attrs.iter().map(|a| a.to_string()).collect();
                println!("{}<{} {}>", prefix, tag_name, ss.join(" "));
                let prefix = prefix.to_string() + "  ";
                tag_children.iter().for_each(|t| t.pretty_print(&prefix));
            }
            Dom::Text { text } if text.len() < 20 => println!("{}{}", prefix, text),
            Dom::Text { text } => println!("{}{}", prefix, &text[..20]),
            Dom::Comment { text } if text.len() < 20 => println!("{}{}", prefix, text),
            Dom::Comment { text } => println!("{}{}", prefix, &text[..20]),
        }
    }
}
