use crate::parsec::Node;

pub enum Dom {
    Doctype { legacy: String },
}

impl Dom {
    pub fn from_node(node: Node) -> Dom {
        match node {
            Node::M { name, children } if name == "DOCTYPE" => Dom::Doctype {
                legacy: children[3].to_text(),
            },
            _ => unreachable!(),
        }
    }
}
