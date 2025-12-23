use std::{cell::RefCell, fmt, rc::{Rc, Weak}};

// find
// add
// remove
// use BTree struct as a wrapper
pub struct Node {
    max_keys: u32, // belongs into wrapper
    is_leaf: bool,
    values: Vec<u32>, 
    keys: Vec<u32>,
    children: Vec<Node>,
    // TODO: replace Rc with Weak
    parent: Option<Rc<RefCell<Node>>>,
}

impl fmt::Debug for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Node")
            .field("is_leaf", &self.is_leaf)
            .field("values", &self.values)
            .field("keys", &self.keys)
            .field("children", &self.children)
            // parent intentionally omitted
            .finish()
    }
}


// implement BTree wrapper later

impl Node {
    pub fn init(max_keys: u32, key: u32, value: u32) -> Self {
        Node {
            max_keys,
            is_leaf: true,
            values: vec![value],
            keys: vec![key],
            children: Vec::new(),
            parent: None,
        }
    }


    fn internal_add_kv(&mut self, (k, v): (u32, Option<u32>)) {
        // TODO: user binary search
        println!("call internal_add_kv with: {}{:?}", k, v);
        let mut i = 0;

        for (index, key) in self.keys.iter().enumerate() {
            i = index;
            if k < *key {
               break;
            }
        }

        self.keys.insert(i, k);

        if let Some(v) = v {
            self.values.insert(i, v);
        }
    }

    fn bubble_up(&mut self, key: u32) {
        // Not yet implemented: just dummy
        self.internal_add_kv((key, None));
    }

    fn bubble_up_to_parent(&mut self, key: u32) -> Rc<RefCell<Node>> {
        match &self.parent {
            Some(p) => {
                let mut p_node = p.borrow_mut();
                
                // split of parent logic here
                p_node.bubble_up(key);
                Rc::clone(p)
            },
            None => {
                let mut new_node = Node {
                    max_keys: self.max_keys,
                    is_leaf: false,
                    values: Vec::new(),
                    keys: Vec::new(),
                    children: Vec::new(),
                    parent: None,
                };

                new_node.internal_add_kv((key, None));
                let new_parent = Rc::new(RefCell::new(new_node));
                self.parent = Some(Rc::clone(&new_parent));

                new_parent
            },
        }
    }

    fn split(mut self) -> Rc<RefCell<Node>> {
        let is_root_and_leaf = self.is_leaf && self.parent.is_none();

        let split_index = (self.max_keys / 2) as usize;
        let left_keys = self.keys[..split_index].to_vec();
        let right_keys = self.keys[split_index..].to_vec();
        let bubble = right_keys[0];

        // Leaf node:
        let left_values = self.values[..split_index].to_vec();
        let right_values = self.values[split_index..].to_vec();

        // later (internal nodes)
        // let mut left_pointers = self.keys[..split_index].to_vec();
        // let mut right_pointers = self.keys[split_index..].to_vec();
        let root = self.bubble_up_to_parent(bubble);
        
        
        let parent = self.parent.take();

        let mut left_children = Vec::new();
        let mut right_children = Vec::new();
        let mut count_pointer = 0;
        for p in self.children.into_iter() {
            count_pointer += 1;
            if count_pointer <= left_keys.len() {
                left_children.push(p); 
            } else {
                right_children.push(p);
            }
        }

        let lnode = Node {
            max_keys: self.max_keys,
            is_leaf: self.is_leaf,
            values: left_values,
            keys: left_keys,
            children: left_children,
            parent: parent.as_ref().map(|p| Rc::clone(&p)),
        };

        let rnode = Node {
            max_keys: self.max_keys,
            is_leaf: self.is_leaf,
            values: right_values,
            keys: right_keys,
            children: right_children,
            parent: parent.as_ref().map(|p| Rc::clone(&p)),
        };

        let rc_pn = parent.unwrap();
        let mut pn = rc_pn.borrow_mut();

        if is_root_and_leaf {
            pn.children.push(lnode);
        }
        pn.children.push(rnode);

        root
    }

}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use crate::tree::Node;

    #[test]
    fn test_split_leaf_having_one_parent() {
        let root = Rc::new(RefCell::new(Node {
            max_keys: 3,
            is_leaf: false,
            values: vec![10, 20],
            keys: vec![10, 20],
            children: Vec::new(),
            parent: None,
        }));

        let child1 = Node {
            max_keys: 3,
            is_leaf: true,
            values: vec![5, 8],
            keys: vec![5, 8],
            children: Vec::new(),
            parent: Some(Rc::clone(&root)),
        };

        let child2 = Node {
            max_keys: 3,
            is_leaf: true,
            values: vec![10, 16],
            keys: vec![10, 16],
            children: Vec::new(),
            parent: Some(Rc::clone(&root)),
        };

        let child3 = Node {
            max_keys: 3,
            is_leaf: true,
            values: vec![20, 30, 40],
            keys: vec![20, 30, 40],
            children: Vec::new(),
            parent: Some(Rc::clone(&root)),
        };

        let mut root_mut = root.borrow_mut();
        root_mut.children.push(child1);
        root_mut.children.push(child2);
        root_mut.children.push(child3);

        // Does not work: do children need to be 
        child3.split();
    }


    #[test]
    fn test_split_root_leaf() {
        let root = Node {
            max_keys: 3,
            is_leaf: true,
            values: vec![10, 20, 30],
            keys: vec![10, 20, 30],
            children: Vec::new(),
            parent: None,
        };

        let new_root = root.split();

        assert!(new_root.borrow().keys.contains(&20));
        assert!(new_root.borrow().keys.len() == 1);
        let children = &new_root.borrow().children;

        assert!(children[0].keys.contains(&10));
        assert!(children[0].keys.len() == 1);

        assert!(children[1].keys.contains(&20) && children[1].keys.contains(&30));
        assert!(children[1].keys.len() == 2);
    }

}