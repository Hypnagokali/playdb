// ignore dead_code while developing
#[allow(dead_code)]
mod data;
#[allow(dead_code)]
mod database;
#[allow(dead_code)]
mod schema;
#[allow(dead_code)]
mod store;
#[allow(unused)]
mod tree;
#[allow(unused)]
mod freespace;

mod examples;

fn main() {
    examples::sequence_vs_index_bm::start();
}
