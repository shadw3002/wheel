#![feature(negative_impls)]
#![feature(associated_type_defaults)]
#![feature(is_some_and)]
#![feature(integer_atomics)]

extern crate atomic;

mod util;

pub mod spsc;
pub mod mpmc;


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);

    }
}
