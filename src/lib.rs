#![feature(negative_impls)]
#![feature(associated_type_defaults)]
#![feature(asm)]
#![feature(is_some_and)]

extern crate atomic;

pub mod spinlock;
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
