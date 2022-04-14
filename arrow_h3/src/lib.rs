pub mod error;
pub mod frame;

pub use error::Error;
pub use frame::H3DataFrame;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
