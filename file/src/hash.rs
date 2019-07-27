//! This module defines the struct `Hash` that represents the SHA256 hash of a serialized object

use data_encoding::HEXLOWER;
use ring::digest::{Context, SHA256};
use serde::{Serialize, Deserialize};

/// A struct that represents the SHA256 hash of a serialized object.
///
/// # Examples
///
/// ```
/// use histo_graph_core::graph::graph::VertexId;
/// use histo_graph_file::Hash;
///
/// # fn main() -> std::result::Result<(), bincode::Error> {
/// let id = 27u64;
/// let serialized: Vec<u8> = bincode::serialize(&id)?;
/// let hash: Hash = (&serialized).into();
/// let str = hash.to_string();
/// assert_eq!(str, "4d159113222bfeb85fbe717cc2393ee8a6a85b7ce5ac1791c4eade5e3dd6de41");
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct Hash([u8; 32]);

impl Hash {

    /// turns the hash into a `String`, so that it can be used a a filename.
    pub fn to_string(&self) -> String {
        HEXLOWER.encode(&self.0)
    }
}

impl<T> From<T> for Hash
    where T: AsRef<[u8]> {
    /// transforms a serialized object into a `Hash`.
    ///
    /// # Examples
    ///
    /// ```
    /// use histo_graph_core::graph::graph::VertexId;
    /// use histo_graph_file::Hash;
    ///
    /// # fn main() -> std::result::Result<(), bincode::Error> {
    /// let id = 27u64;
    /// let serialized: Vec<u8> = bincode::serialize(&id)?;
    /// let hash: Hash = (&serialized).into();
    /// # Ok(())
    /// # }
    /// ```
    fn from(content: T) -> Hash {
        let mut context = Context::new(&SHA256);
        context.update(content.as_ref());
        let digest = context.finish();
        let mut hash: [u8; 32] = [0u8; 32];
        hash.copy_from_slice(digest.as_ref());

        Hash(hash)
    }
}
