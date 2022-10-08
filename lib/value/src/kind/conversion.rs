use super::{Collection, Field, Index, Kind};

impl Kind {
    /// Get the inner object collection.
    ///
    /// This returns `None` if the type is not known to be an object.
    #[must_use]
    pub const fn as_object(&self) -> Option<&Collection<Field>> {
        self.object.as_ref()
    }

    /// Get a mutable reference to the inner object collection.
    ///
    /// This returns `None` if the type is not known to be an object.
    #[must_use]
    pub fn as_object_mut(&mut self) -> Option<&mut Collection<Field>> {
        self.object.as_mut()
    }

    /// Take an object `Collection` type out of the `Kind`.
    ///
    /// This returns `None` if the type is not known to be an object.
    #[must_use]
    #[allow(clippy::missing_const_for_fn /* false positive */)]
    pub fn into_object(self) -> Option<Collection<Field>> {
        self.object
    }

    /// Get the inner array collection.
    ///
    /// This returns `None` if the type is not known to be an array.
    #[must_use]
    pub const fn as_array(&self) -> Option<&Collection<Index>> {
        self.array.as_ref()
    }

    /// Get a mutable reference to the inner array collection.
    ///
    /// This returns `None` if the type is not known to be an array.
    #[must_use]
    pub fn as_array_mut(&mut self) -> Option<&mut Collection<Index>> {
        self.array.as_mut()
    }

    /// Take an array `Collection` type out of the `Kind`.
    ///
    /// This returns `None` if the type is not known to be an array.
    #[must_use]
    #[allow(clippy::missing_const_for_fn /* false positive */)]
    pub fn into_array(self) -> Option<Collection<Index>> {
        self.array
    }

    /// Returns `Kind`, with non-primitive states removed.
    ///
    /// That is, it returns `self,` but removes the `object` and `array` states.
    ///
    /// Returns `None` if no primitive states are set.
    #[must_use]
    pub fn to_primitives(mut self) -> Option<Self> {
        self.remove_array().ok()?;
        self.remove_object().ok()?;

        Some(self)
    }
}

impl From<Collection<Field>> for Kind {
    fn from(collection: Collection<Field>) -> Self {
        Self::object(collection)
    }
}

impl From<Collection<Index>> for Kind {
    fn from(collection: Collection<Index>) -> Self {
        Self::array(collection)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use super::*;

    #[test]
    fn test_to_primitive() {
        struct TestCase {
            kind: Kind,
            want: Option<Kind>,
        }

        for (title, TestCase { kind, want }) in HashMap::from([
            (
                "single primitive",
                TestCase {
                    kind: Kind::bytes(),
                    want: Some(Kind::bytes()),
                },
            ),
            (
                "multiple primitives",
                TestCase {
                    kind: Kind::integer().or_regex(),
                    want: Some(Kind::integer().or_regex()),
                },
            ),
            (
                "array only",
                TestCase {
                    kind: Kind::array(BTreeMap::default()),
                    want: None,
                },
            ),
            (
                "object only",
                TestCase {
                    kind: Kind::object(BTreeMap::default()),
                    want: None,
                },
            ),
            (
                "collections removed",
                TestCase {
                    kind: Kind::timestamp()
                        .or_integer()
                        .or_object(BTreeMap::default())
                        .or_array(BTreeMap::default()),
                    want: Some(Kind::timestamp().or_integer()),
                },
            ),
        ]) {
            assert_eq!(kind.to_primitives(), want, "{}", title);
        }
    }
}
