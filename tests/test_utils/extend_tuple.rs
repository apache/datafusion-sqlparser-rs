//! Submodule providing the extend tuple trait.

/// Trait for extending a tuple with a new element.
pub trait ExtendTuple<ToExtend> {
    /// The output type after extending the tuple.
    type Output;

    /// Extend a tuple with a new element.
    fn extend_tuple(self, to_extend: ToExtend) -> Self::Output;
}

impl<T1, T2> ExtendTuple<(T1,)> for T2 {
    type Output = (T1, T2);

    fn extend_tuple(self, to_extend: (T1,)) -> Self::Output {
        (to_extend.0, self)
    }
}

impl<T1, T2, T3> ExtendTuple<(T1, T2)> for T3 {
    type Output = (T1, T2, T3);

    fn extend_tuple(self, to_extend: (T1, T2)) -> Self::Output {
        (to_extend.0, to_extend.1, self)
    }
}

impl<T1, T2, T3, T4> ExtendTuple<(T1, T2, T3)> for T4 {
    type Output = (T1, T2, T3, T4);

    fn extend_tuple(self, to_extend: (T1, T2, T3)) -> Self::Output {
        (to_extend.0, to_extend.1, to_extend.2, self)
    }
}

impl<T1, T2, T3, T4, T5> ExtendTuple<(T1, T2, T3, T4)> for T5 {
    type Output = (T1, T2, T3, T4, T5);

    fn extend_tuple(self, to_extend: (T1, T2, T3, T4)) -> Self::Output {
        (to_extend.0, to_extend.1, to_extend.2, to_extend.3, self)
    }
}

impl<T1, T2, T3, T4, T5, T6> ExtendTuple<(T1, T2, T3, T4, T5)> for T6 {
    type Output = (T1, T2, T3, T4, T5, T6);

    fn extend_tuple(self, to_extend: (T1, T2, T3, T4, T5)) -> Self::Output {
        (
            to_extend.0,
            to_extend.1,
            to_extend.2,
            to_extend.3,
            to_extend.4,
            self,
        )
    }
}

impl<T1, T2, T3, T4, T5, T6, T7> ExtendTuple<(T1, T2, T3, T4, T5, T6)> for T7 {
    type Output = (T1, T2, T3, T4, T5, T6, T7);

    fn extend_tuple(self, to_extend: (T1, T2, T3, T4, T5, T6)) -> Self::Output {
        (
            to_extend.0,
            to_extend.1,
            to_extend.2,
            to_extend.3,
            to_extend.4,
            to_extend.5,
            self,
        )
    }
}

impl<T1, T2, T3, T4, T5, T6, T7, T8> ExtendTuple<(T1, T2, T3, T4, T5, T6, T7)> for T8 {
    type Output = (T1, T2, T3, T4, T5, T6, T7, T8);

    fn extend_tuple(self, to_extend: (T1, T2, T3, T4, T5, T6, T7)) -> Self::Output {
        (
            to_extend.0,
            to_extend.1,
            to_extend.2,
            to_extend.3,
            to_extend.4,
            to_extend.5,
            to_extend.6,
            self,
        )
    }
}

impl<T1, T2, T3, T4, T5, T6, T7, T8, T9> ExtendTuple<(T1, T2, T3, T4, T5, T6, T7, T8)> for T9 {
    type Output = (T1, T2, T3, T4, T5, T6, T7, T8, T9);

    fn extend_tuple(self, to_extend: (T1, T2, T3, T4, T5, T6, T7, T8)) -> Self::Output {
        (
            to_extend.0,
            to_extend.1,
            to_extend.2,
            to_extend.3,
            to_extend.4,
            to_extend.5,
            to_extend.6,
            to_extend.7,
            self,
        )
    }
}

impl<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10> ExtendTuple<(T1, T2, T3, T4, T5, T6, T7, T8, T9)>
    for T10
{
    type Output = (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);

    fn extend_tuple(self, to_extend: (T1, T2, T3, T4, T5, T6, T7, T8, T9)) -> Self::Output {
        (
            to_extend.0,
            to_extend.1,
            to_extend.2,
            to_extend.3,
            to_extend.4,
            to_extend.5,
            to_extend.6,
            to_extend.7,
            to_extend.8,
            self,
        )
    }
}

impl<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>
    ExtendTuple<(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)> for T11
{
    type Output = (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);

    fn extend_tuple(self, to_extend: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)) -> Self::Output {
        (
            to_extend.0,
            to_extend.1,
            to_extend.2,
            to_extend.3,
            to_extend.4,
            to_extend.5,
            to_extend.6,
            to_extend.7,
            to_extend.8,
            to_extend.9,
            self,
        )
    }
}

impl<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>
    ExtendTuple<(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11)> for T12
{
    type Output = (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);

    fn extend_tuple(
        self,
        to_extend: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11),
    ) -> Self::Output {
        (
            to_extend.0,
            to_extend.1,
            to_extend.2,
            to_extend.3,
            to_extend.4,
            to_extend.5,
            to_extend.6,
            to_extend.7,
            to_extend.8,
            to_extend.9,
            to_extend.10,
            self,
        )
    }
}
