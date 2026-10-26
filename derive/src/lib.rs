// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Procedural macros for sqlparser.
//!
//! This crate provides:
//! - [`Visit`] and [`VisitMut`] derive macros for AST traversal.
//! - [`derive_dialect!`] macro for creating custom SQL dialects.

use quote::quote;
use syn::parse_macro_input;

mod dialect;
mod visit;

/// Implementation of `#[derive(VisitMut)]`
#[proc_macro_derive(VisitMut, attributes(visit))]
pub fn derive_visit_mut(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as syn::DeriveInput);
    visit::derive_visit(
        input,
        &visit::VisitType {
            visit_trait: quote!(VisitMut),
            visitor_trait: quote!(VisitorMut),
            modifier: Some(quote!(mut)),
        },
    )
}

/// Implementation of `#[derive(Visit)]`
#[proc_macro_derive(Visit, attributes(visit))]
pub fn derive_visit_immutable(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as syn::DeriveInput);
    visit::derive_visit(
        input,
        &visit::VisitType {
            visit_trait: quote!(Visit),
            visitor_trait: quote!(Visitor),
            modifier: None,
        },
    )
}

/// Procedural macro for deriving new SQL dialects.
///
/// Enable the `derive-dialect` feature on the `sqlparser` dependency.
#[proc_macro]
pub fn derive_dialect(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = proc_macro2::TokenStream::from(input);
    quote!(::sqlparser::dialect::__derive_dialect_with_methods!(#input);).into()
}

/// Capture method signatures from the Dialect trait definition.
#[doc(hidden)]
#[proc_macro_attribute]
pub fn dialect_methods(
    _args: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let item = parse_macro_input!(item as syn::ItemTrait);
    dialect::dialect_methods(item).into()
}

/// Expand a dialect using the method signatures supplied by sqlparser.
#[doc(hidden)]
#[proc_macro]
pub fn __derive_dialect(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as dialect::DeriveDialectWithMethods);
    dialect::derive_dialect(input)
}
