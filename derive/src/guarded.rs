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

//! Implementation of the `GuardedClone` and `GuardedPartialEq` derive macros.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::ext::IdentExt;
use syn::{parse_quote, Data, DeriveInput, Fields, GenericParam, Generics, Ident};

pub(crate) fn derive_guarded_clone(input: DeriveInput) -> TokenStream {
    let name = &input.ident;
    let generics = add_trait_bounds(input.generics.clone(), quote!(::core::clone::Clone));
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let body = match &input.data {
        Data::Struct(data) => {
            let (pattern, bindings) = bind_fields(&data.fields, "self");
            let copy = rebuild(&data.fields, &bindings);
            quote! {
                let Self #pattern = self;
                Self #copy
            }
        }
        Data::Enum(data) if data.variants.is_empty() => quote!(match *self {}),
        Data::Enum(data) => {
            let arms = data.variants.iter().map(|variant| {
                let variant_name = &variant.ident;
                let (pattern, bindings) = bind_fields(&variant.fields, "self");
                let copy = rebuild(&variant.fields, &bindings);
                quote!(Self::#variant_name #pattern => Self::#variant_name #copy)
            });
            quote! {
                match self {
                    #(#arms,)*
                }
            }
        }
        Data::Union(data) => {
            return syn::Error::new(data.union_token.span, "unions are not supported")
                .to_compile_error()
        }
    };

    quote! {
        #[automatically_derived]
        impl #impl_generics ::core::clone::Clone for #name #ty_generics #where_clause {
            #[cfg_attr(feature = "recursive-protection", recursive::recursive)]
            fn clone(&self) -> Self {
                #body
            }
        }
    }
}

pub(crate) fn derive_guarded_partial_eq(input: DeriveInput) -> TokenStream {
    let name = &input.ident;
    let generics = add_trait_bounds(input.generics.clone(), quote!(::core::cmp::PartialEq));
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let body = match &input.data {
        Data::Struct(data) => {
            let (self_pattern, self_bindings) = bind_fields(&data.fields, "self");
            let (other_pattern, other_bindings) = bind_fields(&data.fields, "other");
            let equal = all_equal(&self_bindings, &other_bindings);
            quote! {
                let Self #self_pattern = self;
                let Self #other_pattern = other;
                #equal
            }
        }
        Data::Enum(data) if data.variants.is_empty() => quote!(match *self {}),
        Data::Enum(data) => {
            let arms = data.variants.iter().map(|variant| {
                let variant_name = &variant.ident;
                let (self_pattern, self_bindings) = bind_fields(&variant.fields, "self");
                let (other_pattern, other_bindings) = bind_fields(&variant.fields, "other");
                let equal = all_equal(&self_bindings, &other_bindings);
                quote!((Self::#variant_name #self_pattern, Self::#variant_name #other_pattern) => #equal)
            });
            let mismatch = (data.variants.len() > 1).then(|| quote!(_ => false,));
            quote! {
                match (self, other) {
                    #(#arms,)*
                    #mismatch
                }
            }
        }
        Data::Union(data) => {
            return syn::Error::new(data.union_token.span, "unions are not supported")
                .to_compile_error()
        }
    };

    quote! {
        #[automatically_derived]
        impl #impl_generics ::core::cmp::PartialEq for #name #ty_generics #where_clause {
            #[cfg_attr(feature = "recursive-protection", recursive::recursive)]
            fn eq(&self, other: &Self) -> bool {
                #body
            }
        }
    }
}

fn add_trait_bounds(mut generics: Generics, bound: TokenStream) -> Generics {
    for param in &mut generics.params {
        if let GenericParam::Type(type_param) = param {
            type_param.bounds.push(parse_quote!(#bound));
        }
    }
    generics
}

/// Returns a pattern binding every field to `{prefix}_{field}`, with the bindings in field order.
fn bind_fields(fields: &Fields, prefix: &str) -> (TokenStream, Vec<Ident>) {
    match fields {
        Fields::Named(named) => {
            let names: Vec<&Ident> = named.named.iter().flat_map(|field| &field.ident).collect();
            let bindings: Vec<Ident> = names
                .iter()
                .map(|name| format_ident!("{}_{}", prefix, name.unraw()))
                .collect();
            (quote!({ #(#names: #bindings),* }), bindings)
        }
        Fields::Unnamed(unnamed) => {
            let bindings: Vec<Ident> = (0..unnamed.unnamed.len())
                .map(|index| format_ident!("{}_{}", prefix, index))
                .collect();
            (quote!(( #(#bindings),* )), bindings)
        }
        Fields::Unit => (TokenStream::new(), Vec::new()),
    }
}

/// Builds the fields of a value of the same shape from clones of `bindings`.
fn rebuild(fields: &Fields, bindings: &[Ident]) -> TokenStream {
    match fields {
        Fields::Named(named) => {
            let names = named.named.iter().flat_map(|field| &field.ident);
            quote!({ #(#names: ::core::clone::Clone::clone(#bindings)),* })
        }
        Fields::Unnamed(_) => quote!(( #(::core::clone::Clone::clone(#bindings)),* )),
        Fields::Unit => TokenStream::new(),
    }
}

fn all_equal(left: &[Ident], right: &[Ident]) -> TokenStream {
    if left.is_empty() {
        quote!(true)
    } else {
        quote!(#(#left == #right)&&*)
    }
}
