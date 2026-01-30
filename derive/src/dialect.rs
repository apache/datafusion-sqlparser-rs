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

//! Implementation of the `derive_dialect!` macro for creating custom SQL dialects.

use proc_macro2::TokenStream;
use quote::{quote, quote_spanned};
use std::collections::HashSet;
use syn::{
    braced,
    parse::{Parse, ParseStream},
    Error, File, FnArg, Ident, Item, LitBool, LitChar, Pat, ReturnType, Signature, Token,
    TraitItem, Type,
};

/// Override value types supported by the macro
pub(crate) enum Override {
    Bool(LitBool),
    Char(LitChar),
    None,
}

/// Parsed input for the `derive_dialect!` macro
pub(crate) struct DeriveDialectInput {
    pub name: Ident,
    pub base: Type,
    pub preserve_type_id: bool,
    pub overrides: Vec<(Ident, Override)>,
}

/// `Dialect` trait method attrs
struct DialectMethod {
    name: Ident,
    signature: Signature,
}

impl Parse for DeriveDialectInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let name: Ident = input.parse()?;
        input.parse::<Token![,]>()?;
        let base: Type = input.parse()?;

        let mut preserve_type_id = false;
        let mut overrides = Vec::new();

        while input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
            if input.is_empty() {
                break;
            }
            if input.peek(Ident) {
                let ident: Ident = input.parse()?;
                match ident.to_string().as_str() {
                    "preserve_type_id" => {
                        input.parse::<Token![=]>()?;
                        preserve_type_id = input.parse::<LitBool>()?.value();
                    }
                    "overrides" => {
                        input.parse::<Token![=]>()?;
                        let content;
                        braced!(content in input);
                        while !content.is_empty() {
                            let key: Ident = content.parse()?;
                            content.parse::<Token![=]>()?;
                            let value = if content.peek(LitBool) {
                                Override::Bool(content.parse()?)
                            } else if content.peek(LitChar) {
                                Override::Char(content.parse()?)
                            } else if content.peek(Ident) {
                                let ident: Ident = content.parse()?;
                                if ident == "None" {
                                    Override::None
                                } else {
                                    return Err(Error::new(
                                        ident.span(),
                                        format!("Expected `true`, `false`, a char, or `None`, found `{ident}`"),
                                    ));
                                }
                            } else {
                                return Err(
                                    content.error("Expected `true`, `false`, a char, or `None`")
                                );
                            };
                            overrides.push((key, value));
                            if content.peek(Token![,]) {
                                content.parse::<Token![,]>()?;
                            }
                        }
                    }
                    other => {
                        return Err(Error::new(ident.span(), format!(
                            "Unknown argument `{other}`. Expected `preserve_type_id` or `overrides`."
                        )));
                    }
                }
            }
        }
        Ok(DeriveDialectInput {
            name,
            base,
            preserve_type_id,
            overrides,
        })
    }
}

/// Entry point for the `derive_dialect!` macro
pub(crate) fn derive_dialect(input: DeriveDialectInput) -> proc_macro::TokenStream {
    let err = |msg: String| {
        Error::new(proc_macro2::Span::call_site(), msg)
            .to_compile_error()
            .into()
    };

    let source = match read_dialect_mod_file() {
        Ok(s) => s,
        Err(e) => return err(format!("Failed to read dialect/mod.rs: {e}")),
    };
    let file: File = match syn::parse_str(&source) {
        Ok(f) => f,
        Err(e) => return err(format!("Failed to parse source: {e}")),
    };
    let methods = match extract_dialect_methods(&file) {
        Ok(m) => m,
        Err(e) => return e.to_compile_error().into(),
    };

    // Validate overrides
    let bool_names: HashSet<_> = methods
        .iter()
        .filter(|m| is_bool_method(&m.signature))
        .map(|m| m.name.to_string())
        .collect();
    for (key, value) in &input.overrides {
        let key_str = key.to_string();
        let err = |msg| Error::new(key.span(), msg).to_compile_error().into();
        match value {
            Override::Bool(_) if !bool_names.contains(&key_str) => {
                return err(format!("Unknown boolean method `{key_str}`"));
            }
            Override::Char(_) | Override::None if key_str != "identifier_quote_style" => {
                return err(format!(
                    "Char/None only valid for `identifier_quote_style`, not `{key_str}`"
                ));
            }
            _ => {}
        }
    }
    generate_derived_dialect(&input, &methods).into()
}

/// Generate the complete derived `Dialect` implementation
fn generate_derived_dialect(input: &DeriveDialectInput, methods: &[DialectMethod]) -> TokenStream {
    let name = &input.name;
    let base = &input.base;

    // Helper to find an override by method name
    let find_override = |method_name: &str| {
        input
            .overrides
            .iter()
            .find(|(k, _)| k == method_name)
            .map(|(_, v)| v)
    };

    // Helper to generate delegation to base dialect
    let delegate = |method: &DialectMethod| {
        let sig = &method.signature;
        let method_name = &method.name;
        let params = extract_param_names(sig);
        quote_spanned! { method_name.span() => #sig { self.dialect.#method_name(#(#params),*) } }
    };

    // Generate the struct
    let struct_def = quote_spanned! { name.span() =>
        #[derive(Debug, Default)]
        pub struct #name {
            dialect: #base,
        }
        impl #name {
            pub fn new() -> Self { Self::default() }
        }
    };

    // Generate TypeId method body
    let type_id_body = if input.preserve_type_id {
        quote! { Dialect::dialect(&self.dialect) }
    } else {
        quote! { ::core::any::TypeId::of::<#name>() }
    };

    // Generate method implementations
    let method_impls = methods.iter().map(|method| {
        let method_name = &method.name;
        match find_override(&method_name.to_string()) {
            Some(Override::Bool(value)) => {
                quote_spanned! { method_name.span() => fn #method_name(&self) -> bool { #value } }
            }
            Some(Override::Char(c)) => {
                quote_spanned! { method_name.span() =>
                    fn identifier_quote_style(&self, _: &str) -> Option<char> { Some(#c) }
                }
            }
            Some(Override::None) => {
                quote_spanned! { method_name.span() =>
                    fn identifier_quote_style(&self, _: &str) -> Option<char> { None }
                }
            }
            None => delegate(method),
        }
    });

    // Wrap impl in a const block with scoped imports so types resolve without qualification
    quote! {
        #struct_def
        const _: () = {
            use ::core::iter::Peekable;
            use ::core::str::Chars;
            use sqlparser::ast::{ColumnOption, Expr, GranteesType, Ident, ObjectNamePart, Statement};
            use sqlparser::dialect::{Dialect, Precedence};
            use sqlparser::keywords::Keyword;
            use sqlparser::parser::{Parser, ParserError};

            impl Dialect for #name {
                fn dialect(&self) -> ::core::any::TypeId { #type_id_body }
                #(#method_impls)*
            }
        };
    }
}

/// Extract parameter names from a method signature (excluding self)
fn extract_param_names(sig: &Signature) -> Vec<&Ident> {
    sig.inputs
        .iter()
        .filter_map(|arg| match arg {
            FnArg::Typed(pt) => match pt.pat.as_ref() {
                Pat::Ident(pi) => Some(&pi.ident),
                _ => None,
            },
            _ => None,
        })
        .collect()
}

/// Read the `dialect/mod.rs` file that contains the Dialect trait.
fn read_dialect_mod_file() -> Result<String, String> {
    let manifest_dir =
        std::env::var("CARGO_MANIFEST_DIR").map_err(|_| "CARGO_MANIFEST_DIR not set")?;
    let path = std::path::Path::new(&manifest_dir).join("src/dialect/mod.rs");
    std::fs::read_to_string(&path).map_err(|e| format!("Failed to read {}: {e}", path.display()))
}

/// Extract all methods from the `Dialect` trait (excluding `dialect` for TypeId)
fn extract_dialect_methods(file: &File) -> Result<Vec<DialectMethod>, Error> {
    let dialect_trait = file
        .items
        .iter()
        .find_map(|item| match item {
            Item::Trait(t) if t.ident == "Dialect" => Some(t),
            _ => None,
        })
        .ok_or_else(|| Error::new(proc_macro2::Span::call_site(), "Dialect trait not found"))?;

    let mut methods: Vec<_> = dialect_trait
        .items
        .iter()
        .filter_map(|item| match item {
            TraitItem::Fn(m) if m.sig.ident != "dialect" => Some(DialectMethod {
                name: m.sig.ident.clone(),
                signature: m.sig.clone(),
            }),
            _ => None,
        })
        .collect();
    methods.sort_by_key(|m| m.name.to_string());
    Ok(methods)
}

/// Check if a method signature is `fn name(&self) -> bool`
fn is_bool_method(sig: &Signature) -> bool {
    sig.inputs.len() == 1
        && matches!(
            sig.inputs.first(),
            Some(FnArg::Receiver(r)) if r.reference.is_some() && r.mutability.is_none()
        )
        && matches!(
            &sig.output,
            ReturnType::Type(_, ty) if matches!(ty.as_ref(), Type::Path(p) if p.path.is_ident("bool"))
        )
}
