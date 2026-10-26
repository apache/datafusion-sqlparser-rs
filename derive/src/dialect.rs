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
    Error, Expr, FnArg, Ident, ItemTrait, LitBool, LitChar, Pat, ReceiverKind, ReturnType,
    Signature, Token, TraitItem, Type,
};

/// Override value types supported by the macro
pub(crate) enum Override {
    Bool(LitBool),
    Char(LitChar),
    Expr(Expr),
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
                            let value = match content.parse::<Expr>()? {
                                Expr::Lit(syn::ExprLit {
                                    lit: syn::Lit::Bool(value),
                                    ..
                                }) => Override::Bool(value),
                                Expr::Lit(syn::ExprLit {
                                    lit: syn::Lit::Char(value),
                                    ..
                                }) => Override::Char(value),
                                expr => Override::Expr(expr),
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

/// Pass the trait's signatures to each derived dialect without reading source files.
pub(crate) fn dialect_methods(item: ItemTrait) -> TokenStream {
    let signatures = item.items.iter().filter_map(|item| match item {
        TraitItem::Fn(method) => Some(&method.sig),
        _ => None,
    });
    quote! {
        #item

        #[doc(hidden)]
        #[macro_export]
        macro_rules! __sqlparser_derive_dialect {
            ($($args:tt)*) => {
                $crate::dialect::__derive_dialect! {
                    trait Dialect { #(#signatures;)* }
                    $($args)*
                }
            };
        }

        #[doc(hidden)]
        pub use __sqlparser_derive_dialect as __derive_dialect_with_methods;
    }
}

pub(crate) struct DeriveDialectWithMethods {
    dialect: ItemTrait,
    input: DeriveDialectInput,
}

impl Parse for DeriveDialectWithMethods {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        Ok(Self {
            dialect: input.parse()?,
            input: input.parse()?,
        })
    }
}

/// Entry point after the trait has supplied its method signatures.
pub(crate) fn derive_dialect(input: DeriveDialectWithMethods) -> proc_macro::TokenStream {
    match derive_dialect_inner(input) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn derive_dialect_inner(input: DeriveDialectWithMethods) -> syn::Result<TokenStream> {
    let methods = extract_dialect_methods(&input.dialect, input.input.name.span());
    let input = input.input;

    // Validate overrides
    let method_names: HashSet<_> = methods.iter().map(|m| m.name.to_string()).collect();
    let bool_names: HashSet<_> = methods
        .iter()
        .filter(|m| is_bool_method(&m.signature))
        .map(|m| m.name.to_string())
        .collect();
    for (key, value) in &input.overrides {
        let key_str = key.to_string();
        if !method_names.contains(&key_str) {
            return Err(Error::new(
                key.span(),
                format!("Unknown method `{key_str}`"),
            ));
        }
        match value {
            Override::Bool(_) if !bool_names.contains(&key_str) => {
                return Err(Error::new(
                    key.span(),
                    format!("Unknown boolean method `{key_str}`"),
                ));
            }
            Override::Char(_) if key_str != "identifier_quote_style" => {
                return Err(Error::new(
                    key.span(),
                    format!("Char only valid for `identifier_quote_style`, not `{key_str}`"),
                ));
            }
            _ => {}
        }
    }
    Ok(generate_derived_dialect(&input, &methods))
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
            Some(Override::Expr(expr)) => {
                let sig = &method.signature;
                quote_spanned! { method_name.span() => #sig { #expr } }
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
            use sqlparser::ast::{ColumnOption, DataType, Expr, GranteesType, Ident, ObjectNamePart, Statement};
            use sqlparser::dialect::{BindPlaceholderStyle, Dialect, Precedence};
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

/// Extract all methods from the supplied trait (excluding `dialect` for TypeId).
fn extract_dialect_methods(
    dialect_trait: &ItemTrait,
    call_site: proc_macro2::Span,
) -> Vec<DialectMethod> {
    let mut methods: Vec<_> = dialect_trait
        .items
        .iter()
        .filter_map(|item| match item {
            TraitItem::Fn(m) if m.sig.ident != "dialect" => {
                let mut signature = m.sig.clone();
                // Override expressions refer to parameters at the invocation site.
                for arg in &mut signature.inputs {
                    if let FnArg::Typed(arg) = arg {
                        if let Pat::Ident(pat) = arg.pat.as_mut() {
                            pat.ident.set_span(call_site);
                        }
                    }
                }
                Some(DialectMethod {
                    name: m.sig.ident.clone(),
                    signature,
                })
            }
            _ => None,
        })
        .collect();
    methods.sort_by_key(|m| m.name.to_string());
    methods
}

/// Check if a method signature is `fn name(&self) -> bool`
fn is_bool_method(sig: &Signature) -> bool {
    sig.inputs.len() == 1
        && matches!(
            sig.inputs.first(),
            Some(FnArg::Receiver(r)) if matches!(&r.kind, ReceiverKind::Reference(_, _, None))
        )
        && matches!(
            &sig.output,
            ReturnType::Type(_, ty) if matches!(ty.as_ref(), Type::Path(p) if p.path.is_ident("bool"))
        )
}
