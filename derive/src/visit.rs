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

//! Implementation of the `Visit` and `VisitMut` derive macros.

use proc_macro2::TokenStream;
use quote::{format_ident, quote, quote_spanned, ToTokens};
use syn::spanned::Spanned;
use syn::{
    parse::{Parse, ParseStream},
    parse_quote, Attribute, Data, Fields, GenericParam, Generics, Ident, Index, LitStr, Meta,
    Token, Type, TypePath,
};
use syn::{Path, PathArguments};

pub(crate) struct VisitType {
    pub visit_trait: TokenStream,
    pub visitor_trait: TokenStream,
    pub modifier: Option<TokenStream>,
}

pub(crate) fn derive_visit(
    input: syn::DeriveInput,
    visit_type: &VisitType,
) -> proc_macro::TokenStream {
    let name = input.ident;

    let VisitType {
        visit_trait,
        visitor_trait,
        modifier,
    } = visit_type;

    let attributes = Attributes::parse(&input.attrs);
    // Add a bound `T: Visit` to every type parameter T.
    let generics = add_trait_bounds(input.generics, visit_type);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let (pre_visit, post_visit) = attributes.visit(quote!(self));
    let children = visit_children(&input.data, visit_type);

    let expanded = quote! {
        // The generated impl.
        // Note that it uses [`recursive::recursive`] to protect from stack overflow.
        // See tests in https://github.com/apache/datafusion-sqlparser-rs/pull/1522/ for more info.
        impl #impl_generics sqlparser::ast::#visit_trait for #name #ty_generics #where_clause {
             #[cfg_attr(feature = "recursive-protection", recursive::recursive)]
            fn visit<V: sqlparser::ast::#visitor_trait>(
                &#modifier self,
                visitor: &mut V
            ) -> ::std::ops::ControlFlow<V::Break> {
                #pre_visit
                #children
                #post_visit
                ::std::ops::ControlFlow::Continue(())
            }
        }
    };

    proc_macro::TokenStream::from(expanded)
}

/// Parses attributes that can be provided to this macro
///
/// `#[visit(leaf, with = "visit_expr")]`
#[derive(Default)]
struct Attributes {
    /// Content for the `with` attribute
    with: Option<Ident>,
}

struct WithIdent {
    with: Option<Ident>,
}
impl Parse for WithIdent {
    fn parse(input: ParseStream) -> Result<Self, syn::Error> {
        let mut result = WithIdent { with: None };
        let ident = input.parse::<Ident>()?;
        if ident != "with" {
            return Err(syn::Error::new(
                ident.span(),
                "Expected identifier to be `with`",
            ));
        }
        input.parse::<Token!(=)>()?;
        let s = input.parse::<LitStr>()?;
        result.with = Some(format_ident!("{}", s.value(), span = s.span()));
        Ok(result)
    }
}

impl Attributes {
    fn parse(attrs: &[Attribute]) -> Self {
        let mut out = Self::default();
        for attr in attrs {
            if let Meta::List(ref metalist) = attr.meta {
                if metalist.path.is_ident("visit") {
                    match syn::parse2::<WithIdent>(metalist.tokens.clone()) {
                        Ok(with_ident) => {
                            out.with = with_ident.with;
                        }
                        Err(e) => {
                            panic!("{}", e);
                        }
                    }
                }
            }
        }
        out
    }

    /// Returns the pre and post visit token streams
    fn visit(&self, s: TokenStream) -> (Option<TokenStream>, Option<TokenStream>) {
        let pre_visit = self.with.as_ref().map(|m| {
            let m = format_ident!("pre_{}", m);
            quote!(visitor.#m(#s)?;)
        });
        let post_visit = self.with.as_ref().map(|m| {
            let m = format_ident!("post_{}", m);
            quote!(visitor.#m(#s)?;)
        });
        (pre_visit, post_visit)
    }
}

// Add a bound `T: Visit` to every type parameter T.
fn add_trait_bounds(mut generics: Generics, VisitType { visit_trait, .. }: &VisitType) -> Generics {
    for param in &mut generics.params {
        if let GenericParam::Type(ref mut type_param) = *param {
            type_param
                .bounds
                .push(parse_quote!(sqlparser::ast::#visit_trait));
        }
    }
    generics
}

// Generate the body of the visit implementation for the given type
fn visit_children(
    data: &Data,
    VisitType {
        visit_trait,
        modifier,
        ..
    }: &VisitType,
) -> TokenStream {
    match data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let name = &f.ident;
                    let is_option = is_option(&f.ty);
                    let attributes = Attributes::parse(&f.attrs);
                    if is_option && attributes.with.is_some() {
                        let (pre_visit, post_visit) = attributes.visit(quote!(value));
                        quote_spanned!(f.span() =>
                            if let Some(value) = &#modifier self.#name {
                                #pre_visit sqlparser::ast::#visit_trait::visit(value, visitor)?; #post_visit
                            }
                        )
                    } else {
                        let (pre_visit, post_visit) = attributes.visit(quote!(&#modifier self.#name));
                        quote_spanned!(f.span() =>
                            #pre_visit sqlparser::ast::#visit_trait::visit(&#modifier self.#name, visitor)?; #post_visit
                        )
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let index = Index::from(i);
                    let attributes = Attributes::parse(&f.attrs);
                    let (pre_visit, post_visit) = attributes.visit(quote!(&self.#index));
                    quote_spanned!(f.span() => #pre_visit sqlparser::ast::#visit_trait::visit(&#modifier self.#index, visitor)?; #post_visit)
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unit => {
                quote!()
            }
        },
        Data::Enum(data) => {
            let statements = data.variants.iter().map(|v| {
                let name = &v.ident;
                match &v.fields {
                    Fields::Named(fields) => {
                        let names = fields.named.iter().map(|f| &f.ident);
                        let visit = fields.named.iter().map(|f| {
                            let name = &f.ident;
                            let attributes = Attributes::parse(&f.attrs);
                            let (pre_visit, post_visit) = attributes.visit(name.to_token_stream());
                            quote_spanned!(f.span() => #pre_visit sqlparser::ast::#visit_trait::visit(#name, visitor)?; #post_visit)
                        });

                        quote!(
                            Self::#name { #(#names),* } => {
                                #(#visit)*
                            }
                        )
                    }
                    Fields::Unnamed(fields) => {
                        let names = fields.unnamed.iter().enumerate().map(|(i, f)| format_ident!("_{}", i, span = f.span()));
                        let visit = fields.unnamed.iter().enumerate().map(|(i, f)| {
                            let name = format_ident!("_{}", i);
                            let attributes = Attributes::parse(&f.attrs);
                            let (pre_visit, post_visit) = attributes.visit(name.to_token_stream());
                            quote_spanned!(f.span() => #pre_visit sqlparser::ast::#visit_trait::visit(#name, visitor)?; #post_visit)
                        });

                        quote! {
                            Self::#name ( #(#names),*) => {
                                #(#visit)*
                            }
                        }
                    }
                    Fields::Unit => {
                        quote! {
                            Self::#name => {}
                        }
                    }
                }
            });

            quote! {
                match self {
                    #(#statements),*
                }
            }
        }
        Data::Union(_) => unimplemented!(),
    }
}

fn is_option(ty: &Type) -> bool {
    if let Type::Path(TypePath {
        path: Path { segments, .. },
        ..
    }) = ty
    {
        if let Some(segment) = segments.last() {
            if segment.ident == "Option" {
                if let PathArguments::AngleBracketed(args) = &segment.arguments {
                    return args.args.len() == 1;
                }
            }
        }
    }
    false
}
