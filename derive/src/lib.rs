use proc_macro2::TokenStream;
use quote::{format_ident, quote, quote_spanned, ToTokens};
use syn::spanned::Spanned;
use syn::{
    parse_macro_input, parse_quote, Attribute, Data, DeriveInput, Fields, GenericParam, Generics,
    Ident, Index, Lit, Meta, MetaNameValue, NestedMeta,
};

#[proc_macro_derive(Visit, attributes(visit))]
pub fn derive_visit(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    // Parse the input tokens into a syntax tree.
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;

    let attributes = Attributes::parse(&input.attrs);
    // Add a bound `T: HeapSize` to every type parameter T.
    let generics = add_trait_bounds(input.generics);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let with = attributes.with.map(|m| quote!(visitor.#m(self)?;));
    let children = visit_children(&input.data);

    let expanded = quote! {
        // The generated impl.
        impl #impl_generics sqlparser::ast::Visit for #name #ty_generics #where_clause {
            fn visit<V: sqlparser::ast::Visitor>(&self, visitor: &mut V) -> ::std::ops::ControlFlow<V::Break> {
                #with
                #children
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

impl Attributes {
    fn parse(attrs: &[Attribute]) -> Self {
        let mut out = Self::default();
        for attr in attrs.iter().filter(|a| a.path.is_ident("visit")) {
            let meta = attr.parse_meta().expect("visit attribute");
            match meta {
                Meta::List(l) => {
                    for nested in &l.nested {
                        match nested {
                            NestedMeta::Meta(Meta::NameValue(v)) => out.parse_name_value(v),
                            _ => panic!("Expected #[visit(key = \"value\")]"),
                        }
                    }
                }
                _ => panic!("Expected #[visit(...)]"),
            }
        }
        out
    }

    /// Updates self with a name value attribute
    fn parse_name_value(&mut self, v: &MetaNameValue) {
        if v.path.is_ident("with") {
            match &v.lit {
                Lit::Str(s) => self.with = Some(format_ident!("{}", s.value(), span = s.span())),
                _ => panic!("Expected a string value, got {}", v.lit.to_token_stream()),
            }
            return;
        }
        panic!("Unrecognised kv attribute {}", v.path.to_token_stream())
    }
}

// Add a bound `T: Visit` to every type parameter T.
fn add_trait_bounds(mut generics: Generics) -> Generics {
    for param in &mut generics.params {
        if let GenericParam::Type(ref mut type_param) = *param {
            type_param.bounds.push(parse_quote!(sqlparser::ast::Visit));
        }
    }
    generics
}

// Generate the body of the visit implementation for the given type
fn visit_children(data: &Data) -> TokenStream {
    match data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let name = &f.ident;
                    let attributes = Attributes::parse(&f.attrs);
                    let with = attributes.with.map(|m| quote!(visitor.#m(&self.#name)?;));
                    quote_spanned!(f.span() => #with sqlparser::ast::Visit::visit(&self.#name, visitor)?;)
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let index = Index::from(i);
                    let attributes = Attributes::parse(&f.attrs);
                    let with = attributes.with.map(|m| quote!(visitor.#m(&self.#index)?;));
                    quote_spanned!(f.span() => #with sqlparser::ast::Visit::visit(&self.#index, visitor)?;)
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
                            let with = attributes.with.map(|m| quote!(visitor.#m(&#name)?;));
                            quote_spanned!(f.span() => #with sqlparser::ast::Visit::visit(#name, visitor)?)
                        });

                        quote!(
                            Self::#name { #(#names),* } => {
                                #(#visit);*
                            }
                        )
                    }
                    Fields::Unnamed(fields) => {
                        let names = fields.unnamed.iter().enumerate().map(|(i, f)| format_ident!("_{}", i, span = f.span()));
                        let visit = fields.unnamed.iter().enumerate().map(|(i, f)| {
                            let name = format_ident!("_{}", i);
                            let attributes = Attributes::parse(&f.attrs);
                            let with = attributes.with.map(|m| quote!(visitor.#m(&#name)?;));
                            quote_spanned!(f.span() => #with sqlparser::ast::Visit::visit(#name, visitor)?)
                        });

                        quote! {
                            Self::#name ( #(#names),*) => {
                                #(#visit);*
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
