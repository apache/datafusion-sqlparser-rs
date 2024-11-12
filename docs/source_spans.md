
## Breaking Changes

These are the current breaking changes introduced by the source spans feature:

#### Added fields for spans (must be added to any existing pattern matches)
- `Ident` now stores a `Span`
- `Select`, `With`, `Cte`, `WildcardAdditionalOptions` now store a `TokenWithLocation` 

#### Misc.
- `TokenWithLocation` stores a full `Span`, rathern than just a source location. Users relying on `token.location` should use `token.location.start` instead.
## Source Span Contributing Guidelines

For contributing source spans improvement in addition to the general [contribution guidelines](../README.md#contributing), please make sure to pay attention to the following:


### Source Span Design Considerations

- `Ident` always have correct source spans
- Downstream breaking change impact is to be as minimal as possible
- To this end, use recursive merging of spans in favor of storing spans on all nodes
- Any metadata added to compute spans must not change semantics (Eq, Ord, Hash, etc.)

The primary reason for missing and inaccurate source spans at this time is missing spans of keyword tokens and values in many structures, either due to lack of time or because adding them would break downstream significantly.  

When considering adding support for source spans on a type, consider the impact to consumers of that type and whether your change would require a consumer to do non-trivial changes to their code.

f.e. of a trivial change
```rust
match node {  
  ast::Query { 
    field1,
    field2, 
    location: _,  // add a new line to ignored location
}
```

If adding source spans to a type would require a significant change like wrapping that type or similar, please open an issue to discuss. 

### AST Node Equality and Hashes

When adding tokens to AST nodes, make sure to wrap them in the [IgnoreField](https://docs.rs/sqlparser/latest/sqlparser/ast/helpers/struct.IgnoreField.html) helper to ensure that semantically equivalent AST nodes always compare as equal and hash to the same value. F.e. `select 5` and `SELECT 5` would compare as different `Select` nodes, if the select token was stored directly. f.e.

```rust
struct Select {
    select_token: IgnoreField<TokenWithLocation>, // only used for spans
    /// remaining fields
    field1,
    field2,
    ...
}
```