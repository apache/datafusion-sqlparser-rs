// Getting this out of the source code is currently a manual process:
// 1) Run `egrep -hv "//!" src/ast/*.rs > mods`
//         this is required, because astexplorer's parser doesn't like the `//!` parent doc-comments
// 2) Paste the results into https://astexplorer.net/ (select the Rust mode)
// 3) Copy the AST data back to `const json =`
// 4) Copy the source code to `srcText`,  escaping backslashes (`\` -> `\\`) and backticks ` -> \`
// 5) Run:    node util/ast-stats.js > util/ast-fields.tsv

const {srcText, json} = require("./real-data.js");
// const {srcText, json} = require("./test-data.js");

/** Walk the JSON object representing the AST depth-first, calling `visitor` on each "complex" node. */
function walk(ast, path, visitor) {
    ast.path = path;

    if (!visitor(ast)) return;
    const nodeName = (ast instanceof Array ? "Array" : ast["_type"]);
    // console.log(`@${path}: ${nodeName} ${ast.toString().substring(1,20)}`)
    for (var prop in ast) {
        if (ast[prop] instanceof Object) {
            walk(ast[prop], (path ? path + "/" : "") + nodeName, visitor);
        }
    }
}

/** Return all AST objects matching the predicate in an array.
    Note: for nested objects only the top-most one is included in the results */
function find(ast, filterFn) {
    let rv = [];
    walk(ast, "", node => {
        if (filterFn(node)) {
            rv.push(node);
            return false;
        }
        return true;
    });
    return rv;
}

/** Return string in the given span of the text.
 * A span is an `{start: LC, end: LC}` object, where
 * LC is {line, column} sub-objects with 1-based positions, inclusive.
 */
function src(text, span) {
    const lineLengths = text.split("\n").map(line => line.length);
    function pos2idx(pos) {
        let idx = 0;
        for (let i = 0; i < pos.line-1; ++i) {
            idx += lineLengths[i] + 1;
        }
        idx += pos.column;
        //console.log(`pos2idx(${pos.line}, ${pos.column}) = ${idx}`)
        return idx;
    }
    return text.substring(pos2idx(span.start), pos2idx(span.end))
}

// let spans = find(json, (node) => node._type == "Field").map(node => node.span);
// console.log(src(srcText, spans[0]));

/** A predicate to filter declarations preceded by a set of #[derive]s used for AST structs/enums */
function filterAST(structOrEnum) {
    const attrs = structOrEnum.attrs.map(attr => src(srcText, attr.span));
    const isAST = attrs.some(a => a == "#[derive(Debug, Clone, PartialEq, Eq, Hash)]");
    //console.log(structOrEnum.ident.to_string, isAST, isAST ? "" : attrs)
    return isAST;
}


const structs = find(json, n => n._type == "ItemStruct").filter(filterAST);
const enums = find(json, n => n._type == "ItemEnum").filter(filterAST);

const enumVariants = enums.flatMap(enum_ => {
    const enum_name = enum_.ident.to_string;
    return find(enum_, n => n._type == "Variant").map(variant => ({
        struct_name: `${enum_name}::${variant.ident.to_string}`,
        enum_name: enum_name,
        variant_name: variant.ident.to_string,
        node: variant
    }));
});

const structsAndEnumVariants = structs.map(struct => ({
    struct_name: struct.ident.to_string,
    enum_name: "", variant_name: "",
    node: struct
})).concat(enumVariants);

let fields = structsAndEnumVariants.flatMap(item => {
    return find(item, (n) => n._type == "Field").map(field => ({
        struct_name: item.struct_name,
        enum_name: item.enum_name, variant_name: item.variant_name,
        field_name: field.ident ? field.ident.to_string : "unnamed",  // e.g. (Bar, Baz)
        field_type: src(srcText, field.ty.span)
    }));
});

const FIELDS = ["struct_name", "enum_name", "variant_name", "field_name", "field_type"];
console.log(FIELDS.join("\t"));
console.log(fields.map(row => FIELDS.map(col => row[col]).join("\t")).join("\n"));
// console.log(fields);

//console.log(src(srcText, {start:{line:1135, column:12}, end: {line:1135, column:17}}));
