package spannerdiff

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

var update = flag.Bool("update", false, "update the files in testdata")

// ignoredPresencePositions are the positions that memefish uses to record whether a keyword or parentheses are present,
// but don't need to be compared.
var ignoredPresencePositions = map[string]string{
	"ArrayLiteral.Array":                  "ARRAY of an array literal is optional",
	"AsAlias.As":                          "AS of an alias is optional",
	"GQLElementPatternFiller.Label.Colon": "the same position as GQLLabelFilter.Colon",
	"GQLFilter.Filter":                    "a filter with and without FILTER are in different fields",
	"GQLFilter.Where":                     "WHERE after FILTER is optional",
	"GQLLabelFilter.Colon":                "IS and : of a label filter are the same",
	"Insert.Or":                           "INSERT is not DDL",
	"IsDestinationExpr.Of":                "OF of IS DESTINATION OF is optional",
	"IsSourceExpr.Of":                     "OF of IS SOURCE OF is optional",
	"LambdaArg.Lparen":                    "the parentheses of a single lambda argument are optional",
	"TypelessStructLiteral.Struct":        "STRUCT of a struct literal is optional",
	"UsagePrivilegeOnSchema.Default":      "grants on the default schema and on named schemas are different definitions",
}

// TestPresencePositions checks that every position whose validity changes the SQL printed by memefish
// is either compared by presencePositions or listed in ignoredPresencePositions.
// A position that isn't compared is ignored, so a change of the keyword recorded by it is not detected.
func TestPresencePositions(t *testing.T) {
	found := make(map[string]struct{})
	for _, file := range parseMemefishAST(t) {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || fn.Body == nil || len(fn.Recv.List[0].Names) == 0 {
				continue
			}
			receiver := fn.Recv.List[0].Names[0].Name
			receiverType := strings.TrimPrefix(types.ExprString(fn.Recv.List[0].Type), "*")
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "Invalid" {
					return true
				}
				if field, ok := strings.CutPrefix(types.ExprString(sel.X), receiver+"."); ok {
					found[receiverType+"."+field] = struct{}{}
				}
				return true
			})
		}
	}

	compared := make(map[string]struct{})
	for typ, fields := range presencePositions {
		for _, field := range fields {
			compared[typ.Name()+"."+field] = struct{}{}
		}
	}

	for _, pos := range slices.Sorted(maps.Keys(found)) {
		_, isCompared := compared[pos]
		_, isIgnored := ignoredPresencePositions[pos]
		switch {
		case isCompared && isIgnored:
			t.Errorf("%s is in both presencePositions and ignoredPresencePositions", pos)
		case !isCompared && !isIgnored:
			t.Errorf("%s changes the SQL by its validity, add it to presencePositions or ignoredPresencePositions", pos)
		}
	}
	// A position that is no longer found may be used in another way that this test can't find.
	for _, pos := range slices.Sorted(maps.Keys(compared)) {
		if _, ok := found[pos]; !ok {
			t.Errorf("%s in presencePositions is not found in memefish", pos)
		}
	}
	for _, pos := range slices.Sorted(maps.Keys(ignoredPresencePositions)) {
		if _, ok := found[pos]; !ok {
			t.Errorf("%s in ignoredPresencePositions is not found in memefish", pos)
		}
	}
}

// TestMemefishAST checks that the fields of the AST nodes of memefish are the same as testdata/memefish_ast.txt.
// A new field of a node may need to be handled by alter of the definition, because a difference that alter doesn't handle
// recreates the definition. Run the test with -update after checking the new fields.
func TestMemefishAST(t *testing.T) {
	structs := make(map[string][]string)
	for _, file := range parseMemefishAST(t) {
		ast.Inspect(file, func(n ast.Node) bool {
			spec, ok := n.(*ast.TypeSpec)
			if !ok {
				return true
			}
			st, ok := spec.Type.(*ast.StructType)
			if !ok || !spec.Name.IsExported() {
				return false
			}
			fields := []string{}
			for _, f := range st.Fields.List {
				for _, name := range f.Names {
					fields = append(fields, fmt.Sprintf("\t%s %s\n", name.Name, types.ExprString(f.Type)))
				}
			}
			structs[spec.Name.Name] = fields
			return false
		})
	}
	var sb strings.Builder
	for _, name := range slices.Sorted(maps.Keys(structs)) {
		sb.WriteString(name + "\n")
		sb.WriteString(strings.Join(structs[name], ""))
	}
	got := sb.String()

	const golden = "testdata/memefish_ast.txt"
	if *update {
		if err := os.WriteFile(golden, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(string(want), got); diff != "" {
		t.Errorf("the AST of memefish changed, check that the new fields are handled and run the test with -update (-want +got):\n%s", diff)
	}
}

// parseMemefishAST parses the source files of the ast package of memefish in the module cache.
func parseMemefishAST(t *testing.T) []*ast.File {
	t.Helper()
	out, err := exec.Command("go", "list", "-f", "{{.Dir}}", "github.com/cloudspannerecosystem/memefish/ast").Output()
	if err != nil {
		t.Fatalf("failed to find the ast package of memefish: %v", err)
	}
	paths, err := filepath.Glob(filepath.Join(strings.TrimSpace(string(out)), "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	var files []*ast.File
	fset := token.NewFileSet()
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, file)
	}
	if len(files) == 0 {
		t.Fatal("no source file of memefish is found")
	}
	return files
}
