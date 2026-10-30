package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"unicode"
)

const defaultBaseline = "tools/commentlint/baseline.txt"

var generatedMarker = regexp.MustCompile(`^// Code generated .* DO NOT EDIT\.$`)

// violation 是一条注释违规。key 由路径和符号组成，不含行号，避免无关改动让基线失效。
type violation struct {
	path   string
	line   int
	symbol string
	msg    string
}

func (v violation) String() string {
	if v.line == 0 {
		return fmt.Sprintf("%s: %s %s", v.path, v.symbol, v.msg)
	}
	return fmt.Sprintf("%s:%d: %s %s", v.path, v.line, v.symbol, v.msg)
}

func (v violation) key() string {
	return v.path + "\t" + v.symbol
}

// packageDoc 汇总一个目录内非测试、非生成文件的包注释情况。
type packageDoc struct {
	name      string
	hasDoc    bool
	hasHanDoc bool
}

func main() {
	root := flag.String("root", ".", "要检查的仓库根目录")
	baselinePath := flag.String("baseline", defaultBaseline, "基线文件，相对 root")
	write := flag.Bool("write-baseline", false, "用当前违规重写基线")
	flag.Parse()

	violations, err := scan(*root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "commentlint: 扫描失败: %v\n", err)
		os.Exit(1)
	}
	path := filepath.Join(*root, *baselinePath)
	if *write {
		if writeErr := writeBaseline(path, violations); writeErr != nil {
			fmt.Fprintf(os.Stderr, "commentlint: 写基线失败: %v\n", writeErr)
			os.Exit(1)
		}
		fmt.Printf("commentlint: 基线已写入 %d 条\n", len(violations))
		return
	}
	baseline, err := readBaseline(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "commentlint: 读基线失败: %v\n", err)
		os.Exit(1)
	}
	problems := compare(violations, baseline)
	if len(problems) == 0 {
		fmt.Printf("commentlint: ok (基线 %d 条)\n", len(baseline))
		return
	}
	fmt.Fprintf(os.Stderr, "commentlint: %d 个问题\n", len(problems))
	for _, problem := range problems {
		fmt.Fprintln(os.Stderr, "  "+problem)
	}
	os.Exit(1)
}

// compare 返回不在基线里的新违规，以及基线里已经修复、应当删除的条目。
func compare(violations []violation, baseline map[string]bool) []string {
	current := make(map[string]bool, len(violations))
	var problems []string
	for _, v := range violations {
		current[v.key()] = true
		if !baseline[v.key()] {
			problems = append(problems, v.String())
		}
	}
	for key := range baseline {
		if !current[key] {
			path, symbol, _ := strings.Cut(key, "\t")
			problems = append(problems, fmt.Sprintf("%s: %s 已符合要求，请从 %s 删除该条", path, symbol, defaultBaseline))
		}
	}
	sort.Strings(problems)
	return problems
}

// scan 遍历 root 下的 Go 源文件，返回按路径排序的全部违规。
func scan(root string) ([]violation, error) {
	packages := make(map[string]*packageDoc)
	var violations []violation
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if path != root && shouldSkipDir(info.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		generated, err := isGenerated(path)
		if err != nil || generated {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		fileViolations, err := scanFile(path, filepath.ToSlash(rel), packages)
		violations = append(violations, fileViolations...)
		return err
	})
	if err != nil {
		return nil, err
	}
	for dir, pkg := range packages {
		rel, err := filepath.Rel(root, dir)
		if err != nil {
			return nil, err
		}
		v := violation{path: filepath.ToSlash(rel), symbol: "package " + pkg.name}
		switch {
		case !pkg.hasDoc:
			v.msg = "缺少包注释"
		case !pkg.hasHanDoc:
			v.msg = "的包注释必须包含中文"
		default:
			continue
		}
		violations = append(violations, v)
	}
	sort.Slice(violations, func(i, j int) bool {
		if violations[i].path != violations[j].path {
			return violations[i].path < violations[j].path
		}
		if violations[i].line != violations[j].line {
			return violations[i].line < violations[j].line
		}
		return violations[i].symbol < violations[j].symbol
	})
	return violations, nil
}

func scanFile(path, rel string, packages map[string]*packageDoc) ([]violation, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
	if err != nil {
		return nil, err
	}
	dir := filepath.Dir(path)
	pkg := packages[dir]
	if pkg == nil {
		pkg = &packageDoc{name: file.Name.Name}
		packages[dir] = pkg
	}
	if file.Doc != nil {
		pkg.hasDoc = true
		pkg.hasHanDoc = pkg.hasHanDoc || hasHan(file.Doc.Text())
	}
	// main 包的导出符号不是对外 API，只要求包注释。
	if file.Name.Name == "main" {
		return nil, nil
	}

	var violations []violation
	report := func(pos token.Pos, symbol string, doc *ast.CommentGroup) {
		v := violation{path: rel, line: fset.Position(pos).Line, symbol: symbol}
		switch {
		case doc == nil:
			v.msg = "缺少文档注释"
		case !hasHan(doc.Text()):
			v.msg = "的文档注释必须包含中文"
		default:
			return
		}
		violations = append(violations, v)
	}
	for _, decl := range file.Decls {
		switch decl := decl.(type) {
		case *ast.FuncDecl:
			if symbol, ok := funcSymbol(decl); ok {
				report(decl.Pos(), symbol, decl.Doc)
			}
		case *ast.GenDecl:
			for _, spec := range decl.Specs {
				symbol, doc := specSymbol(spec)
				if symbol == "" {
					continue
				}
				if doc == nil {
					doc = decl.Doc
				}
				report(spec.Pos(), symbol, doc)
			}
		}
	}
	return violations, nil
}

// funcSymbol 返回导出函数名，或导出类型上的导出方法名 "T.M"。
func funcSymbol(decl *ast.FuncDecl) (string, bool) {
	if !decl.Name.IsExported() {
		return "", false
	}
	if decl.Recv == nil || len(decl.Recv.List) == 0 {
		return decl.Name.Name, true
	}
	receiver := receiverName(decl.Recv.List[0].Type)
	if !ast.IsExported(receiver) {
		return "", false
	}
	return receiver + "." + decl.Name.Name, true
}

// specSymbol 返回 type/const/var 规格中的导出名和它自己的注释；没有导出名时返回空串。
func specSymbol(spec ast.Spec) (string, *ast.CommentGroup) {
	switch spec := spec.(type) {
	case *ast.TypeSpec:
		if spec.Name.IsExported() {
			return spec.Name.Name, spec.Doc
		}
	case *ast.ValueSpec:
		var names []string
		for _, name := range spec.Names {
			if name.IsExported() {
				names = append(names, name.Name)
			}
		}
		if len(names) > 0 {
			return strings.Join(names, ","), spec.Doc
		}
	}
	return "", nil
}

func receiverName(expr ast.Expr) string {
	for {
		switch current := expr.(type) {
		case *ast.StarExpr:
			expr = current.X
		case *ast.ParenExpr:
			expr = current.X
		case *ast.IndexExpr:
			expr = current.X
		case *ast.IndexListExpr:
			expr = current.X
		case *ast.Ident:
			return current.Name
		default:
			return ""
		}
	}
}

func hasHan(text string) bool {
	for _, current := range text {
		if unicode.Is(unicode.Han, current) {
			return true
		}
	}
	return false
}

func shouldSkipDir(name string) bool {
	return name == "vendor" || name == "testdata" || name == "node_modules" ||
		strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_")
}

// isGenerated 按 Go 约定识别生成文件：package 子句之前出现生成标记。
func isGenerated(path string) (bool, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return false, err
	}
	for _, line := range strings.Split(string(content), "\n") {
		line = strings.TrimSuffix(line, "\r")
		if generatedMarker.MatchString(line) {
			return true, nil
		}
		if strings.HasPrefix(line, "package ") {
			return false, nil
		}
	}
	return false, nil
}

func readBaseline(path string) (map[string]bool, error) {
	content, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return map[string]bool{}, nil
	}
	if err != nil {
		return nil, err
	}
	return parseBaseline(bytes.NewReader(content))
}

func parseBaseline(r io.Reader) (map[string]bool, error) {
	baseline := make(map[string]bool)
	scanner := bufio.NewScanner(r)
	for lineNo := 1; scanner.Scan(); lineNo++ {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.Contains(line, "\t") {
			return nil, fmt.Errorf("第 %d 行应为 \"路径<TAB>符号\": %q", lineNo, line)
		}
		baseline[line] = true
	}
	return baseline, scanner.Err()
}

func writeBaseline(path string, violations []violation) error {
	var b strings.Builder
	b.WriteString("# commentlint 存量豁免：路径<TAB>符号。只减不增，新代码不得登记。\n")
	b.WriteString("# 补完注释后运行 go run ./tools/commentlint -write-baseline 重写本文件。\n")
	seen := make(map[string]bool)
	for _, v := range violations {
		if seen[v.key()] {
			continue
		}
		seen[v.key()] = true
		b.WriteString(v.key() + "\n")
	}
	return os.WriteFile(path, []byte(b.String()), 0o600)
}
