// Package main 实现 commentlint 命令，在 CI 中强制中文注释要求。
//
// 规则：每个 Go 包必须有包注释；非 main 包中导出的函数、方法（接收者类型也导出）、类型、
// 常量和变量必须有文档注释。两类注释都必须包含中文。const/var/type 分组可以用分组注释
// 覆盖组内全部声明。_test.go、带 "Code generated ... DO NOT EDIT." 标记的文件以及
// vendor、testdata 和以 "." 或 "_" 开头的目录不检查。
//
// 存量豁免记录在 tools/commentlint/baseline.txt，每行一个 "路径<TAB>符号"。基线只减不增：
// 不在基线里的违规会失败，基线里已修复的条目也会失败，提示把它删掉。补完注释后运行
// go run ./tools/commentlint -write-baseline 重写基线；不要用它给新代码登记豁免。
//
// 修改规则后运行 go test ./tools/commentlint 和 go run ./tools/commentlint。
package main
