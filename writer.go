//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package main

import (
	"fmt"
	"io"
	"strings"
)

type Writer interface {
	Write(values []string)
	Flush()
}

// baseWriter is a writer to write table records in bulk.
//
// NOTE: baseWriter is not goroutine-safe.
type baseWriter struct {
	out   io.Writer
	table *Table
}

type InsertWriter struct {
	baseWriter
	buffer   [][]string
	bulkSize uint
}

type UpdateWriter struct {
	baseWriter
	columns []string
}

// NewInsertWriter creates InsertWrite with specified configs.
func NewInsertWriter(table *Table, out io.Writer, bulkSize uint) Writer {
	return &InsertWriter{
		baseWriter: baseWriter{
			out:   out,
			table: table,
		},

		buffer:   make([][]string, 0, bulkSize),
		bulkSize: bulkSize,
	}
}

// Flush flushes the buffered records.
func (w *baseWriter) Flush() {
}

// Write writes a single record into the buffer. If buffer becomes full, it is flushed.
func (w *InsertWriter) Write(values []string) {
	w.buffer = append(w.buffer, values)
	if len(w.buffer) >= int(w.bulkSize) {
		w.Flush()
	}
}

// Flush flushes the buffered records.
func (w *InsertWriter) Flush() {
	if len(w.buffer) == 0 {
		return
	}
	w.flushInInsert()
}

func (w *baseWriter) quote(column string) string {
	return fmt.Sprintf("`%s`", column)
}

// Flush flushes the buffered records in insert statement
func (w *InsertWriter) flushInInsert() {
	if len(w.buffer) == 0 {
		return
	}

	quotedColumns := w.table.quotedColumnList()

	// Calculate the size of buffer for strings.Builder
	n := len(w.buffer) * 2 // 2 is for value separator (", ")
	n += len(quotedColumns)
	n += 100 // 100 is for remained statement ("INSERT INTO ...")
	for i := 0; i < len(w.buffer); i++ {
		n += len(w.buffer[i])
	}

	// Use strings.Builder to avoid string being copied to build INSERT statement
	sb := &strings.Builder{}
	sb.Grow(n)
	sb.WriteString("INSERT INTO `")
	sb.WriteString(w.table.Name)
	sb.WriteString("` (")
	sb.WriteString(quotedColumns)
	sb.WriteString(") VALUES ")
	for i, b := range w.buffer {
		sb.WriteString(fmt.Sprintf("(%s)", strings.Join(b, ", ")))
		if i < (len(w.buffer) - 1) {
			sb.WriteString(", ")
		}
	}
	sb.WriteString(";\n")

	fmt.Fprint(w.out, sb.String())
	w.buffer = w.buffer[:0]
}

func (w *baseWriter) findColumnValue(values []string, primaryKey string) string {
	for i, column := range w.table.Columns {
		if column == primaryKey {
			return values[i]
		}
	}
	panic("not primary key value found")
}

// NewUpdateWriter creates InsertWrite with specified configs.
func NewUpdateWriter(table *Table, out io.Writer, columns []string) Writer {
	return &UpdateWriter{
		baseWriter: baseWriter{
			out:   out,
			table: table,
		},
		columns: columns,
	}
}

func (w *UpdateWriter) Columns() []string {
	columns := w.columns
	if len(columns) == 0 {
		columns = w.table.Columns
	}
	return columns
}

func (w *UpdateWriter) Write(values []string) {

	// Use strings.Builder to avoid string being copied to build INSERT statement
	sb := &strings.Builder{}
	sb.WriteString("UPDATE `")
	sb.WriteString(w.table.Name)
	sb.WriteString("` SET ")

	columns := w.Columns()

	primaryKeyValue := w.findColumnValue(values, w.table.PrimaryKey)

	for i, c := range columns {
		value := w.findColumnValue(values, c)
		if i == 0 {
			sb.WriteString(fmt.Sprintf("%s = %s", w.quote(c), value))
		} else {
			sb.WriteString(fmt.Sprintf(" , %s = %s", w.quote(c), value))
		}
	}

	sb.WriteString(" WHERE ")
	sb.WriteString(fmt.Sprintf("%s = %s", w.table.PrimaryKey, primaryKeyValue))

	sb.WriteString(";\n")

	fmt.Fprint(w.out, sb.String())
}
func (w *UpdateWriter) quotedColumnList() []string {
	columns := w.columns
	if len(columns) == 0 {
		columns = w.table.Columns
	}
	quoted := make([]string, 0, len(columns))
	for _, column := range columns {
		quoted = append(quoted, fmt.Sprintf("`%s`", column))
	}
	return quoted
}
