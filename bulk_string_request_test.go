// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package elastic

import (
	"testing"
)

func TestBulkStringRequestSerialization(t *testing.T) {
	tests := []struct {
		Request  BulkableRequest
		Expected []string
	}{
		// #0
		{
			Request: NewBulkStringRequest().Index("index101").Type("employee").Id("1").
				Doc(employee{User: "olivere"}),
			Expected: []string{
				`{"index":{"_id":"1","_index":"index101","_type":"employee"}}`,
				`{"user":"olivere","city":"","age":0}`,
			},
		},
		// #1
		{
			Request: NewBulkStringRequest().OpType("create").Index("index101").Type("employee").Id("1").
				Doc(employee{User: "olivere", City: "santafe", Age: 56}),
			Expected: []string{
				`{"create":{"_id":"1","_index":"index101","_type":"employee"}}`,
				`{"user":"olivere","city":"santafe","age":56}`,
			},
		},
		// #2
		{
			Request: NewBulkStringRequest().OpType("index").Index("index101").Type("employee").Id("1").
				Doc(employee{User: "olivere"}),
			Expected: []string{
				`{"index":{"_id":"1","_index":"index101","_type":"employee"}}`,
				`{"user":"olivere","city":"","age":0}`,
			},
		},
		// #3
		{
			Request: NewBulkStringRequest().OpType("index").Index("index101").Type("employee").Id("1").RetryOnConflict(42).
				Doc(employee{User: "olivere"}),
			Expected: []string{
				`{"index":{"_id":"1","_index":"index101","_retry_on_conflict":42,"_type":"employee"}}`,
				`{"user":"olivere","city":"","age":0}`,
			},
		},
		// #4
		{
			Request: NewBulkStringRequest().OpType("index").Index("index101").Type("employee").Id("1").Pipeline("my_pipeline").
				Doc(employee{User: "olivere"}),
			Expected: []string{
				`{"index":{"_id":"1","_index":"index101","_type":"employee","pipeline":"my_pipeline"}}`,
				`{"user":"olivere","city":"","age":0}`,
			},
		},
		// #5
		{
			Request: NewBulkStringRequest().OpType("index").Index("index101").Type("employee").Id("1").TTL("1m").
				Doc(employee{User: "olivere"}),
			Expected: []string{
				`{"index":{"_id":"1","_index":"index101","_ttl":"1m","_type":"employee"}}`,
				`{"user":"olivere","city":"","age":0}`,
			},
		},
	}

	for i, test := range tests {
		lines, err := test.Request.Source()
		if err != nil {
			t.Fatalf("case #%d: expected no error, got: %v", i, err)
		}
		if lines == nil {
			t.Fatalf("case #%d: expected lines, got nil", i)
		}
		if len(lines) != len(test.Expected) {
			t.Fatalf("case #%d: expected %d lines, got %d", i, len(test.Expected), len(lines))
		}
		for j, line := range lines {
			if line != test.Expected[j] {
				t.Errorf("case #%d: expected line #%d to be %s, got: %s", i, j, test.Expected[j], line)
			}
		}
	}
}
