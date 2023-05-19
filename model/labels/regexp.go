// Copyright 2020 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package labels

import (
	"strings"

	"github.com/grafana/regexp"
	"github.com/grafana/regexp/syntax"
)

type FastRegexMatcher struct {
	re       *regexp.Regexp
	prefix   string
	prefixOp syntax.Op
	suffix   string
	suffixOp syntax.Op
	contains string

	singleOp syntax.Op
}

func NewFastRegexMatcher(v string) (*FastRegexMatcher, error) {
	re, err := regexp.Compile("^(?:" + v + ")$")
	if err != nil {
		return nil, err
	}

	parsed, err := syntax.Parse(v, syntax.Perl)
	if err != nil {
		return nil, err
	}

	m := &FastRegexMatcher{
		re: re,
	}

	if parsed.Op == syntax.OpConcat {
		m.optimizeConcatRegex(parsed)
	}

	m.optimizeStartRegex(parsed)

	return m, nil
}

func (m *FastRegexMatcher) MatchString(s string) bool {
	if m.singleOp == syntax.OpStar {
		return !strings.Contains(s, "\n")
	}

	if m.singleOp == syntax.OpPlus {
		return len(s) > 0 && !strings.Contains(s, "\n")
	}

	if m.prefix != "" {
		if !strings.HasPrefix(s, m.prefix) {
			return false
		}
		if m.suffixOp == syntax.OpStar {
			return !strings.Contains(s[len(m.prefix):], "\n")
		}

		if m.suffixOp == syntax.OpPlus {
			return len(s) > len(m.prefix) && !strings.Contains(s[len(m.prefix):], "\n")
		}
	}
	if m.suffix != "" {
		if !strings.HasSuffix(s, m.suffix) {
			return false
		}
		if m.prefixOp == syntax.OpStar {
			return !strings.Contains(s[0:len(m.suffix)], "\n")
		}

		if m.prefixOp == syntax.OpPlus {
			return len(s) > len(m.suffix) && !strings.Contains(s[0:len(m.suffix)], "\n")
		}
	}
	if m.contains != "" && !strings.Contains(s, m.contains) {
		return false
	}
	return m.re.MatchString(s)
}

func (m *FastRegexMatcher) GetRegexString() string {
	return m.re.String()
}

func (m *FastRegexMatcher) optimizeStartRegex(r *syntax.Regexp) {
	if r.Op == syntax.OpPlus || r.Op == syntax.OpStar {
		if r.Sub[0].Op == syntax.OpAnyCharNotNL {
			m.singleOp = r.Op
		}
	}
}

// optimizeConcatRegex returns literal prefix/suffix text that can be safely
// checked against the label value before running the regexp matcher.
func (m *FastRegexMatcher) optimizeConcatRegex(r *syntax.Regexp) {
	sub := r.Sub

	// We can safely remove begin and end text matchers respectively
	// at the beginning and end of the regexp.
	if len(sub) > 0 && sub[0].Op == syntax.OpBeginText {
		sub = sub[1:]
	}
	if len(sub) > 0 && sub[len(sub)-1].Op == syntax.OpEndText {
		sub = sub[:len(sub)-1]
	}

	if len(sub) == 0 {
		return
	}

	last := len(sub) - 1

	if len(sub) == 1 {
		m.optimizeStartRegex(sub[0])
	}

	// Given Prometheus regex matchers are always anchored to the begin/end
	// of the text, if the first/last operations are literals, we can safely
	// treat them as prefix/suffix.
	if sub[0].Op == syntax.OpLiteral && (sub[0].Flags&syntax.FoldCase) == 0 {
		m.prefix = string(sub[0].Rune)
		if len(sub) == 2 && (sub[last].Op == syntax.OpStar || sub[last].Op == syntax.OpPlus) && sub[last].Sub[0].Op == syntax.OpAnyCharNotNL {
			m.suffixOp = sub[last].Op
		}
	}
	if sub[last].Op == syntax.OpLiteral && (sub[last].Flags&syntax.FoldCase) == 0 {
		m.suffix = string(sub[last].Rune)
		if len(sub) == 2 && (sub[0].Op == syntax.OpStar || sub[0].Op == syntax.OpPlus) && sub[0].Sub[0].Op == syntax.OpAnyCharNotNL {
			m.prefixOp = sub[0].Op
		}
	}

	// If contains any literal which is not a prefix/suffix, we keep the
	// 1st one. We do not keep the whole list of literals to simplify the
	// fast path.
	for i := 1; i < len(sub)-1; i++ {
		if sub[i].Op == syntax.OpLiteral && (sub[i].Flags&syntax.FoldCase) == 0 {
			m.contains = string(sub[i].Rune)
			break
		}
	}

	return
}
