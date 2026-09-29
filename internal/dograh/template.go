package dograh

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata" // {{current_time_<Zone>}} must work on hosts without a zoneinfo database
	"unicode"
)

// templateVar is Dograh's TEMPLATE_VAR_PATTERN (api/services/workflow/
// workflow.py): {{path}}, {{path | default}}, {{path | fallback:default}}.
var templateVar = regexp.MustCompile(`\{\{\s*([^|\s}]+)(?:\s*\|\s*([^:}]+)(?::([^}]+))?)?\s*\}\}`)

// builtinTZ finds the zone of a {{current_time_<Zone>}} or
// {{current_weekday_<Zone>}}, which a bare {{current_weekday}} or
// {{current_time}} in the same template then inherits.
var builtinTZ = regexp.MustCompile(`\{\{\s*(?:current_time|current_weekday)_([^|\s}]+)`)

// renderTemplate fills in {{variables}} the way Dograh's render_template
// does (api/utils/template_renderer.py): built-ins current_time[_<Zone>] and
// current_weekday[_<Zone>] first, then dotted paths into vars, then the
// "| default" fallback; missing values become "". Literal "\n" sequences
// become newlines.
func renderTemplate(s string, vars map[string]any, now time.Time) string {
	if s == "" {
		return s
	}

	defaultTZ := ""
	if m := builtinTZ.FindStringSubmatch(s); m != nil {
		defaultTZ = strings.TrimSpace(m[1])
	}

	out := templateVar.ReplaceAllStringFunc(s, func(match string) string {
		m := templateVar.FindStringSubmatch(match)
		path := strings.TrimSpace(m[1])

		if v, ok := builtinValue(path, defaultTZ, now); ok {
			return v
		}

		value := lookupPath(vars, path)

		if m[2] != "" && (value == nil || value == "") {
			filter := strings.TrimSpace(m[2])
			if filter == "fallback" {
				if fb := strings.TrimSpace(m[3]); m[3] != "" {
					value = fb
				} else {
					value = pyTitle(path)
				}
			} else {
				value = filter
			}
		}

		return formatValue(value)
	})

	return strings.ReplaceAll(out, `\n`, "\n")
}

// builtinValue resolves Dograh's built-in date/time variables. An unknown
// zone isn't a built-in (Dograh logs it and falls through to vars).
func builtinValue(path, defaultTZ string, now time.Time) (string, bool) {
	const timeLayout = "2006-01-02 15:04:05 MST" // Python's "%Y-%m-%d %H:%M:%S %Z"

	in := func(zone string) (time.Time, bool) {
		if zone == "" {
			zone = "UTC"
		}

		loc, err := time.LoadLocation(zone)
		if err != nil {
			return time.Time{}, false
		}

		return now.In(loc), true
	}

	var (
		t    time.Time
		ok   bool
		wday bool
	)

	switch {
	case path == "current_time":
		t, ok = in(defaultTZ)
	case strings.HasPrefix(path, "current_time_"):
		t, ok = in(strings.TrimPrefix(path, "current_time_"))
	case path == "current_weekday":
		t, ok = in(defaultTZ)
		wday = true
	case strings.HasPrefix(path, "current_weekday_"):
		t, ok = in(strings.TrimPrefix(path, "current_weekday_"))
		wday = true
	default:
		return "", false
	}

	if !ok {
		return "", false
	}

	if wday {
		return t.Weekday().String(), true
	}

	return t.Format(timeLayout), true
}

// lookupPath follows a dotted path ("customer.address.city") into vars.
func lookupPath(vars map[string]any, path string) any {
	var cur any = vars

	for _, key := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}

		if cur = m[key]; cur == nil {
			return nil
		}
	}

	return cur
}

// formatValue matches Python's str() for JSON-decoded values.
func formatValue(v any) string {
	switch v := v.(type) {
	case nil:
		return ""
	case string:
		return v
	case bool:
		if v {
			return "True"
		}

		return "False"
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	default: // objects and arrays: JSON, as Dograh does
		b, _ := json.Marshal(v)
		return string(b)
	}
}

// pyTitle is Python's str.title(): upper-case each letter that follows a
// non-letter, lower-case the rest ("caller_name" -> "Caller_Name").
func pyTitle(s string) string {
	var b strings.Builder

	prevLetter := false

	for _, r := range s {
		if unicode.IsLetter(r) {
			if prevLetter {
				r = unicode.ToLower(r)
			} else {
				r = unicode.ToUpper(r)
			}

			prevLetter = true
		} else {
			prevLetter = false
		}

		b.WriteRune(r)
	}

	return b.String()
}
