package iop

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
)

const jkey = "json-test-secret"

func jsonTF(t *testing.T, args ...string) TransformLegacy {
	t.Helper()
	withKey(t, jkey)
	tf, err := newHmacJSONTransform(args)
	if err != nil {
		t.Fatal(err)
	}
	return tf
}

func expectHmac(val string) string {
	mac := hmac.New(sha256.New, []byte(jkey))
	mac.Write([]byte(val))
	return hex.EncodeToString(mac.Sum(nil))[:32]
}

// The column transform now shares the pooled HMAC. It must produce EXACTLY what
// the previously deployed naive implementation did, or every table already
// synced under the old binary stops joining to rows synced under the new one.
func TestHmacPooledMatchesNaive(t *testing.T) {
	withKey(t, "test-secret-key")
	key := []byte("test-secret-key")

	for _, v := range []string{"", "a", "someone@example.com", "9876543210",
		strings.Repeat("x", 1000), "ünïcödé", "16d113840f999444259f73bac9ab8b10"} {
		naiveMac := hmac.New(sha256.New, key)
		naiveMac.Write([]byte(v))
		naive := hex.EncodeToString(naiveMac.Sum(nil))[:32]

		if got := hmacHex32(key, []byte(v)); got != naive {
			t.Errorf("pooled(%q) = %q, naive = %q", v, got, naive)
		}
		// and through the registered transform
		viaTF, err := TransformsLegacyMap["hmac_sha256"].FuncString(nil, v)
		if err != nil {
			t.Fatal(err)
		}
		if viaTF != naive {
			t.Errorf("hmac_sha256(%q) = %q, naive = %q", v, viaTF, naive)
		}
	}
}

func TestHmacJSON_RewritesDeclaredKeys(t *testing.T) {
	tf := jsonTF(t, "lead_email", "lead_phone_number")

	md5Email := "16d113840f999444259f73bac9ab8b10"
	md5Phone := "e388c1c5df4933fa01f6da9f92595589"
	in := fmt.Sprintf(`{"lead_email":"%s","lead_phone_number":"%s","lead_name":"%s","source":"web"}`,
		md5Email, md5Phone, md5Email)

	got, err := tf.FuncString(nil, in)
	if err != nil {
		t.Fatal(err)
	}

	want := fmt.Sprintf(`{"lead_email":"%s","lead_phone_number":"%s","lead_name":"%s","source":"web"}`,
		expectHmac(md5Email), expectHmac(md5Phone), md5Email)
	if got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
	// lead_name was NOT declared, so it must survive as raw md5
	if !strings.Contains(got, `"lead_name":"`+md5Email+`"`) {
		t.Error("undeclared key lead_name was modified")
	}
}

// telephony's X-CALLERNO lives inside a nested sip_headers object.
func TestHmacJSON_NestedKey(t *testing.T) {
	tf := jsonTF(t, "CallerID", "X-CALLERNO")
	m := "e388c1c5df4933fa01f6da9f92595589"
	in := fmt.Sprintf(`{"CallerID":"%s","sip_headers":{"X-CALLERNO":"%s","X-UUI":"abc"},"n":1}`, m, m)

	got, err := tf.FuncString(nil, in)
	if err != nil {
		t.Fatal(err)
	}
	h := expectHmac(m)
	want := fmt.Sprintf(`{"CallerID":"%s","sip_headers":{"X-CALLERNO":"%s","X-UUI":"abc"},"n":1}`, h, h)
	if got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}

// The source really emits "customer_no_with_prefix " with a trailing space
// alongside the unspaced spelling. One declared key must match both.
func TestHmacJSON_KeyWhitespaceTolerant(t *testing.T) {
	tf := jsonTF(t, "customer_no_with_prefix")
	m := "e388c1c5df4933fa01f6da9f92595589"
	in := fmt.Sprintf(`{"customer_no_with_prefix ":"%s","customer_no_with_prefix":"%s"}`, m, m)

	got, err := tf.FuncString(nil, in)
	if err != nil {
		t.Fatal(err)
	}
	h := expectHmac(m)
	want := fmt.Sprintf(`{"customer_no_with_prefix ":"%s","customer_no_with_prefix":"%s"}`, h, h)
	if got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}

// Only 32-char lowercase hex is rewritten. Anything else means the source SQL
// did not hash that occurrence, and re-keying plaintext would corrupt data.
func TestHmacJSON_OnlyTouchesHex32(t *testing.T) {
	tf := jsonTF(t, "phone", "email", "id")
	in := `{"phone":"+91-9876543210","email":"asha@gmail.com","id":"NOT-HEX","x":"e388c1c5df4933fa01f6da9f9259558","y":"E388C1C5DF4933FA01F6DA9F92595589"}`

	got, err := tf.FuncString(nil, in)
	if err != nil {
		t.Fatal(err)
	}
	if got != in {
		t.Errorf("non-hex32 values were modified:\n got %s\nwant %s", got, in)
	}
}

// A string VALUE that happens to equal a declared key name must not be treated
// as a key. This is what a naive search-and-replace would get wrong.
func TestHmacJSON_ValueLookingLikeKeyUntouched(t *testing.T) {
	tf := jsonTF(t, "phone")
	m := "e388c1c5df4933fa01f6da9f92595589"
	in := fmt.Sprintf(`{"note":"phone","other":"%s","phone":"%s"}`, m, m)

	got, err := tf.FuncString(nil, in)
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf(`{"note":"phone","other":"%s","phone":"%s"}`, m, expectHmac(m))
	if got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}

// The whole reason for byte-level replacement: a parse/re-marshal round trip
// would reorder keys and destroy numeric fidelity. Nothing outside the 32-byte
// windows may change.
func TestHmacJSON_PreservesStructureAndNumbers(t *testing.T) {
	tf := jsonTF(t, "phone")
	m := "e388c1c5df4933fa01f6da9f92595589"
	in := fmt.Sprintf(`{"zeta":1,"alpha":2,"big_id":9007199254740993,"money":1.0,"exp":1e3,"neg":-0.5,"phone":"%s","esc":"a\"b\\c","uni":"Zoë Đặng","nul":null,"arr":[1,2,{"k":"v"}],"t":true}`, m)

	got, err := tf.FuncString(nil, in)
	if err != nil {
		t.Fatal(err)
	}

	want := strings.Replace(in, `"phone":"`+m+`"`, `"phone":"`+expectHmac(m)+`"`, 1)
	if got != want {
		t.Errorf("structure changed:\n got %s\nwant %s", got, want)
	}
	if len(got) != len(in) {
		t.Errorf("length changed: %d -> %d (replacement must be length-preserving)", len(in), len(got))
	}
	// sanity: the large integer survived verbatim (a float64 round trip would break it)
	if !strings.Contains(got, "9007199254740993") {
		t.Error("large integer lost precision")
	}
	if !json.Valid([]byte(got)) {
		t.Error("output is not valid JSON")
	}
}

func TestHmacJSON_NoMatchReturnsInputUnchanged(t *testing.T) {
	tf := jsonTF(t, "phone")
	in := `{"a":1,"b":"hello","c":{"d":[1,2,3]}}`
	got, err := tf.FuncString(nil, in)
	if err != nil {
		t.Fatal(err)
	}
	if got != in {
		t.Errorf("got %s, want %s", got, in)
	}
}

func TestHmacJSON_EdgeCases(t *testing.T) {
	tf := jsonTF(t, "phone")
	m := "e388c1c5df4933fa01f6da9f92595589"
	for _, in := range []string{
		``, `{}`, `[]`, `null`, `{"phone":null}`, `{"phone":123}`,
		`{"phone":`, `{"unterminated":"abc`, `not json at all`,
		`{"phone" : "` + m + `"}`, // whitespace around colon
	} {
		got, err := tf.FuncString(nil, in)
		if err != nil {
			t.Fatalf("input %q errored: %v", in, err)
		}
		_ = got // must not panic; content asserted below for the spaced case
	}
	// the spaced-colon form must still be rewritten
	got, _ := tf.FuncString(nil, `{"phone" : "`+m+`"}`)
	if !strings.Contains(got, expectHmac(m)) {
		t.Errorf("whitespace around colon prevented rewrite: %s", got)
	}
}

// Fails closed exactly like the column transform.
func TestHmacJSON_FailsClosedWithoutKey(t *testing.T) {
	tf, err := newHmacJSONTransform([]string{"phone"})
	if err != nil {
		t.Fatal(err)
	}
	resetHmacSecret()
	t.Setenv(HmacSecretEnvVar, "")
	t.Cleanup(resetHmacSecret)

	if _, err := tf.FuncString(nil, `{"phone":"e388c1c5df4933fa01f6da9f92595589"}`); err == nil {
		t.Fatal("expected an error with no key set")
	}
}

func TestParseTransformArgs(t *testing.T) {
	cases := []struct {
		in   string
		name string
		args []string
	}{
		{"hash_md5", "hash_md5", nil},
		{"hmac_sha256_json(a)", "hmac_sha256_json", []string{"a"}},
		{"hmac_sha256_json(a,b,c)", "hmac_sha256_json", []string{"a", "b", "c"}},
		{"hmac_sha256_json( a , b )", "hmac_sha256_json", []string{"a", "b"}},
		{"hmac_sha256_json(X-CALLERNO,sip)", "hmac_sha256_json", []string{"X-CALLERNO", "sip"}},
	}
	for _, c := range cases {
		n, a := parseTransformArgs(c.in)
		if n != c.name || fmt.Sprint(a) != fmt.Sprint(c.args) {
			t.Errorf("parseTransformArgs(%q) = (%q,%v), want (%q,%v)", c.in, n, a, c.name, c.args)
		}
	}
}

// End to end through the real config path.
func TestHmacJSON_ThroughNewTransform(t *testing.T) {
	withKey(t, jkey)
	m := "e388c1c5df4933fa01f6da9f92595589"

	stages, err := ParseStageTransforms([]any{
		map[string]any{"raw_payload": "hmac_sha256_json(lead_phone_number)"},
	})
	if err != nil {
		t.Fatal(err)
	}
	sp := NewStreamProcessor()
	sp.ds = NewDatastream(NewColumns(Column{Name: "raw_payload", Type: StringType}))
	tf := NewTransform(stages, sp)
	if tf == nil {
		t.Fatal("NewTransform returned nil for a parameterized transform")
	}

	row, err := tf.Evaluate([]any{`{"lead_phone_number":"` + m + `"}`})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"lead_phone_number":"` + expectHmac(m) + `"}`
	if row[0].(string) != want {
		t.Errorf("got %v, want %s", row[0], want)
	}
}

func TestHmacJSON_Concurrent(t *testing.T) {
	tf := jsonTF(t, "phone")
	m := "e388c1c5df4933fa01f6da9f92595589"
	in := `{"phone":"` + m + `"}`
	want := `{"phone":"` + expectHmac(m) + `"}`

	var wg sync.WaitGroup
	errs := make(chan string, 64)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 300; j++ {
				got, err := tf.FuncString(nil, in)
				if err != nil {
					errs <- err.Error()
					return
				}
				if got != want {
					errs <- "got " + got
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
}

// ---- benchmarks: syncs must not get slower ----

const realPayload = `{"call_id":"CAGE6-T2-1782331289.139170","caller_id_number":"e388c1c5df4933fa01f6da9f92595589","call_to_number":"15eeb0a2f05e21b05efbc635a9ec68b1","answered_agent_number":"e73f2c5f0d33a3f5c3408c94f7c76c5a","answered_agent_name":"fdcae9466c269905bcf585ab087a1aff","digits_dialed":"0188f7d73a2522934b0d4931e838199c","recording_url":"4803c249ee4fb38d6598d76248df0a00","billsec":142,"call_status":"answered","direction":"inbound","hangup_cause":"NORMAL_CLEARING","start_stamp":"2026-08-01 10:22:11","end_stamp":"2026-08-01 10:24:33","campaign_name":"dsc_masterclass_1","billing_circle":{"circle":"Delhi","operator":"Airtel"},"uuid":"9f8e7d6c5b4a39281706f5e4d3c2b1a0","ref_id":"XYZ-9912"}`

func BenchmarkHmacJSON_SixKeysMatch(b *testing.B) {
	b.Setenv(HmacSecretEnvVar, "bench-key")
	resetHmacSecret()
	tf, _ := newHmacJSONTransform([]string{
		"caller_id_number", "call_to_number", "answered_agent_number",
		"answered_agent_name", "digits_dialed", "recording_url"})
	b.ReportAllocs()
	b.SetBytes(int64(len(realPayload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := tf.FuncString(nil, realPayload); err != nil {
			b.Fatal(err)
		}
	}
}

// The common incremental case: payload has none of the declared keys. Must be
// allocation-free so untouched streams pay nothing.
func BenchmarkHmacJSON_NoMatch(b *testing.B) {
	b.Setenv(HmacSecretEnvVar, "bench-key")
	resetHmacSecret()
	tf, _ := newHmacJSONTransform([]string{"nonexistent_key_a", "nonexistent_key_b"})
	b.ReportAllocs()
	b.SetBytes(int64(len(realPayload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := tf.FuncString(nil, realPayload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHmacSha256_Column(b *testing.B) {
	b.Setenv(HmacSecretEnvVar, "bench-key")
	resetHmacSecret()
	tf := TransformsLegacyMap["hmac_sha256"]
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := tf.FuncString(nil, "16d113840f999444259f73bac9ab8b10"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHashMd5_Column_Baseline(b *testing.B) {
	tf := TransformsLegacyMap["hash_md5"]
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := tf.FuncString(nil, "someone@example.com"); err != nil {
			b.Fatal(err)
		}
	}
}

// What we are NOT doing, for comparison: parse into a map and re-marshal.
func BenchmarkJSONRoundTrip_Rejected(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(int64(len(realPayload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var m map[string]any
		if err := json.Unmarshal([]byte(realPayload), &m); err != nil {
			b.Fatal(err)
		}
		if _, err := json.Marshal(m); err != nil {
			b.Fatal(err)
		}
	}
}
