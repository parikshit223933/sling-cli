package iop

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"sync"
	"testing"

	"github.com/flarco/g"
	"github.com/spf13/cast"
)

// resetHmacSecret clears the memoised key so a test can choose its own.
// Production never does this — the key is read once per process.
func resetHmacSecret() {
	hmacSecretOnce = sync.Once{}
	hmacSecretVal = nil
}

func withKey(t *testing.T, key string) {
	t.Helper()
	resetHmacSecret()
	t.Setenv(HmacSecretEnvVar, key)
	t.Cleanup(resetHmacSecret)
}

// expected computes the reference value independently of the implementation.
func expected(key, val string) string {
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write([]byte(val))
	return hex.EncodeToString(mac.Sum(nil))[:32]
}

func TestHmacSha256_RegisteredAndMatchesReference(t *testing.T) {
	withKey(t, "test-secret-key")

	tf, ok := TransformsLegacyMap["hmac_sha256"]
	if !ok {
		t.Fatal("hmac_sha256 not registered in TransformsLegacyMap")
	}

	for _, val := range []string{
		"someone@example.com",
		"9876543210",
		g.MD5("someone@example.com"),
		"",
		"ünïcödé ✓",
		strings.Repeat("x", 5000),
	} {
		got, err := tf.FuncString(nil, val)
		if err != nil {
			t.Fatalf("FuncString(%q) errored: %v", val, err)
		}
		if want := expected("test-secret-key", val); got != want {
			t.Errorf("FuncString(%q) = %q, want %q", val, got, want)
		}
		if len(got) != 32 {
			t.Errorf("FuncString(%q) length = %d, want 32 (md5-compatible width)", val, len(got))
		}
	}
}

// Deterministic across calls — otherwise joins on the hashed column break.
func TestHmacSha256_Deterministic(t *testing.T) {
	withKey(t, "test-secret-key")
	tf := TransformsLegacyMap["hmac_sha256"]

	first, _ := tf.FuncString(nil, "9876543210")
	for i := 0; i < 100; i++ {
		again, err := tf.FuncString(nil, "9876543210")
		if err != nil {
			t.Fatal(err)
		}
		if again != first {
			t.Fatalf("call %d returned %q, first call returned %q", i, again, first)
		}
	}
}

// A different key must produce a different value, or rotating the key would
// silently be a no-op.
func TestHmacSha256_KeyChangesOutput(t *testing.T) {
	withKey(t, "key-one")
	a, err := TransformsLegacyMap["hmac_sha256"].FuncString(nil, "9876543210")
	if err != nil {
		t.Fatal(err)
	}

	withKey(t, "key-two")
	b, err := TransformsLegacyMap["hmac_sha256"].FuncString(nil, "9876543210")
	if err != nil {
		t.Fatal(err)
	}

	if a == b {
		t.Fatal("different keys produced the same output; the key is not being used")
	}
}

// The load-bearing safety property: with no key set, the transform must ERROR.
// Returning the input, an empty string, or an unkeyed hash would silently
// repopulate the warehouse with reversible values while the run reports success.
func TestHmacSha256_FailsClosedWithoutKey(t *testing.T) {
	resetHmacSecret()
	t.Setenv(HmacSecretEnvVar, "")
	t.Cleanup(resetHmacSecret)

	got, err := TransformsLegacyMap["hmac_sha256"].FuncString(nil, "someone@example.com")
	if err == nil {
		t.Fatalf("expected an error with no key set, got value %q", got)
	}
	if got != "" {
		t.Errorf("expected empty value alongside the error, got %q", got)
	}
	if !strings.Contains(err.Error(), HmacSecretEnvVar) {
		t.Errorf("error should name %s so the operator knows what to set; got: %v", HmacSecretEnvVar, err)
	}
}

// Chaining hash_md5 -> hmac_sha256 must equal HMAC(K, md5(v)). This is the
// composition the replication configs will actually use.
func TestHmacSha256_ChainedOntoMd5(t *testing.T) {
	withKey(t, "test-secret-key")

	const raw = "someone@example.com"
	md5Val, err := TransformsLegacyMap["hash_md5"].FuncString(nil, raw)
	if err != nil {
		t.Fatal(err)
	}
	chained, err := TransformsLegacyMap["hmac_sha256"].FuncString(nil, md5Val)
	if err != nil {
		t.Fatal(err)
	}

	if want := expected("test-secret-key", g.MD5(raw)); chained != want {
		t.Errorf("chained = %q, want HMAC(K, md5(v)) = %q", chained, want)
	}
}

// The equality graph of the underlying md5 must survive the keying step:
// inputs that collided before still collide, inputs that differed still differ.
// This is what guarantees existing dashboard joins keep working after the
// migration — values change, join results do not.
func TestHmacSha256_PreservesEqualityGraph(t *testing.T) {
	withKey(t, "test-secret-key")

	// Same raw value reached via different rows; and case variants, which
	// md5 already treats as distinct (no normalisation is applied anywhere).
	inputs := []string{
		"someone@example.com",
		"someone@example.com",
		"SOMEONE@example.com",
		"9876543210",
		"09876543210",
	}

	md5s := make([]string, len(inputs))
	keyed := make([]string, len(inputs))
	for i, in := range inputs {
		md5s[i], _ = TransformsLegacyMap["hash_md5"].FuncString(nil, in)
		keyed[i], _ = TransformsLegacyMap["hmac_sha256"].FuncString(nil, md5s[i])
	}

	for i := range inputs {
		for j := range inputs {
			md5Equal := md5s[i] == md5s[j]
			keyedEqual := keyed[i] == keyed[j]
			if md5Equal != keyedEqual {
				t.Errorf("equality graph broken for %q vs %q: md5Equal=%v keyedEqual=%v",
					inputs[i], inputs[j], md5Equal, keyedEqual)
			}
		}
	}
}

// Chaining must survive the real config path: a stage list naming the same
// column twice has to apply BOTH stages, in order, through Transform.Evaluate.
// Testing the transform funcs directly misses this — the stage list is built by
// ParseStageTransforms/NewTransform, and a bug there silently drops stages.
func TestChainedStagesAppliedInOrder(t *testing.T) {
	withKey(t, "test-secret-key")

	stages, err := ParseStageTransforms([]any{
		map[string]any{"email": "hash_md5"},
		map[string]any{"email": "hmac_sha256"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(stages) != 2 {
		t.Fatalf("ParseStageTransforms dropped stages: got %d, want 2 (%v)", len(stages), stages)
	}

	sp := NewStreamProcessor()
	sp.ds = NewDatastream(NewColumns(Column{Name: "email", Type: StringType}))

	tf := NewTransform(stages, sp)
	if tf == nil {
		t.Fatal("NewTransform returned nil")
	}

	row, err := tf.Evaluate([]any{"someone@example.com"})
	if err != nil {
		t.Fatal(err)
	}

	want := expected("test-secret-key", g.MD5("someone@example.com"))
	if got := cast.ToString(row[0]); got != want {
		t.Errorf("chained Evaluate = %q, want HMAC(K, md5(v)) = %q", got, want)
	}
}

// The same shape sling builds internally for database sources: transforms
// arrive as map[string][]string. Per-column order must be preserved, or the
// hmac stage could be applied before the md5 it is meant to wrap.
func TestParseStageTransformsPreservesPerColumnOrder(t *testing.T) {
	stages, err := ParseStageTransforms(map[string][]string{
		"email": {"trim_space", "lower", "hash_md5", "hmac_sha256"},
	})
	if err != nil {
		t.Fatal(err)
	}

	var got []string
	for _, s := range stages {
		got = append(got, s["email"])
	}
	want := []string{"trim_space", "lower", "hash_md5", "hmac_sha256"}
	if len(got) != len(want) {
		t.Fatalf("got %d stages %v, want %d %v", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("stage order = %v, want %v", got, want)
		}
	}
}

// Adding the transform must not perturb any existing one. If hash_md5 changed,
// every already-synced table would silently stop joining to freshly synced rows.
func TestExistingTransformsUnchanged(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{"hash_md5", "someone@example.com", "16d113840f999444259f73bac9ab8b10"},
		{"hash_md5", "", "d41d8cd98f00b204e9800998ecf8427e"},
		{"lower", "SOMEONE@Example.COM", "someone@example.com"},
		{"trim_space", "  padded  ", "padded"},
	}
	for _, c := range cases {
		tf, ok := TransformsLegacyMap[c.name]
		if !ok {
			t.Fatalf("%s missing from TransformsLegacyMap", c.name)
		}
		got, err := tf.FuncString(nil, c.in)
		if err != nil {
			t.Fatalf("%s(%q) errored: %v", c.name, c.in, err)
		}
		if got != c.want {
			t.Errorf("%s(%q) = %q, want %q", c.name, c.in, got, c.want)
		}
	}
}

// Safe under the concurrent stream processing sling does.
func TestHmacSha256_ConcurrentUse(t *testing.T) {
	withKey(t, "test-secret-key")
	tf := TransformsLegacyMap["hmac_sha256"]
	want := expected("test-secret-key", "9876543210")

	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				got, err := tf.FuncString(nil, "9876543210")
				if err != nil {
					errs <- err
					return
				}
				if got != want {
					errs <- g.Error("got %q, want %q", got, want)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}
