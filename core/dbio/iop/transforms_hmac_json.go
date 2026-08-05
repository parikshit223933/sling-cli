package iop

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"strings"
	"sync"

	"github.com/flarco/g"
)

// Keyed re-hashing of PII that lives INSIDE a JSON column.
//
// Most PII is a column of its own, so the plain hmac_sha256 transform can key
// it. But a webhook payload arrives as one jsonb blob with the phone number as
// a key inside it, and the scrub for those runs as SQL on the SOURCE database:
//
//	request_body || jsonb_build_object('CallerID', md5(...))
//
// The key cannot go into that SQL — it would land in the source database's
// pg_stat_activity and slow-query log, both readable by the same analysts the
// hash is meant to protect against. So the value arrives here already md5'd,
// and this transform keys it in flight, inside the sling process.
//
// APPROACH: the md5 hex the SQL produced is 32 characters, and the HMAC output
// is truncated to 32 characters too. Replacement is therefore EXACTLY
// length-preserving, which means the JSON never has to be parsed or rebuilt —
// we scan for the declared keys and overwrite 32 bytes in place.
//
// That is not just a speed choice, it is a correctness one. Unmarshalling into
// map[string]any and re-marshalling would reorder every object (Go map
// iteration), reformat every number through float64 (silently destroying
// precision on large ids), and drop the distinction between 1 and 1.0. The
// payload would no longer be the payload. Byte-level replacement changes
// exactly the 32 bytes we intend and nothing else.

// keyedMac pairs a primed HMAC with the key it was primed for. hmac.New
// re-derives the padded key blocks on every call, which is pure waste when the
// key never changes; Reset() keeps that precomputed inner/outer state. The key
// is carried alongside so a pooled instance can never be reused under a
// different key — in production the key is fixed for the process lifetime, but
// silently hashing under a stale key would be an invisible data-corruption bug,
// and the bytes.Equal check costs ~10ns against a ~250ns hash.
type keyedMac struct {
	key []byte
	mac hash.Hash
}

// hmacPool is a pool rather than one shared instance because sling processes
// streams concurrently and hash.Hash is not reentrant.
var hmacPool = sync.Pool{}

// hmacHex32 returns the first 32 hex chars of HMAC-SHA256(key, val).
// Takes []byte to avoid the allocation a string->[]byte conversion would make
// on every single value.
func hmacHex32(key []byte, val []byte) string {
	var km *keyedMac
	if v := hmacPool.Get(); v != nil {
		km = v.(*keyedMac)
		if !bytes.Equal(km.key, key) {
			km = &keyedMac{key: key, mac: hmac.New(sha256.New, key)}
		} else {
			km.mac.Reset()
		}
	} else {
		km = &keyedMac{key: key, mac: hmac.New(sha256.New, key)}
	}

	km.mac.Write(val)
	var sum [sha256.Size]byte
	digest := km.mac.Sum(sum[:0])

	var out [hmacHexLen]byte
	hex.Encode(out[:], digest[:hmacHexLen/2])
	hmacPool.Put(km)
	return string(out[:])
}

// isHex32 reports whether s is exactly 32 lowercase hex characters — the shape
// of the md5 the source SQL emitted. Anything else is left untouched: it means
// the SQL did not scrub that occurrence, and re-keying a plaintext value would
// corrupt data rather than protect it.
func isHex32(s string) bool {
	if len(s) != 32 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func isJSONSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

// hmacJSONKeys rewrites, in place, the value of every declared key whose value
// is a 32-char lowercase hex string.
//
// It walks the document once, skipping over complete strings, so a value that
// merely looks like a key can never be mistaken for one: a string is treated as
// a key only when the next non-space byte is ':'. Keys are matched at ANY depth,
// which is required — telephony's X-CALLERNO sits inside a nested sip_headers
// object — and is safe because the 32-hex guard rejects anything the SQL did
// not already hash.
//
// Key comparison ignores surrounding whitespace in the JSON key itself. The
// source genuinely emits "customer_no_with_prefix " with a trailing space
// alongside the unspaced spelling, and both must match one declared key rather
// than forcing an invisible trailing space into the YAML config.
func hmacJSONKeys(s string, keys map[string]struct{}, key []byte) string {
	if len(s) == 0 || len(keys) == 0 {
		return s
	}

	var b []byte // allocated lazily, only if something actually matches
	src := s
	n := len(src)
	i := 0

	for i < n {
		if src[i] != '"' {
			i++
			continue
		}

		// scan the string starting at i
		ks := i + 1
		j := ks
		for j < n {
			if src[j] == '\\' {
				j += 2
				continue
			}
			if src[j] == '"' {
				break
			}
			j++
		}
		if j >= n {
			break // unterminated; leave the remainder alone
		}
		ke := j
		i = j + 1 // past the closing quote

		// a key must be followed by ':'
		p := i
		for p < n && isJSONSpace(src[p]) {
			p++
		}
		if p >= n || src[p] != ':' {
			continue // that was a value, not a key
		}
		p++

		// trim whitespace inside the key name before matching
		ts, te := ks, ke
		for ts < te && isJSONSpace(src[ts]) {
			ts++
		}
		for te > ts && isJSONSpace(src[te-1]) {
			te--
		}
		if _, want := keys[src[ts:te]]; !want {
			continue
		}

		// value must be a string of exactly 32 lowercase hex chars
		for p < n && isJSONSpace(src[p]) {
			p++
		}
		if p >= n || src[p] != '"' {
			continue
		}
		vs := p + 1
		ve := vs + hmacHexLen
		if ve >= n || src[ve] != '"' {
			continue
		}
		if !isHex32(src[vs:ve]) {
			continue
		}

		// Copy once, on the first match only. Rows with nothing to rewrite —
		// and in an incremental sync most payloads are small — cost zero allocs.
		if b == nil {
			b = []byte(src)
		}
		// b mirrors src and rewritten ranges never overlap, so b[vs:ve] still
		// holds the original md5 here. hmacHex32 fully computes before we copy.
		copy(b[vs:ve], hmacHex32(key, b[vs:ve]))
		i = ve + 1
	}

	if b == nil {
		return s // nothing matched; hand back the original with zero copies
	}
	return string(b)
}

// parseTransformArgs splits "name(a,b,c)" into "name" and ["a","b","c"].
// Returns the input unchanged and a nil slice when there is no argument list.
func parseTransformArgs(spec string) (name string, args []string) {
	spec = strings.TrimSpace(spec)
	open := strings.IndexByte(spec, '(')
	if open < 0 || !strings.HasSuffix(spec, ")") {
		return spec, nil
	}
	name = strings.TrimSpace(spec[:open])
	inner := spec[open+1 : len(spec)-1]
	for _, a := range strings.Split(inner, ",") {
		if a = strings.TrimSpace(a); a != "" {
			args = append(args, a)
		}
	}
	return name, args
}

// TransformsParameterized holds transforms configured as name(arg,...).
// Kept separate from TransformsLegacyMap so the plain-name lookup used by every
// existing transform is untouched.
var TransformsParameterized = map[string]func(args []string) (TransformLegacy, error){
	"hmac_sha256_json": newHmacJSONTransform,
}

func newHmacJSONTransform(args []string) (TransformLegacy, error) {
	if len(args) == 0 {
		return TransformLegacy{}, g.Error("hmac_sha256_json requires at least one JSON key, e.g. hmac_sha256_json(lead_email,lead_phone_number)")
	}

	keys := make(map[string]struct{}, len(args))
	for _, a := range args {
		keys[strings.TrimSpace(a)] = struct{}{}
	}

	return TransformLegacy{
		Name: "hmac_sha256_json",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			key, err := hmacSecret()
			if err != nil {
				return "", err
			}
			return hmacJSONKeys(val, keys, key), nil
		},
	}, nil
}
