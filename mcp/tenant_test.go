package main

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

// Tenant confinement is the load-bearing security claim of this sidecar: the
// caller never names a bucket, it names a `module`, and the sidecar derives
// t-<tenant>-<module> from the authoritative X-Tenant-Id. These tests attack that
// derivation instead of restating it.

// THE ESCAPE THIS FILE EXISTS FOR: "t-" + tenant + "-" + module is only injective
// while at most one of the two segments may contain "-". Tenant ids in this
// estate are slugs that do (gridiron-robotics), so with a dash allowed in the
// caller-supplied module,
//
//	tenant "acme" + module "hr-payroll"  ==  tenant "acme-hr" + module "payroll"
//
// and tenant acme reads/writes/deletes tenant acme-hr's payroll bucket while
// presenting a perfectly correct X-Tenant-Id. No header spoofing needed.
func TestModuleCannotReachAnotherTenantsBucket(t *testing.T) {
	victim, herr := bucketFor("acme-hr", "payroll")
	if herr != nil {
		t.Fatalf("victim bucket rejected: %v", herr.msg)
	}
	if victim != "t-acme-hr-payroll" {
		t.Fatalf("victim bucket = %q, want t-acme-hr-payroll", victim)
	}
	got, herr := bucketFor("acme", "hr-payroll")
	if herr == nil {
		t.Fatalf("tenant acme reached %q via module %q — that is tenant acme-hr's bucket", got, "hr-payroll")
	}
	if herr.status != 422 {
		t.Fatalf("dashed module -> status %d, want 422", herr.status)
	}
}

// Same escape, end to end through /invoke: the request must be refused BEFORE any
// S3 call, not merely relabelled in the response.
func TestInvokeCannotWriteForeignTenantBucket(t *testing.T) {
	var paths []string
	s := s3Stub(t, func(w http.ResponseWriter, r *http.Request) {
		if answerLocation(w, r) {
			return
		}
		paths = append(paths, r.URL.Path)
		_, _ = io.ReadAll(r.Body)
		w.Header().Set("ETag", `"x"`)
		w.WriteHeader(http.StatusOK)
	})
	code, _ := invokeResult(t, s, "acme",
		`{"tool":"put_object","arguments":{"module":"hr-payroll","key":"q4.csv","content_base64":"aGk="}}`)
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("cross-tenant put -> %d, want 422", code)
	}
	for _, p := range paths {
		if strings.HasPrefix(p, "/t-acme-hr-payroll") {
			t.Fatalf("request reached foreign tenant bucket on the wire: %s", p)
		}
	}
	if len(paths) != 0 {
		t.Fatalf("rejected call still hit S3 %d time(s) (%v), want 0", len(paths), paths)
	}
}

// Mangling non-[a-z0-9-] characters mapped distinct tenant identities onto ONE
// bucket ("acme_hr" -> "acme-hr"), silently merging two tenants' namespaces.
// Rejecting is the fail-closed behaviour.
func TestAmbiguousTenantIdsAreRejectedNotMerged(t *testing.T) {
	real, herr := bucketFor("acme-hr", "payroll")
	if herr != nil {
		t.Fatalf("acme-hr rejected: %v", herr.msg)
	}
	for _, spoof := range []string{"acme_hr", "acme.hr", "acme/../acme-hr", "acme hr", "ACME-HR/x"} {
		got, herr := bucketFor(spoof, "payroll")
		if herr == nil && got == real {
			t.Fatalf("tenant %q mapped onto tenant acme-hr's bucket %q", spoof, got)
		}
		if herr == nil {
			t.Fatalf("tenant %q accepted as %q — expected a 422", spoof, got)
		}
		if herr.status != 422 {
			t.Fatalf("tenant %q -> status %d, want 422", spoof, herr.status)
		}
	}
	// A legitimately dashed tenant id must still work — the fix must not have
	// closed the hole by banning real tenants.
	if b, herr := bucketFor("gridiron-robotics", "orders"); herr != nil || b != "t-gridiron-robotics-orders" {
		t.Fatalf("bucketFor(gridiron-robotics, orders) = %q,%v — legitimate dashed tenants must still work", b, herr)
	}
}

// Truncating an over-long segment merges two tenants that share a prefix; the
// over-long name also produces a bucket longer than S3's 63-char limit, whose
// failure would arrive as a 502 and page the self-heal rail for a caller mistake.
func TestOverlongSegmentsRejectedNotTruncated(t *testing.T) {
	long1 := "tenant-with-a-very-long-identifier-aaaaaa1"
	long2 := "tenant-with-a-very-long-identifier-aaaaaa2"
	b1, e1 := bucketFor(long1, "orders")
	b2, e2 := bucketFor(long2, "orders")
	if e1 == nil && e2 == nil && b1 == b2 {
		t.Fatalf("two distinct tenants truncated onto one bucket %q", b1)
	}
	if e1 == nil || e1.status != 422 {
		t.Fatalf("over-long tenant -> %v, want a 422", e1)
	}
	if b, herr := bucketFor(strings.Repeat("a", 40), strings.Repeat("b", 40)); herr == nil {
		t.Fatalf("bucketFor produced %q (len=%d) — over S3's %d-char bucket limit", b, len(b), maxBucketLen)
	}
}

// list_buckets scoped by a bare "t-<tenant>-" prefix also matches a LONGER
// tenant's buckets: "t-acme-" prefixes "t-acme-hr-payroll". That leaks a sibling
// tenant's bucket inventory (and its module list) to any caller of tenant acme.
func TestListBucketsHidesSiblingTenant(t *testing.T) {
	const bucketsXML = `<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Owner><ID>minio</ID><DisplayName>minio</DisplayName></Owner>
  <Buckets>
    <Bucket><Name>t-acme-orders</Name><CreationDate>2024-01-01T00:00:00.000Z</CreationDate></Bucket>
    <Bucket><Name>t-acme-hr-payroll</Name><CreationDate>2024-01-01T00:00:00.000Z</CreationDate></Bucket>
    <Bucket><Name>t-globex-orders</Name><CreationDate>2024-01-01T00:00:00.000Z</CreationDate></Bucket>
  </Buckets>
</ListAllMyBucketsResult>`
	s := s3Stub(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = io.WriteString(w, bucketsXML)
	})
	code, res := invokeResult(t, s, "acme", `{"tool":"list_buckets","arguments":{}}`)
	if code != http.StatusOK {
		t.Fatalf("list_buckets -> %d, want 200", code)
	}
	bs, _ := res["buckets"].([]any)
	names := []string{}
	for _, b := range bs {
		m, _ := b.(map[string]any)
		names = append(names, m["bucket"].(string))
	}
	if len(names) != 1 || names[0] != "t-acme-orders" {
		t.Fatalf("tenant acme sees %v, want exactly [t-acme-orders] "+
			"(t-acme-hr-payroll belongs to tenant acme-hr, t-globex-orders to globex)", names)
	}
	// And the sibling tenant still sees its own bucket.
	code, res = invokeResult(t, s, "acme-hr", `{"tool":"list_buckets","arguments":{}}`)
	if code != http.StatusOK {
		t.Fatalf("list_buckets(acme-hr) -> %d, want 200", code)
	}
	bs, _ = res["buckets"].([]any)
	if len(bs) != 1 {
		t.Fatalf("tenant acme-hr sees %v, want exactly its own t-acme-hr-payroll", res["buckets"])
	}
	first, _ := bs[0].(map[string]any)
	if first["bucket"] != "t-acme-hr-payroll" || first["module"] != "payroll" {
		t.Fatalf("acme-hr entry = %v, want bucket t-acme-hr-payroll module payroll", first)
	}
}

// Object keys are the other half of the addressing pair. MinIO routes with
// SkipClean(true) so "../" is a literal key there, but presigned URLs are handed
// to browsers and proxies that DO normalize before sending. Keep the signed
// request and the sent request identical by refusing traversal keys outright.
func TestTraversalKeysRejected(t *testing.T) {
	var paths []string
	s := s3Stub(t, func(w http.ResponseWriter, r *http.Request) {
		if answerLocation(w, r) {
			return
		}
		paths = append(paths, r.URL.Path)
		_, _ = io.ReadAll(r.Body)
		w.Header().Set("ETag", `"x"`)
		w.WriteHeader(http.StatusOK)
	})
	// "a/b/" is here because the shared guard now takes an allowTrailingSlash
	// flag: a prefix may end in "/", a key may not (that is a trailing empty
	// segment). Flipping that flag for keys must not go unnoticed.
	for _, bad := range []string{"../t-globex-orders/steal.txt", "a/../../x", "/abs.txt", "", "a//b", "a/b/"} {
		for _, tool := range []string{"put_object", "delete_object", "stat_object", "presign_get", "presign_put"} {
			body := `{"tool":"` + tool + `","arguments":{"module":"orders","key":` + jsonStr(bad) + `,"content_base64":"aGk="}}`
			code, _ := invokeResult(t, s, "acme", body)
			if code != http.StatusUnprocessableEntity {
				t.Fatalf("%s with key %q -> %d, want 422", tool, bad, code)
			}
		}
	}
	if len(paths) != 0 {
		t.Fatalf("traversal keys reached S3 %d time(s): %v", len(paths), paths)
	}
	// A presigned URL must never carry a traversal segment a client would resolve.
	code, res := invokeResult(t, s, "acme",
		`{"tool":"presign_get","arguments":{"module":"orders","key":"reports/q4.pdf"}}`)
	if code != http.StatusOK {
		t.Fatalf("legitimate presign_get -> %d, want 200", code)
	}
	if u, _ := res["url"].(string); !strings.Contains(u, "/t-acme-orders/reports/q4.pdf") || strings.Contains(u, "..") {
		t.Fatalf("presigned url = %q", u)
	}
}

func jsonStr(s string) string {
	b, err := json.Marshal(s)
	if err != nil { // a Go string is always marshalable; keep the helper total anyway
		return `""`
	}
	return string(b)
}

// listXMLEmpty is an empty (untruncated) ListBucketResult — enough for minio-go
// to finish a list, so a prefix that is NOT rejected reaches the stub and is
// visible on the wire.
const listXMLEmpty = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>t-acme-orders</Name><KeyCount>0</KeyCount>
  <MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>
</ListBucketResult>`

// list_objects was the ONE tool of six that handed caller-supplied path input to
// MinIO without the guard the other five apply to `key`. It is not an escape on
// today's surface — the prefix rides in a query parameter, the tool is read-only,
// and the bucket is still derived server-side from the tenant — but "today" is
// the entire claim, and the tool that does not follow the rule is exactly where
// the next change introduces a real hole. The segments a key is refused for must
// be refused for a prefix, and refused BEFORE any S3 call.
func TestTraversalPrefixesRejected(t *testing.T) {
	var seen []string
	s := s3Stub(t, func(w http.ResponseWriter, r *http.Request) {
		if answerLocation(w, r) {
			return
		}
		seen = append(seen, r.URL.Path+"?"+r.URL.RawQuery)
		w.Header().Set("Content-Type", "application/xml")
		_, _ = io.WriteString(w, listXMLEmpty)
	})
	for _, bad := range []string{
		"../t-globex-orders/", "a/../../x", "/abs/", `a\b`, "a//b", "..", ".", "reports/./q4",
	} {
		body := `{"tool":"list_objects","arguments":{"module":"orders","prefix":` + jsonStr(bad) + `}}`
		code, _ := invokeResult(t, s, "acme", body)
		if code != http.StatusUnprocessableEntity {
			t.Fatalf("list_objects with prefix %q -> %d, want 422 (the guard the other five tools apply)", bad, code)
		}
	}
	if len(seen) != 0 {
		t.Fatalf("traversal prefixes reached S3 %d time(s): %v", len(seen), seen)
	}
}

// The guard must be applied, not faked by dropping the argument: a legitimate
// prefix must still reach MinIO verbatim — including the trailing-slash "folder"
// form, which is the one shape a prefix may have and a key may not.
func TestLegitimatePrefixReachesS3Verbatim(t *testing.T) {
	for _, want := range []string{"", "reports/", "reports/q4", "reports/q4-final", "a/b/c"} {
		var got string
		calls := 0
		s := s3Stub(t, func(w http.ResponseWriter, r *http.Request) {
			if answerLocation(w, r) {
				return
			}
			calls++
			got = r.URL.Query().Get("prefix")
			w.Header().Set("Content-Type", "application/xml")
			_, _ = io.WriteString(w, listXMLEmpty)
		})
		code, res := invokeResult(t, s, "acme",
			`{"tool":"list_objects","arguments":{"module":"orders","prefix":`+jsonStr(want)+`}}`)
		if code != http.StatusOK {
			t.Fatalf("list_objects with legitimate prefix %q -> %d, want 200", want, code)
		}
		if calls == 0 {
			t.Fatalf("prefix %q: no list call reached S3", want)
		}
		if got != want {
			t.Fatalf("prefix on the wire = %q, want %q — the guard must not rewrite or drop it", got, want)
		}
		if res["bucket"] != "t-acme-orders" {
			t.Fatalf("bucket = %v, want t-acme-orders", res["bucket"])
		}
	}
}

// stat_object mapped EVERY error to 404, so a dead or unreachable store answered
// "not found" — never >=500, therefore never paging the self-heal rail.
func TestStatObjectOutageIs502(t *testing.T) {
	s := s3Stub(t, func(w http.ResponseWriter, r *http.Request) {
		if answerLocation(w, r) {
			return
		}
		w.WriteHeader(http.StatusForbidden) // credentials/policy failure, not a missing key
	})
	code, _ := invokeResult(t, s, "acme",
		`{"tool":"stat_object","arguments":{"module":"orders","key":"a/b.txt"}}`)
	if code != http.StatusBadGateway {
		t.Fatalf("stat during a storage failure -> %d, want 502 (a 404 pages nobody)", code)
	}
}
