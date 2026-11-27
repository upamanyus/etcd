package auth

import (
	"fmt"
	"testing"

	"go.etcd.io/etcd/api/v3/authpb"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// TestRoleGrantPermissionCanonicalOrder: the role's stored permissions are sorted by (key, range
// end), whatever order they were granted in, also when several share a key: two stores given the
// same grants in different orders hold the same record.
func TestRoleGrantPermissionCanonicalOrder(t *testing.T) {
	type g struct{ key, end string }
	orders := [][]g{
		{{"a", "b"}, {"c", "d"}, {"c", "e"}, {"c", ""}, {"b", "c"}, {"c", "\x00"}},
		{{"c", "\x00"}, {"c", "e"}, {"b", "c"}, {"c", ""}, {"c", "d"}, {"a", "b"}},
		{{"c", ""}, {"c", "d"}, {"a", "b"}, {"c", "\x00"}, {"b", "c"}, {"c", "e"}},
	}
	want := "[a,b)[b,c)[c,)[c,\x00)[c,d)[c,e)"
	for i, order := range orders {
		as, tearDown := setupAuthStore(t)
		for _, p := range order {
			_, err := as.RoleGrantPermission(&pb.AuthRoleGrantPermissionRequest{
				Name: "role-test",
				Perm: &authpb.Permission{PermType: authpb.READ, Key: []byte(p.key), RangeEnd: []byte(p.end)},
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		// Re-granting one with another type keeps its place.
		_, err := as.RoleGrantPermission(&pb.AuthRoleGrantPermissionRequest{
			Name: "role-test",
			Perm: &authpb.Permission{PermType: authpb.WRITE, Key: []byte("c"), RangeEnd: []byte("d")},
		})
		if err != nil {
			t.Fatal(err)
		}
		r, err := as.RoleGet(&pb.AuthRoleGetRequest{Role: "role-test"})
		if err != nil {
			t.Fatal(err)
		}
		got := ""
		for _, p := range r.Perm {
			got += fmt.Sprintf("[%s,%s)", p.Key, p.RangeEnd)
		}
		if got != want {
			t.Errorf("order %d: record %q, want %q", i, got, want)
		}
		tearDown(t)
	}
}
