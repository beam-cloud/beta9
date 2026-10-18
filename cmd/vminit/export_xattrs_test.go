package main

import (
	"errors"
	"reflect"
	"syscall"
	"testing"
)

func TestCollectTreeXattrsUpperFailsClosed(t *testing.T) {
	failure := syscall.EIO
	for _, tc := range []struct {
		name string
		list func(string) ([]string, error)
		get  func(string, string) ([]byte, error)
	}{
		{"list_error", func(string) ([]string, error) { return nil, failure }, func(string, string) ([]byte, error) { t.Fatal("get after failed list"); return nil, nil }},
		{"metacopy_unreadable", func(string) ([]string, error) { return []string{"user.ok", "trusted.overlay.metacopy"}, nil }, func(_ string, name string) ([]byte, error) {
			if name == "trusted.overlay.metacopy" {
				return nil, failure
			}
			return []byte("kept"), nil
		}},
		{"other_attribute_unreadable", func(string) ([]string, error) { return []string{"trusted.overlay.metacopy", "user.failed"}, nil }, func(_ string, name string) ([]byte, error) {
			if name == "user.failed" {
				return nil, failure
			}
			return []byte{0, 0, 0, 0}, nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			records, err := collectTreeXattrRecords("/upper/file", true, tc.list, tc.get)
			if !errors.Is(err, failure) || records != nil {
				t.Fatalf("upper export must abort before writing sparse bytes: records%v err%v", records, err)
			}
		})
	}
}

func TestCollectTreeXattrsGenericRemainsBestEffort(t *testing.T) {
	if records, err := collectTreeXattrRecords("/generic/file", false, func(string) ([]string, error) { return nil, syscall.EOPNOTSUPP }, nil); err != nil || records != nil {
		t.Fatalf("unsupported generic xattrs: %v %v", records, err)
	}
	records, err := collectTreeXattrRecords("/generic/file", false, func(string) ([]string, error) {
		return []string{"unreadable", "user.binary", "trusted.overlay.metacopy"}, nil
	}, func(_ string, name string) ([]byte, error) {
		switch name {
		case "unreadable":
			return nil, syscall.EIO
		case "user.binary":
			return []byte{0, 'b', 255}, nil
		default:
			return []byte{0, 0, 0, 0}, nil
		}
	})
	want := map[string]string{"SCHILY.xattr.user.binary": string([]byte{0, 'b', 255}), "SCHILY.xattr.trusted.overlay.metacopy": string([]byte{0, 0, 0, 0})}
	if err != nil || !reflect.DeepEqual(records, want) {
		t.Fatalf("generic archive changed: %v %v", records, err)
	}
}
