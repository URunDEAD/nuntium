package history

import (
	"reflect"
	"testing"

	"launchpad.net/go-dbus/v1"
)

func TestMessage_Exists(t *testing.T) {
	testCases := []struct {
		name string
		m    Message
		want bool
	}{
		{},
		{"nil", Message(nil), false},
		{"empty", Message{}, true},
		{"not empty", Message{"aaa": dbus.Variant{nil}}, true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			e := tc.m.Exists()
			if e != tc.want {
				t.Errorf("%#v.Exists() = %v, want %v", tc.m, e, tc.want)
			}
		})
	}
}

func TestMessage_IsNew(t *testing.T) {
	testCases := []struct {
		name string
		m    Message
		want bool
		err  error
	}{
		{"nil", Message(nil), false, ErrorNonExistentMessage},
		{"empty", Message{}, false, ErrorMessagePropertyMissing(string(FieldNewEvent))},
		{"missing", Message{"aaa": dbus.Variant{nil}}, false, ErrorMessagePropertyMissing(string(FieldNewEvent))},
		{"wrong type nil", Message{string(FieldNewEvent): dbus.Variant{nil}}, false, ErrorMessagePropertyType{string(FieldNewEvent), false, nil}},
		{"wrong type int", Message{string(FieldNewEvent): dbus.Variant{10}}, false, ErrorMessagePropertyType{string(FieldNewEvent), false, 10}},
		{"true", Message{string(FieldNewEvent): dbus.Variant{true}}, true, nil},
		{"false", Message{string(FieldNewEvent): dbus.Variant{false}}, false, nil},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			isnew, err := tc.m.IsNew()
			if isnew != tc.want || !reflect.DeepEqual(err, tc.err) {
				t.Errorf("%#v.IsNew() = %v, %#v, want %v, %#v", tc.m, isnew, err, tc.want, tc.err)
			}
		})
	}
}

func TestMessage_Status(t *testing.T) {
	testCases := []struct {
		name string
		m    Message
		want MessageStatus
		err  error
	}{
		{"nil", Message(nil), 0, ErrorNonExistentMessage},
		{"empty", Message{}, 0, ErrorMessagePropertyMissing(string(FieldMessageStatus))},
		{"missing", Message{"aaa": dbus.Variant{nil}}, 0, ErrorMessagePropertyMissing(string(FieldMessageStatus))},
		{"wrong type nil", Message{string(FieldMessageStatus): dbus.Variant{nil}}, 0, ErrorMessagePropertyType{string(FieldMessageStatus), int64(0), nil}},
		{"wrong type bool", Message{string(FieldMessageStatus): dbus.Variant{true}}, 0, ErrorMessagePropertyType{string(FieldMessageStatus), int64(0), bool(true)}},
		{"wrong type int", Message{string(FieldMessageStatus): dbus.Variant{int(0)}}, 0, ErrorMessagePropertyType{string(FieldMessageStatus), int64(0), int(0)}},
		{"wrong status -1", Message{string(FieldMessageStatus): dbus.Variant{int64(-1)}}, 0, ErrorUnknownMessageStatus(-1)},
		{"status unknown", Message{string(FieldMessageStatus): dbus.Variant{int64(0)}}, MessageStatusUnknown, nil},
		{"status delivered", Message{string(FieldMessageStatus): dbus.Variant{int64(1)}}, MessageStatusDelivered, nil},
		{"status temporarily failed", Message{string(FieldMessageStatus): dbus.Variant{int64(2)}}, MessageStatusTemporarilyFailed, nil},
		{"status permanently failed", Message{string(FieldMessageStatus): dbus.Variant{int64(3)}}, MessageStatusPermanentlyFailed, nil},
		{"status accepted", Message{string(FieldMessageStatus): dbus.Variant{int64(4)}}, MessageStatusAccepted, nil},
		{"status read", Message{string(FieldMessageStatus): dbus.Variant{int64(5)}}, MessageStatusRead, nil},
		{"status deleted", Message{string(FieldMessageStatus): dbus.Variant{int64(6)}}, MessageStatusDeleted, nil},
		{"status pending", Message{string(FieldMessageStatus): dbus.Variant{int64(7)}}, MessageStatusPending, nil},
		{"status draft", Message{string(FieldMessageStatus): dbus.Variant{int64(8)}}, MessageStatusDraft, nil},
		{"status padding", Message{string(FieldMessageStatus): dbus.Variant{int64(9)}}, _MessageStatusPadding, nil},
		{"wrong status 10", Message{string(FieldMessageStatus): dbus.Variant{int64(10)}}, 0, ErrorUnknownMessageStatus(10)},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			status, err := tc.m.Status()
			if status != tc.want || !reflect.DeepEqual(err, tc.err) {
				t.Errorf("%#v.Status() = %v, %#v, want %v, %#v", tc.m, status, err, tc.want, tc.err)
			}
		})
	}
}
