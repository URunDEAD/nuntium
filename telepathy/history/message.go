package history

import (
	"fmt"

	"github.com/godbus/dbus/v5"
)

type MessageStatus int64

// Should match MessageStatus enum in https://gitlab.com/ubports/core/history-service/-/blob/main/src/types.h#L60
const (
	MessageStatusUnknown MessageStatus = iota
	MessageStatusDelivered
	MessageStatusTemporarilyFailed
	MessageStatusPermanentlyFailed
	MessageStatusAccepted
	MessageStatusRead
	MessageStatusDeleted
	MessageStatusPending
	MessageStatusDraft
	_MessageStatusPadding
)

type MessageField string

// Should have it's counterpart in https://gitlab.com/ubports/core/history-service/-/blob/main/src/types.h
const (
	FieldNewEvent      MessageField = "newEvent"
	FieldMessageStatus MessageField = "messageStatus"
)

type Message map[string]dbus.Variant

func (m Message) Exists() bool {
	return m != nil
}

var ErrorNonExistentMessage = fmt.Errorf("message doesn't exist")

type ErrorMessagePropertyMissing string

func (e ErrorMessagePropertyMissing) Error() string {
	return fmt.Sprintf("Message proprety missing: %s", string(e))
}

type ErrorMessagePropertyType struct {
	property       string
	wantType, have interface{}
}

func (e ErrorMessagePropertyType) Error() string {
	return fmt.Sprintf("Message property \"%s\" type is %T, want %T", e.property, e.have, e.wantType)
}

func (m Message) IsNew() (bool, error) {
	if !m.Exists() {
		return false, ErrorNonExistentMessage
	}
	v, ok := m[string(FieldNewEvent)]
	if !ok {
		return false, ErrorMessagePropertyMissing(string(FieldNewEvent))
	}

	var newEvent bool
	if v.Store(&newEvent) != nil {
		return false, ErrorMessagePropertyType{string(FieldNewEvent), bool(false), v.Value()}
	}

	return newEvent, nil
}

type ErrorUnknownMessageStatus int64

func (e ErrorUnknownMessageStatus) Error() string {
	return fmt.Sprintf("Unknown message status: %d", int(e))
}

func messageStatusFromInt(i int64) (MessageStatus, error) {
	if i < int64(MessageStatusUnknown) || i > int64(_MessageStatusPadding) {
		return 0, ErrorUnknownMessageStatus(i)
	}
	return MessageStatus(i), nil
}

func (m Message) Status() (MessageStatus, error) {
	if !m.Exists() {
		return 0, ErrorNonExistentMessage
	}
	v, ok := m[string(FieldMessageStatus)]
	if !ok {
		return 0, ErrorMessagePropertyMissing(string(FieldMessageStatus))
	}

	var status int64
	if v.Store(&status) != nil {
		return 0, ErrorMessagePropertyType{string(FieldMessageStatus), int64(0), v.Value()}
	}

	return messageStatusFromInt(status)
}
