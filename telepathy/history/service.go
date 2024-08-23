package history

import (
	"fmt"
	"log"

	"github.com/godbus/dbus/v5"
)

// HistoryService allows to communicate with message history service through dbus.
type HistoryService struct {
	conn *dbus.Conn
}

func NewHistoryService(conn *dbus.Conn) *HistoryService {
	return &HistoryService{conn}
}

// Returns message identified by parameters from HistoryService.
func (service *HistoryService) GetSingleMessage(accountId, threadId, eventId string) (Message, error) {
	eventType := int32(0) // History::EventTypeText
	msg := map[string]dbus.Variant{}
	err := service.conn.Object("com.lomiri.HistoryService", "/com/lomiri/HistoryService").
		Call("com.lomiri.HistoryService.GetSingleEvent", 0, eventType, accountId, threadId, eventId).
		Store(&msg)
	if err != nil {
		return nil, err
	}
	return Message(msg), nil
}

var ErrorNilHistoryService = fmt.Errorf("nil HistoryService pointer")

// Returns first message identified by eventId from HistoryService.
func (service *HistoryService) GetMessage(eventId string) (Message, error) {
	if service == nil {
		return nil, ErrorNilHistoryService
	}

	// Get event view.
	eventType := int32(0) // History::EventTypeText
	sort := map[string]dbus.Variant(nil)
	filter := map[string]dbus.Variant{
		"filterType":     dbus.MakeVariant(int32(0)), // FilterTypeStandard
		"filterProperty": dbus.MakeVariant("eventId"),
		"filterValue":    dbus.MakeVariant(eventId),
		"matchFlags":     dbus.MakeVariant(int32(1)), // MatchCaseSensitive
	}
	var eventView string
	err := service.conn.Object("com.lomiri.HistoryService", "/com/lomiri/HistoryService").
		Call("com.lomiri.HistoryService.QueryEvents", 0, eventType, sort, filter).
		Store(&eventView)
	if err != nil {
		return nil, err
	}

	// Destroy event view at end.
	// dbus-send --session --print-reply --dest=com.lomiri.HistoryService /com/lomiri/HistoryService/eventview2413609620210130164828892 com.lomiri.HistoryService.EventView.Destroy
	obj := service.conn.Object("com.lomiri.HistoryService", dbus.ObjectPath(eventView))
	defer func() {
		if err := obj.Call("com.lomiri.HistoryService.EventView.Destroy", 0).Err; err != nil {
			log.Printf("HistoryService.GetMessage: Destroy error: %v", err)
		}
	}()

	// Check if query is valid.
	var isValid bool
	if err := obj.Call("com.lomiri.HistoryService.EventView.IsValid", 0).Store(&isValid); err != nil {
		return nil, fmt.Errorf("Request validation error: %w", err)
	}
	if !isValid {
		return nil, fmt.Errorf("QueryEvents got invalid query")
	}

	// Get message.
	msgs := []map[string]dbus.Variant(nil)
	if err := obj.Call("com.lomiri.HistoryService.EventView.NextPage", 0).Store(&msgs); err != nil {
		return nil, fmt.Errorf("Next page error: %w", err)
	}
	if len(msgs) > 1 {
		return nil, fmt.Errorf("Too many messages found: %d", len(msgs))
	}
	if len(msgs) == 0 {
		return Message(nil), nil
	}
	return Message(msgs[0]), nil
}
