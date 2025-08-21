/*
 * Copyright 2014 Canonical Ltd.
 *
 * Authors:
 * Sergio Schvezov: sergio.schvezov@cannical.com
 *
 * This file is part of telepathy.
 *
 * mms is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; version 3.
 *
 * mms is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package telepathy

import (
	"encoding/json"
	_ "errors"
	"fmt"
	"log"
	"path/filepath"
	_ "reflect"
	"strings"
	"time"

	"github.com/godbus/dbus/v5"
	"github.com/godbus/dbus/v5/prop"

	"gitlab.com/ubports/development/core/nuntium/mms"
	"gitlab.com/ubports/development/core/nuntium/storage"
	"gitlab.com/ubports/development/core/nuntium/telepathy/history"
)

//Payload is used to build the dbus messages; this is a workaround as v1 of go-dbus
//tries to encode and decode private fields.
type Payload struct {
	Path       dbus.ObjectPath
	Properties map[string]dbus.Variant
}

type MMSService struct {
	payload              Payload
	Properties           map[string]dbus.Variant
	conn                 *dbus.Conn
	msgChan              chan *dbus.Message
	messageHandlers      map[dbus.ObjectPath]*MessageInterface
	msgDeleteChan        chan dbus.ObjectPath
	msgRedownloadChan    chan dbus.ObjectPath
	identity             string
	outMessage           chan *OutgoingMessage
	mNotificationIndChan chan<- *mms.MNotificationInd
}

type Attachment struct {
	Id        string
	MediaType string
	FilePath  string
	Offset    uint64
	Length    uint64
}

type OutAttachment struct {
	Id          string
	ContentType string
	FilePath    string
}

type OutgoingMessage struct {
	Recipients  []string
	Attachments []OutAttachment
	UUID        chan string
}

func NewMMSService(conn *dbus.Conn, modemObjPath dbus.ObjectPath, identity string, outgoingChannel chan *OutgoingMessage, useDeliveryReports bool, mNotificationIndChan chan<- *mms.MNotificationInd) *MMSService {
	properties := make(map[string]dbus.Variant)
	properties[identityProperty] = dbus.MakeVariant(identity)
	serviceProperties := make(map[string]dbus.Variant)
	serviceProperties[useDeliveryReportsProperty] = dbus.MakeVariant(useDeliveryReports)
	serviceProperties[modemObjectPathProperty] = dbus.MakeVariant(modemObjPath)
	payload := Payload{
		Path:       dbus.ObjectPath(MMS_DBUS_PATH + "/" + identity),
		Properties: properties,
	}
	service := MMSService{
		payload:              payload,
		Properties:           serviceProperties,
		conn:                 conn,
		msgChan:              make(chan *dbus.Message),
		msgDeleteChan:        make(chan dbus.ObjectPath),
		msgRedownloadChan:    make(chan dbus.ObjectPath),
		messageHandlers:      make(map[dbus.ObjectPath]*MessageInterface),
		outMessage:           outgoingChannel,
		identity:             identity,
		mNotificationIndChan: mNotificationIndChan,
	}

	go service.watchMessageDeleteCalls()
	go service.watchMessageRedownloadCalls()

	if pc, err := service.GetPreferredContext(); err == nil {
		service.Properties[preferredContextProperty] = dbus.MakeVariant(pc)
	} else {
		// Using "/" as an invalid 'path' even though it could be considered 'incorrect'
		service.Properties[preferredContextProperty] = dbus.MakeVariant(dbus.ObjectPath("/"))
	}
	propsMap := prop.Map{
		MMS_SERVICE_DBUS_IFACE: {
			preferredContextProperty: {
				Value: service.Properties[preferredContextProperty],
				Writable: true,
				Emit: prop.EmitTrue,
				Callback: service.setPropPreferredContext,
			},
		},
	}
	prop.Export(service.conn, payload.Path, propsMap)
	methodTable := map[string]interface{}{
		"GetMessages":   service.getMessages,
		"SendMessage":   service.sendMessage,
		"GetProperties": service.getProperties,
	}
	if err := service.conn.ExportMethodTable(methodTable, payload.Path, MMS_SERVICE_DBUS_IFACE); err != nil {
		panic(err)
	}
	return &service
}

func (service *MMSService) getMessages() ([]Payload, *dbus.Error) {
	//TODO implement store and forward
	return []Payload{}, nil
}

func (service *MMSService) sendMessage(msg dbus.Message, recipients []string, attachments []OutAttachment) (dbus.ObjectPath, *dbus.Error) {
	outMessage := &OutgoingMessage{
		Recipients: recipients,
		Attachments: attachments,
		UUID: make(chan string),
	}
	service.outMessage <- outMessage
	uuid := <-outMessage.UUID
	msgObjectPath := service.GenMessagePath(uuid)
	msgIface := NewMessageInterface(service.conn, msgObjectPath, service.msgDeleteChan, nil)
	service.messageHandlers[msgObjectPath] = msgIface
	service.MessageAdded(msgIface.GetPayload())
	return msgObjectPath, nil
}

func (service *MMSService) getProperties() (map[string]dbus.Variant, *dbus.Error) {
	if pc, err := service.GetPreferredContext(); err == nil {
		service.Properties[preferredContextProperty] = dbus.MakeVariant(pc)
	} else {
		// Using "/" as an invalid 'path' even though it could be considered 'incorrect'
		service.Properties[preferredContextProperty] = dbus.MakeVariant(dbus.ObjectPath("/"))
	}
	return service.Properties, nil
}

func (*MMSService) getMMSState(objectPath dbus.ObjectPath) (storage.MMSState, error) {
	uuid, err := getUUIDFromObjectPath(objectPath)
	if err != nil {
		return storage.MMSState{}, err
	}

	return storage.GetMMSState(uuid)
}

func (service *MMSService) watchMessageDeleteCalls() {
	for msgObjectPath := range service.msgDeleteChan {
		if mmsState, err := service.getMMSState(msgObjectPath); err == nil {
			if mmsState.State != storage.RESPONDED && mmsState.MNotificationInd != nil && !mmsState.MNotificationInd.Expired() {
				log.Printf("Message %s is not responded and not expired, not deleting.", string(msgObjectPath))
				continue
			}
		}

		if err := service.MessageRemoved(msgObjectPath); err != nil {
			log.Print("Failed to delete ", msgObjectPath, ": ", err)
		}
	}
}

func (service *MMSService) watchMessageRedownloadCalls() {
	for msgObjectPath := range service.msgRedownloadChan {
		mmsState, err := service.getMMSState(msgObjectPath)
		if err != nil {
			log.Printf("Redownload of %s error: retrieving message state error: %v", string(msgObjectPath), err)
			continue
		}
		if mmsState.State != storage.NOTIFICATION {
			log.Printf("Redownload of %s error: message was already downloaded", string(msgObjectPath))
			continue
		}
		if mmsState.MNotificationInd == nil {
			log.Printf("Redownload of %s error: no mNotificationInd found", string(msgObjectPath))
			continue
		}

		// Create new mNotificationInd with info about redownload.
		newMNotificationInd := mmsState.MNotificationInd.Copy()
		newMNotificationInd.RedownloadOfUUID = mmsState.MNotificationInd.UUID
		newMNotificationInd.UUID = mms.GenUUID()
		if _, err := storage.Create(mmsState.ModemId, newMNotificationInd); err != nil {
			log.Printf("Error creating message in storage during redownload: %v", err)
		}

		// Stop previous message handling, remove from storage and signal that message was removed.
		if err := service.MessageRemoved(msgObjectPath); err != nil {
			log.Printf("Redownload of %s warning: removing message error: %v", string(msgObjectPath), err)
		}

		// Start mNotificationInd handling as if pushed from MMS service.
		service.mNotificationIndChan <- newMNotificationInd
	}
}

func getUUIDFromObjectPath(objectPath dbus.ObjectPath) (string, error) {
	str := string(objectPath)
	defaultError := fmt.Errorf("%s is not a proper object path for a Message", str)
	if str == "" {
		return "", defaultError
	}
	uuid := filepath.Base(str)
	if uuid == "" || uuid == ".." || uuid == "." {
		return "", defaultError
	}
	return uuid, nil
}

func (service *MMSService) SetPreferredContext(context dbus.ObjectPath) error {
	// make set a noop if we are setting the same thing
	if pc, err := service.GetPreferredContext(); err != nil && context == pc {
		return nil
	}

	if err := storage.SetPreferredContext(service.identity, context); err != nil {
		return err
	}
	return service.conn.Emit(service.payload.Path,
		MMS_SERVICE_DBUS_IFACE + "." + propertyChangedSignal,
		preferredContextProperty, dbus.MakeVariant(context))
}

func (service *MMSService) GetPreferredContext() (dbus.ObjectPath, error) {
	return storage.GetPreferredContext(service.identity)
}

func (service *MMSService) setPropPreferredContext(change *prop.Change) *dbus.Error {
	preferredContextObjectPath := change.Value.(dbus.ObjectPath)
	service.Properties[preferredContextProperty] = dbus.MakeVariant(preferredContextObjectPath)
	err := service.SetPreferredContext(preferredContextObjectPath)
	return dbus.NewError(err.Error(), nil)
}

// MessageRemoved closes message handlers, removes message from storage and emits the MessageRemoved signal to mms service dbus interface for message identified by objectPath parameter in this order.
// If message is not handled, removing from storage or sending signal fails, error is returned.
func (service *MMSService) MessageRemoved(objectPath dbus.ObjectPath) error {
	if service == nil {
		return ErrorNilMMSService
	}

	if _, ok := service.messageHandlers[objectPath]; !ok {
		return fmt.Errorf("message not handled")
	}

	service.messageHandlers[objectPath].Close()
	delete(service.messageHandlers, objectPath)

	uuid, err := getUUIDFromObjectPath(objectPath)
	if err != nil {
		return err
	}
	if err := storage.Destroy(uuid); err != nil {
		return err
	}

	return service.SingnalMessageRemoved(objectPath)
}

// Sends messageRemovedSignal signal to MMS_SERVICE_DBUS_IFACE to indicate, that the message stopped being handled and was removed from nuntium storage.
func (service *MMSService) SingnalMessageRemoved(objectPath dbus.ObjectPath) error {
	if service == nil {
		return ErrorNilMMSService
	}

	return service.conn.Emit(service.payload.Path, MMS_SERVICE_DBUS_IFACE + "." + messageRemovedSignal, objectPath)
}

func (service *MMSService) IncomingMessageFailAdded(mNotificationInd *mms.MNotificationInd, downloadError error) error {
	if service == nil {
		return fmt.Errorf("Nil MMSService")
	}

	if err := mNotificationInd.PopDebugError(mms.DebugErrorTelepathyErrorNotify); err != nil {
		log.Printf("Forcing IncomingMessageFailAdded debug error: %#v", err)
		storage.UpdateMNotificationInd(mNotificationInd)
		return err
	}

	params := make(map[string]dbus.Variant)

	params["Status"] = dbus.MakeVariant("received")
	params["Date"] = dbus.MakeVariant(time.Now().Format(time.RFC3339))
	params["Sender"] = dbus.MakeVariant(strings.TrimSuffix(mNotificationInd.From, PLMN))

	errorCode := "x-ubports-nuntium-mms-error-unknown"
	if eci, ok := downloadError.(interface{ Code() string }); ok {
		errorCode = eci.Code()
	}

	allowRedownload := false
	if ari, ok := downloadError.(interface{ AllowRedownload() bool }); ok {
		allowRedownload = ari.AllowRedownload()
	}

	expire := mNotificationInd.Expire().Format(time.RFC3339)
	if allowRedownload && mNotificationInd.Expired() {
		// Expired, don't allow redownload.
		log.Printf("Message expired at %s", mNotificationInd.Expire())
		allowRedownload = false
	}

	var mobileData *bool
	if enabled, err := service.MobileDataEnabled(); err == nil {
		mobileData = &enabled
	} else {
		log.Printf("Error detecting if mobile data is enabled: %v", err)
	}

	errorMessage, err := json.Marshal(&struct {
		Code       string
		Message    string
		Expire     string `json:",omitempty"`
		Size       uint64 `json:",omitempty"`
		MobileData *bool  `json:",omitempty"`
	}{errorCode, downloadError.Error(), expire, mNotificationInd.Size, mobileData})
	if err != nil {
		log.Printf("Error marshaling download error message to json: %v", err)
		errorMessage = []byte("{}")
	}
	params["Error"] = dbus.MakeVariant(string(errorMessage))
	params["AllowRedownload"] = dbus.MakeVariant(allowRedownload)

	if mNotificationInd.RedownloadOfUUID != "" {
		params["DeleteEvent"] = dbus.MakeVariant(string(service.GenMessagePath(mNotificationInd.RedownloadOfUUID)))
	}
	if !mNotificationInd.Received.IsZero() {
		params["Received"] = dbus.MakeVariant(uint32(mNotificationInd.Received.Unix()))
	}

	payload := Payload{Path: service.GenMessagePath(mNotificationInd.UUID), Properties: params}

	// Don't pass a redownload channel to NewMessageInterface if redownload not allowed.
	redownloadChan := service.msgRedownloadChan
	if !allowRedownload {
		redownloadChan = nil
	}
	service.messageHandlers[payload.Path] = NewMessageInterface(service.conn, payload.Path, service.msgDeleteChan, redownloadChan)
	return service.MessageAdded(&payload)
}

//IncomingMessageAdded emits a MessageAdded with the path to the added message which
//is taken as a parameter and creates an object path on the message interface.
func (service *MMSService) IncomingMessageAdded(mRetConf *mms.MRetrieveConf, mNotificationInd *mms.MNotificationInd) error {
	if service == nil {
		return ErrorNilMMSService
	}

	if err := mNotificationInd.PopDebugError(mms.DebugErrorReceiveHandle); err != nil {
		log.Printf("Forcing getAndHandleMRetrieveConf debug error: %#v", err)
		storage.UpdateMNotificationInd(mNotificationInd)
		return err
	}

	payload, err := service.parseMessage(mRetConf)
	if err != nil {
		return err
	}

	if mNotificationInd.RedownloadOfUUID != "" {
		payload.Properties["DeleteEvent"] = dbus.MakeVariant(string(service.GenMessagePath(mNotificationInd.RedownloadOfUUID)))
	}
	if !mNotificationInd.Received.IsZero() {
		payload.Properties["Received"] = dbus.MakeVariant(mNotificationInd.Received.Unix())
	}

	service.messageHandlers[payload.Path] = NewMessageInterface(service.conn, payload.Path, service.msgDeleteChan, nil)
	return service.MessageAdded(&payload)
}

func (service *MMSService) InitializationMessageAdded(mRetConf *mms.MRetrieveConf, mNotificationInd *mms.MNotificationInd) error {
	if service == nil {
		return ErrorNilMMSService
	}

	if mNotificationInd == nil {
		return ErrorNilMNotificationInd
	}

	path := service.GenMessagePath(mNotificationInd.UUID)
	if _, ok := service.messageHandlers[path]; ok {
		return fmt.Errorf("message is already handled")
	}

	// Initialization message only needs these properties to spawn proper handles in telepathy.
	payload := Payload{Path: path, Properties: map[string]dbus.Variant{
		"Status":  dbus.MakeVariant("received"),
		"Sender":  dbus.MakeVariant(strings.TrimSuffix(mNotificationInd.From, PLMN)),
		"Rescued": dbus.MakeVariant(true),
		"Silent":  dbus.MakeVariant(true),
	}}

	// Extract "Sender" and "Recipients" property from mRetConf, if any.
	if mRetConf != nil {
		if pl, err := service.parseMessage(mRetConf); err == nil {
			if _, ok := pl.Properties["Sender"]; !ok {
				payload.Properties["Sender"] = pl.Properties["Sender"]
			}
			if _, ok := pl.Properties["Recipients"]; !ok {
				payload.Properties["Recipients"] = pl.Properties["Recipients"]
			}
		} else {
			log.Printf("Error parsing mRetConf for initialization message %s: %v", path, err)
		}
	}

	service.messageHandlers[path] = NewMessageInterface(service.conn, path, service.msgDeleteChan, service.msgRedownloadChan)
	return service.MessageAdded(&payload)
}

//MessageAdded emits a MessageAdded with the path to the added message which
//is taken as a parameter
func (service *MMSService) MessageAdded(msgPayload *Payload) error {
	return service.conn.Emit(service.payload.Path, MMS_SERVICE_DBUS_IFACE + "." + messageAddedSignal, msgPayload.Path, msgPayload.Properties)
}

func (service *MMSService) isService(identity string) bool {
	path := dbus.ObjectPath(MMS_DBUS_PATH + "/" + identity)
	if path == service.payload.Path {
		return true
	}
	return false
}

func (service *MMSService) Close() {
	service.conn.Export(nil, service.payload.Path, MMS_MESSAGE_DBUS_IFACE)
	close(service.msgChan)
	close(service.msgDeleteChan)
	close(service.msgRedownloadChan)
}

func (service *MMSService) parseMessage(mRetConf *mms.MRetrieveConf) (Payload, error) {
	params := make(map[string]dbus.Variant)
	params["Status"] = dbus.MakeVariant("received")
	//TODO retrieve date correctly
	params["Date"] = dbus.MakeVariant(parseDate(mRetConf.Date))
	params["Sender"] = dbus.MakeVariant(strings.TrimSuffix(mRetConf.From, PLMN))
	if mRetConf.Subject != "" {
		params["Subject"] = dbus.MakeVariant(mRetConf.Subject)
	}

	params["Recipients"] = dbus.MakeVariant(parseRecipients(strings.Join(mRetConf.To, ",")))
	if smil, err := mRetConf.GetSmil(); err == nil {
		params["Smil"] = dbus.MakeVariant(smil)
	}
	var attachments []Attachment
	dataParts := mRetConf.GetDataParts()
	for i := range dataParts {
		var filePath string
		if f, err := storage.GetMMS(mRetConf.UUID); err == nil {
			filePath = f
		} else {
			return Payload{}, err
		}
		attachment := Attachment{
			Id:        dataParts[i].ContentId,
			MediaType: dataParts[i].MediaType,
			FilePath:  filePath,
			Offset:    uint64(dataParts[i].Offset),
			Length:    uint64(len(dataParts[i].Data)),
		}
		attachments = append(attachments, attachment)
	}
	params["Attachments"] = dbus.MakeVariant(attachments)
	payload := Payload{Path: service.GenMessagePath(mRetConf.UUID), Properties: params}
	return payload, nil
}

func parseDate(unixTime uint64) string {
	const layout = "2014-03-30T18:15:30-0300"
	date := time.Unix(int64(unixTime), 0)
	return date.Format(time.RFC3339)
}

func parseRecipients(to string) []string {
	recipients := strings.Split(to, ",")
	for i := range recipients {
		if strings.HasSuffix(recipients[i], PLMN) {
			recipients[i] = recipients[i][:len(recipients[i])-len(PLMN)]
		}
	}
	return recipients
}

func (service *MMSService) MessageDestroy(uuid string) error {
	msgObjectPath := service.GenMessagePath(uuid)
	if msgInterface, ok := service.messageHandlers[msgObjectPath]; ok {
		msgInterface.Close()
		delete(service.messageHandlers, msgObjectPath)
		return nil
	}
	return fmt.Errorf("no message interface handler for object path %s", msgObjectPath)
}

func (service *MMSService) MessageStatusChanged(uuid, status string) error {
	msgObjectPath := service.GenMessagePath(uuid)
	if msgInterface, ok := service.messageHandlers[msgObjectPath]; ok {
		return msgInterface.StatusChanged(status)
	}
	return fmt.Errorf("no message interface handler for object path %s", msgObjectPath)
}

//TODO randomly creating a uuid until the download manager does this for us
func (service *MMSService) GenMessagePath(uuid string) dbus.ObjectPath {
	if service == nil {
		return dbus.ObjectPath(MMS_DBUS_PATH + "//" + uuid)
	}

	return dbus.ObjectPath(MMS_DBUS_PATH + "/" + service.identity + "/" + uuid)
}

// Returns if mobile data is enabled right now.
// Under the hood, DBus service property is read, if something fails, error is returned.
//
// dbus-send --session --print-reply \
//     --dest=com.lomiri.connectivity1 \
//     /com/lomiri/connectivity1/Private \
//     org.freedesktop.DBus.Properties.Get \
// string:com.lomiri.connectivity1.Private \
// 	string:'MobileDataEnabled'
func (service *MMSService) MobileDataEnabled() (bool, error) {
	v, err := service.conn.Object("com.lomiri.connectivity1", "/com/lomiri/connectivity1/Private").
		GetProperty("com.lomiri.connectivity1.Private.MobileDataEnabled")
	if err != nil {
		return false, err
	}
	var enabled bool
	if err := v.Store(&enabled); err != nil {
		return false, fmt.Errorf("decoding error: %w", err)
	}
	return enabled, nil
}

func (service *MMSService) HistoryService() *history.HistoryService {
	if service == nil {
		return nil
	}
	return history.NewHistoryService(service.conn)
}
