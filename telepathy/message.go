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
	"fmt"
	"log"
	"sort"

	"github.com/godbus/dbus/v5"
)

var validStatus sort.StringSlice

func init() {
	validStatus = sort.StringSlice{SENT, PERMANENT_ERROR, TRANSIENT_ERROR}
	sort.Strings(validStatus)
}

type MessageInterface struct {
	conn           *dbus.Conn
	objectPath     dbus.ObjectPath
	msgChan        chan *dbus.Message
	deleteChan     chan dbus.ObjectPath
	redownloadChan chan dbus.ObjectPath
	status         string
}

func NewMessageInterface(conn *dbus.Conn, objectPath dbus.ObjectPath, deleteChan chan dbus.ObjectPath, redownloadChan chan dbus.ObjectPath) *MessageInterface {
	msgInterface := MessageInterface{
		conn:           conn,
		objectPath:     objectPath,
		deleteChan:     deleteChan,
		redownloadChan: redownloadChan,
		msgChan:        make(chan *dbus.Message),
		status:         "draft",
	}
	exportMap := map[string]string{
		"delete": "Delete",
		"redownload": "Redownload",
	}
	if err := msgInterface.conn.ExportWithMap(msgInterface, exportMap, msgInterface.objectPath, MMS_MESSAGE_DBUS_IFACE); err != nil {
		panic(err)
	}
	return &msgInterface
}

func (msgInterface *MessageInterface) Close() {
	close(msgInterface.msgChan)
	msgInterface.msgChan = nil
	msgInterface.conn.Export(nil, msgInterface.objectPath, MMS_MESSAGE_DBUS_IFACE)
}

func (msgInterface *MessageInterface) delete() *dbus.Error {
	if msgInterface.deleteChan == nil {
		log.Printf("Deletion of %s is not allowed", msgInterface.objectPath)
		return nil
	}
	msgInterface.deleteChan <- msgInterface.objectPath
	return nil
}

func (msgInterface *MessageInterface) redownload() *dbus.Error {
	if msgInterface.redownloadChan == nil {
		log.Printf("Redownload of %s is not allowed", msgInterface.objectPath)
		return nil
	}
	msgInterface.redownloadChan <- msgInterface.objectPath
	return nil
}

func (msgInterface *MessageInterface) StatusChanged(status string) error {
	i := validStatus.Search(status)
	if i < validStatus.Len() && validStatus[i] == status {
		msgInterface.status = status
		if err := msgInterface.conn.Emit(msgInterface.objectPath,
			MMS_MESSAGE_DBUS_IFACE + "." + propertyChangedSignal, statusProperty, dbus.MakeVariant(status)); err != nil {
			return err
		}
		log.Print("Status changed for ", msgInterface.objectPath, " to ", status)
		return nil
	}
	return fmt.Errorf("status %s is not a valid status", status)
}

func (msgInterface *MessageInterface) GetPayload() *Payload {
	properties := make(map[string]dbus.Variant)
	properties["Status"] = dbus.MakeVariant(msgInterface.status)
	return &Payload{
		Path:       msgInterface.objectPath,
		Properties: properties,
	}
}
