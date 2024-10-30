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

	"github.com/godbus/dbus/v5"

	"gitlab.com/ubports/development/core/nuntium/mms"
)

type MMSManager struct {
	conn     *dbus.Conn
	services []*MMSService
}

func NewMMSManager(conn *dbus.Conn) (*MMSManager, error) {
	reply, err := conn.RequestName(MMS_DBUS_NAME, dbus.NameFlagDoNotQueue)
	if err != nil {
		return nil, fmt.Errorf("Could not aquire name %s: %v", MMS_DBUS_NAME, err)
	}
	if reply != dbus.RequestNameReplyPrimaryOwner {
		return nil, fmt.Errorf("Name %s already taken", MMS_DBUS_NAME)
	}

	log.Printf("Registered %s on bus as %s", conn.Names()[0], MMS_DBUS_NAME)

	manager := MMSManager{conn: conn}

	exportMap := map[string]string{"getServices": "GetServices"}
	if err := manager.conn.ExportWithMap(manager, exportMap, MMS_DBUS_PATH, MMS_MANAGER_DBUS_IFACE); err != nil {
		panic(err)
	}
	return &manager, nil
}

func (manager *MMSManager) getServices() ([]Payload, *dbus.Error) {
	log.Print("Received GetServices()")
	var payloads []Payload
	for i, _ := range manager.services {
		payloads = append(payloads, manager.services[i].payload)
	}
	return payloads, nil
}

func (manager *MMSManager) serviceAdded(payload *Payload) error {
	log.Print("Service added ", payload.Path)
	if err := manager.conn.Emit(MMS_DBUS_PATH, MMS_MANAGER_DBUS_IFACE + "." + serviceAddedSignal, payload.Path, payload.Properties); err != nil {
		return fmt.Errorf("Cannot send ServiceAdded for %s", payload.Path)
	}
	return nil
}

func (manager *MMSManager) AddService(identity string, modemObjPath dbus.ObjectPath, outgoingChannel chan *OutgoingMessage, useDeliveryReports bool, mNotificationIndChan chan<- *mms.MNotificationInd) (*MMSService, error) {
	for i := range manager.services {
		if manager.services[i].isService(identity) {
			return manager.services[i], nil
		}
	}
	service := NewMMSService(manager.conn, modemObjPath, identity, outgoingChannel, useDeliveryReports, mNotificationIndChan)
	if err := manager.serviceAdded(&service.payload); err != nil {
		return &MMSService{}, err
	}
	manager.services = append(manager.services, service)
	return service, nil
}

func (manager *MMSManager) serviceRemoved(payload *Payload) error {
	log.Print("Service removed ", payload.Path)
	if err := manager.conn.Emit(MMS_DBUS_PATH, MMS_MANAGER_DBUS_IFACE + "." + serviceRemovedSignal, payload.Path); err != nil {
		return fmt.Errorf("Cannot emit ServiceRemoved for %s", payload.Path)
	}
	return nil
}

func (manager *MMSManager) RemoveService(identity string) error {
	for i := range manager.services {
		if manager.services[i].isService(identity) {
			manager.serviceRemoved(&manager.services[i].payload)
			manager.services[i].Close()
			manager.services = append(manager.services[:i], manager.services[i+1:]...)
			log.Print("Service left: ", len(manager.services))
			return nil
		}
	}
	return fmt.Errorf("Cannot find service serving %s", identity)
}
