/*
 * Copyright 2014 Canonical Ltd.
 *
 * Authors:
 * Sergio Schvezov: sergio.schvezov@cannical.com
 *
 * This file is part of nuntium.
 *
 * nuntium is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; version 3.
 *
 * nuntium is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package ofono

import (
	"encoding/hex"
	"fmt"
	"log"
	"sync"

	"github.com/godbus/dbus/v5"

	"gitlab.com/ubports/development/core/nuntium/mms"
)

/*
 in = "aya{sv}", out = ""
*/
type OfonoPushNotification struct {
	Data []byte
	Info map[string]*dbus.Variant
}

type PushAgent struct {
	conn       *dbus.Conn
	modem      dbus.ObjectPath
	Push       chan *PushPDU
	Registered bool
	m          sync.Mutex
}

func NewPushAgent(modem dbus.ObjectPath) *PushAgent {
	return &PushAgent{modem: modem}
}

func (agent *PushAgent) Register() (err error) {
	agent.m.Lock()
	defer agent.m.Unlock()
	if agent.conn == nil {
		if agent.conn, err = dbus.ConnectSystemBus(); err != nil {
			return err
		}
	}
	if agent.Registered {
		log.Printf("Agent already registered for %s", agent.modem)
		return nil
	}
	agent.Registered = true
	log.Print("Registering agent for ", agent.modem, " on path ", AGENT_TAG, " and name ", agent.conn.Names()[0])
	obj := agent.conn.Object("org.ofono", agent.modem)
	if c := obj.Call(PUSH_NOTIFICATION_INTERFACE + ".RegisterAgent", 0, AGENT_TAG); c.Err != nil {
		return fmt.Errorf("Cannot register agent for %s: %s", agent.modem, c.Err)
	}
	agent.Push = make(chan *PushPDU)

	methodTable := map[string]interface{}{
		"ReceiveNotification": agent.receiveNotification,
		"Release":             agent.release,
	}
	if err := agent.conn.ExportMethodTable(methodTable, AGENT_TAG, PUSH_NOTIFICATION_AGENT_INTERFACE); err != nil {
		panic(err)
	}

	log.Print("Agent Registered for ", agent.modem, " on path ", AGENT_TAG)
	return nil
}

func (agent *PushAgent) Unregister() error {
	agent.m.Lock()
	defer agent.m.Unlock()
	if !agent.Registered {
		log.Printf("Agent no registered for %s", agent.modem)
		return nil
	}
	log.Print("Unregistering agent on ", agent.modem)
	obj := agent.conn.Object("org.ofono", agent.modem)
	if c := obj.Call(PUSH_NOTIFICATION_INTERFACE + ".UnregisterAgent", 0, AGENT_TAG); c.Err != nil {
		log.Print("Unregister failed ", c.Err)
		return c.Err
	}
	agent.doRelease()
	agent.modem = dbus.ObjectPath("")
	return nil
}

func (agent *PushAgent) doRelease() {
	agent.Registered = false
	// remove export
	agent.conn.Export(nil, AGENT_TAG, PUSH_NOTIFICATION_AGENT_INTERFACE)
	close(agent.Push)
	agent.Push = nil
}

func (agent *PushAgent) receiveNotification(data []byte, info map[string]*dbus.Variant) *dbus.Error {
	push := &OfonoPushNotification{data, info}
	log.Print("Received ReceiveNotification() method call from ", push.Info["Sender"].Value())
	log.Print("Push data\n", hex.Dump(push.Data))
	dec := NewDecoder(push.Data)
	pdu := new(PushPDU)
	if err := dec.Decode(pdu); err != nil {
		log.Print("Error ", err)
		return dbus.NewError("org.freedesktop.DBus.Error", []interface{}{"DecodeError"})
	}
	// TODO later switch on ApplicationId and ContentType to different channels
	if pdu.ApplicationId == mms.PUSH_APPLICATION_ID && pdu.ContentType == mms.VND_WAP_MMS_MESSAGE {
		agent.Push <- pdu
	} else {
		log.Print("Unhandled push pdu", pdu)
	}
	return nil
}

func (agent *PushAgent) release() *dbus.Error {
	log.Printf("Push Agent on %s received Release", agent.modem)
	agent.doRelease()

	return nil
}
