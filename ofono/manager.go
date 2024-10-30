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
	"log"

	"github.com/godbus/dbus/v5"
)

type Modems map[dbus.ObjectPath]*Modem

type ModemManager struct {
	ModemAdded   chan (*Modem)
	ModemRemoved chan (*Modem)
	modems       Modems
	conn         *dbus.Conn
}

func NewModemManager(conn *dbus.Conn) *ModemManager {
	return &ModemManager{
		conn:         conn,
		ModemAdded:   make(chan *Modem),
		ModemRemoved: make(chan *Modem),
		modems:       make(Modems),
	}
}

func (mm *ModemManager) Init() error {
	//Use a different connection for the modem signals to avoid go-dbus blocking issues
	conn, err := dbus.ConnectSystemBus()
	if err != nil {
		return err
	}

	if err := conn.AddMatchSignal(
		dbus.WithMatchObjectPath("/"),
		dbus.WithMatchInterface(OFONO_MANAGER_INTERFACE),
		dbus.WithMatchMember("ModemAdded"),
		dbus.WithMatchSender(OFONO_SENDER),
		); err != nil {
		return err
	}
	if err := conn.AddMatchSignal(
		dbus.WithMatchObjectPath("/"),
		dbus.WithMatchInterface(OFONO_MANAGER_INTERFACE),
		dbus.WithMatchMember("ModemRemoved"),
		dbus.WithMatchSender(OFONO_SENDER),
		); err != nil {
		return err
	}
	signal := make(chan *dbus.Signal)
	go mm.watchModems(signal)
	conn.Signal(signal)

	//Check for existing modems
	modemPaths, err := getModems(conn)
	if err != nil {
		log.Print("Cannot preemptively add modems: ", err)
	} else {
		for _, objectPath := range modemPaths {
			mm.addModem(objectPath)
		}
	}
	return nil
}

func (mm *ModemManager) watchModems(signal chan *dbus.Signal) {
	const modemAdded = OFONO_MANAGER_INTERFACE + ".ModemAdded"
	const modemRemoved = OFONO_MANAGER_INTERFACE + ".ModemRemoved"

	for s := range signal {
		var objectPath dbus.ObjectPath
		if s.Sender == OFONO_SENDER && s.Name == modemAdded {
			var signalProps PropertiesType
			if err := dbus.Store(s.Body, &objectPath, &signalProps); err != nil {
				log.Print(err)
				continue
			}
			mm.addModem(objectPath)
		} else if s.Sender == OFONO_SENDER && s.Name == modemRemoved {
			if err := dbus.Store(s.Body, &objectPath); err != nil {
				log.Print(err)
				continue
			}
			mm.removeModem(objectPath)
		}
	}
}

func (mm *ModemManager) addModem(objectPath dbus.ObjectPath) {
	if modem, ok := mm.modems[objectPath]; ok {
		log.Printf("Need to delete stale modem instance %s", modem.Modem)
		modem.Delete()
		delete(mm.modems, objectPath)
	}
	mm.modems[objectPath] = NewModem(mm.conn, objectPath)
	mm.ModemAdded <- mm.modems[objectPath]
}

func (mm *ModemManager) removeModem(objectPath dbus.ObjectPath) {
	if modem, ok := mm.modems[objectPath]; ok {
		mm.ModemRemoved <- mm.modems[objectPath]
		log.Printf("Deleting modem instance %s", modem.Modem)
		modem.Delete()
		delete(mm.modems, objectPath)
	} else {
		log.Printf("Cannot satisfy request to remove modem %s as it does not exist", objectPath)
	}
}
