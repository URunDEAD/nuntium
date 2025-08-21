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
	ModemAdded    chan (*Modem)
	ModemRemoved  chan (*Modem)
	modems        Modems
	conn          *dbus.Conn
	ofonoConnName string
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
		dbus.WithMatchSender(DBUS_SERVICE),
		dbus.WithMatchObjectPath(DBUS_OBJECTPATH),
		dbus.WithMatchInterface(DBUS_INTERFACE),
		dbus.WithMatchMember("NameOwnerChanged"),
		dbus.WithMatchArg(0, OFONO_SENDER),
	); err != nil {
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

	c := conn.BusObject().Call(DBUS_INTERFACE+".GetNameOwner", 0, OFONO_SENDER)
	if c.Err != nil {
		log.Print(
			"Cannot get current oFono connection name "+
				"(oFono may appear later):", err)
	} else {
		if err := dbus.Store(c.Body, &mm.ofonoConnName); err != nil {
			// DBus returns giberish?
			log.Print(
				"Cannot get current oFono connection name "+
					"(oFono may appear later):", err)
		}
	}

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
	const dbusNameOwnerChanged = DBUS_INTERFACE + ".NameOwnerChanged"
	const modemAdded = OFONO_MANAGER_INTERFACE + ".ModemAdded"
	const modemRemoved = OFONO_MANAGER_INTERFACE + ".ModemRemoved"

	for s := range signal {
		var name, oldOwner, newOwner string
		var objectPath dbus.ObjectPath

		// DBUS_SERVICE is the only well-known name included in signals.
		if s.Sender == DBUS_SERVICE && s.Name == dbusNameOwnerChanged {
			if err := dbus.Store(s.Body, &name, &oldOwner, &newOwner); err != nil {
				log.Print(err)
				continue
			}
			if name != OFONO_SENDER || newOwner == mm.ofonoConnName {
				continue
			}
			mm.ofonoConnName = newOwner

			// Either oFono has died or new instance has been respawned.
			// Either ways, any old modems we have is now invalid. Remove
			// them all. For respawned case, oFono will proceed to emit
			// ModemAdded signal so we'll learn new modems that way.
			mm.removeAllModems()
		} else if s.Sender == mm.ofonoConnName && s.Name == modemAdded {
			var signalProps PropertiesType
			if err := dbus.Store(s.Body, &objectPath, &signalProps); err != nil {
				log.Print(err)
				continue
			}
			mm.addModem(objectPath)
		} else if s.Sender == mm.ofonoConnName && s.Name == modemRemoved {
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
	mm.modems[objectPath] = NewModem(mm.conn, mm.ofonoConnName, objectPath)
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

func (mm *ModemManager) removeAllModems() {
	for _, modem := range mm.modems {
		mm.ModemRemoved <- modem
		log.Printf("Deleting modem instance %s", modem.Modem)
		modem.Delete()
	}

	mm.modems = make(Modems)
}
