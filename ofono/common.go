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
	"github.com/godbus/dbus/v5"
)

const (
	DBUS_SERVICE                      = "org.freedesktop.DBus"
	DBUS_OBJECTPATH                   = dbus.ObjectPath("/org/freedesktop/DBus")
	DBUS_INTERFACE                    = "org.freedesktop.DBus"
	AGENT_TAG                         = dbus.ObjectPath("/nuntium")
	PUSH_NOTIFICATION_INTERFACE       = "org.ofono.PushNotification"
	PUSH_NOTIFICATION_AGENT_INTERFACE = "org.ofono.PushNotificationAgent"
	CONNECTION_MANAGER_INTERFACE      = "org.ofono.ConnectionManager"
	CONNECTION_CONTEXT_INTERFACE      = "org.ofono.ConnectionContext"
	SIM_MANAGER_INTERFACE             = "org.ofono.SimManager"
	OFONO_MANAGER_INTERFACE           = "org.ofono.Manager"
	OFONO_SENDER                      = "org.ofono"
	MODEM_INTERFACE                   = "org.ofono.Modem"
)

type PropertiesType map[string]dbus.Variant

func getModems(conn *dbus.Conn) (modemPaths []dbus.ObjectPath, err error) {
	modemsReply, err := getOfonoProps(conn, "/", OFONO_SENDER, "org.ofono.Manager", "GetModems")
	if err != nil {
		return nil, err
	}
	for _, modemReply := range modemsReply {
		modemPaths = append(modemPaths, modemReply.ObjectPath)
	}
	return modemPaths, nil
}
