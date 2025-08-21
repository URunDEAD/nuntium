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
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/godbus/dbus/v5"
)

const (
	contextTypeInternet = "internet"
	contextTypeMMS      = "mms"
)

const (
	ofonoAttachInProgressError = "org.ofono.Error.AttachInProgress"
	ofonoInProgressError       = "org.ofono.Error.InProgress"
	ofonoNotAttachedError      = "org.ofono.Error.NotAttached"
	ofonoFailedError           = "org.ofono.Error.Failed"
)

type OfonoContext struct {
	ObjectPath dbus.ObjectPath
	Properties PropertiesType
}

type Modem struct {
	conn                   *dbus.Conn
	ofonoConnName          string
	Modem                  dbus.ObjectPath
	PushAgent              *PushAgent
	identity               string
	IdentityAdded          chan string
	IdentityRemoved        chan string
	endWatch               chan bool
	PushInterfaceAvailable chan bool
	pushInterfaceAvailable bool
	online                 bool
	signal                 chan *dbus.Signal
}

type ProxyInfo struct {
	Host string
	Port uint64
}

const PROP_SETTINGS = "Settings"
const SETTINGS_PROXY = "Proxy"
const SETTINGS_PROXYPORT = "ProxyPort"
const DBUS_CALL_GET_PROPERTIES = "GetProperties"

func (p ProxyInfo) String() string {
	return fmt.Sprintf("%s:%d", p.Host, p.Port)
}

func (oProp OfonoContext) String() string {
	var s string
	s += fmt.Sprintf("ObjectPath: %s\n", oProp.ObjectPath)
	for k, v := range oProp.Properties {
		s += fmt.Sprint("\t", k, ": ", v.Value(), "\n")
	}
	return s
}

func NewModem(conn *dbus.Conn, ofonoConnName string, objectPath dbus.ObjectPath) *Modem {
	return &Modem{
		conn:                   conn,
		ofonoConnName:          ofonoConnName,
		Modem:                  objectPath,
		IdentityAdded:          make(chan string),
		IdentityRemoved:        make(chan string),
		PushInterfaceAvailable: make(chan bool),
		endWatch:               make(chan bool),
		PushAgent:              NewPushAgent(objectPath),
		signal:                 make(chan *dbus.Signal),
	}
}

func (modem *Modem) Init() (err error) {
	log.Printf("Initializing modem %s", modem.Modem)
	if err := modem.conn.AddMatchSignal(
		dbus.WithMatchObjectPath(modem.Modem),
		dbus.WithMatchInterface(MODEM_INTERFACE),
		dbus.WithMatchMember("PropertyChanged"),
		dbus.WithMatchSender(OFONO_SENDER),
		); err != nil {
		return err
	}

	if err := modem.conn.AddMatchSignal(
		dbus.WithMatchObjectPath(modem.Modem),
		dbus.WithMatchInterface(SIM_MANAGER_INTERFACE),
		dbus.WithMatchMember("PropertyChanged"),
		dbus.WithMatchSender(OFONO_SENDER),
		); err != nil {
		return err
	}

	// the calling order here avoids race conditions
	go modem.watchStatus()
	modem.conn.Signal(modem.signal)
	modem.fetchExistingStatus()

	return nil
}

// fetchExistingStatus fetches key required for the modem to be considered operational
// from a push notification point of view
//
// status updates are fetched through dbus method calls
func (modem *Modem) fetchExistingStatus() {
	if v, err := modem.getProperty(MODEM_INTERFACE, "Interfaces"); err == nil {
		modem.updatePushInterfaceState(*v)
	} else {
		log.Print("Initial value couldn't be retrieved: ", err)
	}
	if v, err := modem.getProperty(MODEM_INTERFACE, "Online"); err == nil {
		modem.handleOnlineState(*v)
	} else {
		log.Print("Initial value couldn't be retrieved: ", err)
	}
	if v, err := modem.getProperty(SIM_MANAGER_INTERFACE, "SubscriberIdentity"); err == nil {
		modem.handleIdentity(*v)
	}
}

// watchStatus monitors key states required for the modem to be considered operational
// from a push notification point of view
//
// status updates are monitered by hooking up to dbus signals
func (modem *Modem) watchStatus() {
	const modemChanged = MODEM_INTERFACE + ".PropertyChanged"
	const simChanged = SIM_MANAGER_INTERFACE + ".PropertyChanged"
	var propName string
	var propValue dbus.Variant
watchloop:
	for {
		select {
		case <-modem.endWatch:
			log.Printf("Ending modem watch for %s", modem.Modem)
			break watchloop
		case s := <-modem.signal:
			if s.Sender == modem.ofonoConnName && s.Path == modem.Modem && s.Name == modemChanged {
				if err := dbus.Store(s.Body, &propName, &propValue); err != nil {
					log.Printf("Cannot interpret Modem Property change: %s", err)
					continue watchloop
				}
				switch propName {
				case "Interfaces":
					modem.updatePushInterfaceState(propValue)
				case "Online":
					modem.handleOnlineState(propValue)
				default:
					continue watchloop
				}
			} else if s.Sender == modem.ofonoConnName && s.Path == modem.Modem && s.Name == simChanged {
				if err := dbus.Store(s.Body, &propName, &propValue); err != nil {
					log.Printf("Cannot interpret Sim Property change: %s", err)
					continue watchloop
				}
				switch propName {
				case "SubscriberIdentity":
					modem.handleIdentity(propValue)
				case "Present":
					modem.handlePresent(propValue)
				default:
					continue watchloop
				}
			}
		}
	}
}

func (modem *Modem) handleOnlineState(propValue dbus.Variant) {
	origState := modem.online
	modem.online = reflect.ValueOf(propValue.Value()).Bool()
	if modem.online != origState {
		log.Printf("Modem online: %t", modem.online)
	}
}

func (modem *Modem) handleIdentity(propValue dbus.Variant) {
	identity := reflect.ValueOf(propValue.Value()).String()
	if identity == "" && modem.identity != "" {
		log.Printf("Identity before remove %s", modem.identity)

		modem.IdentityRemoved <- modem.identity
		modem.identity = identity
	}
	log.Printf("Identity added %s", identity)
	if identity != "" && modem.identity == "" {
		modem.identity = identity
		modem.IdentityAdded <- identity
	}
}

func (modem *Modem) handlePresent(propValue dbus.Variant) {
	present := reflect.ValueOf(propValue.Value()).Bool()

	// When a SIM card is removed, some (all?) versions of oFono will send
	// only PropertyChanged event of "Present" property. Thus we have to
	// also handle this case as well.
	if !present && modem.identity != "" {
		log.Printf("SIM with identity %s removed", modem.identity)
		modem.IdentityRemoved <- modem.identity
		modem.identity = ""
	}
}

func (modem *Modem) updatePushInterfaceState(interfaces dbus.Variant) {
	nextState := false
	availableInterfaces := reflect.ValueOf(interfaces.Value())
	for i := 0; i < availableInterfaces.Len(); i++ {
		interfaceName := reflect.ValueOf(availableInterfaces.Index(i).Interface().(string)).String()
		if interfaceName == PUSH_NOTIFICATION_INTERFACE {
			nextState = true
			break
		}
	}
	if modem.pushInterfaceAvailable != nextState {
		modem.pushInterfaceAvailable = nextState
		log.Printf("Push interface state: %t", modem.pushInterfaceAvailable)
		if modem.pushInterfaceAvailable {
			modem.PushInterfaceAvailable <- true
		} else if modem.PushAgent.Registered {
			modem.PushInterfaceAvailable <- false
		}
	}
}

var getOfonoProps = func(conn *dbus.Conn, objectPath dbus.ObjectPath, destination, iface, method string) (oProps []OfonoContext, err error) {
	err = conn.Object(destination, objectPath).Call(iface + "." + method, 0).Store(&oProps)
	return
}

//ActivateMMSContext activates a context if necessary and returns the context
//to operate with MMS.
//
//If the context is already active it's a nop.
//Returns either the type=internet context or the type=mms, if none is found
//an error is returned.
func (modem *Modem) ActivateMMSContext(preferredContext dbus.ObjectPath) (OfonoContext, error) {
	contexts, err := modem.GetMMSContexts(preferredContext)
	if err != nil {
		return OfonoContext{}, err
	}
	for _, context := range contexts {
		if context.isActive() {
			return context, nil
		}
		if err := context.toggleActive(true, modem.conn); err == nil {
			return context, nil
		} else {
			log.Println("Failed to activate for", context.ObjectPath, ":", err)
		}
	}
	return OfonoContext{}, errors.New("no context available to activate")
}

//DeactivateMMSContext deactivates the context if it is of type mms
func (modem *Modem) DeactivateMMSContext(context OfonoContext) error {
	if context.isTypeInternet() {
		return nil
	}

	return context.toggleActive(false, modem.conn)
}

func activationErrorNeedsWait(err error) bool {
	// ofonoFailedError might be due to network issues or to wrong APN configuration.
	// Retrying would not make sense for the latter, but we cannot distinguish
	// and any possible delay retrying might cause would happen only the first time
	// (provided we end up finding the right APN on the list so we save it as
	// preferred).
	if dbusErr, ok := err.(*dbus.Error); ok {
		return dbusErr.Name == ofonoInProgressError ||
			dbusErr.Name == ofonoAttachInProgressError ||
			dbusErr.Name == ofonoNotAttachedError ||
			dbusErr.Name == ofonoFailedError
	}
	return false
}

func (context *OfonoContext) getContextProperties(conn *dbus.Conn) {
	ctxObj := conn.Object(OFONO_SENDER, context.ObjectPath)
	if err := ctxObj.Call(CONNECTION_CONTEXT_INTERFACE + "." + DBUS_CALL_GET_PROPERTIES, 0).Store(&context.Properties); err == nil {
		log.Println("Cannot get properties for", context.ObjectPath, err)
	}
}

func (context *OfonoContext) toggleActive(state bool, conn *dbus.Conn) error {
	log.Println("Trying to set Active property to", state, "for context on", state, context.ObjectPath)
	obj := conn.Object("org.ofono", context.ObjectPath)
	for i := 0; i < 3; i++ {
		c := obj.Call(CONNECTION_CONTEXT_INTERFACE + ".SetProperty", 0, "Active", dbus.MakeVariant(state))
		if c.Err != nil {
			log.Printf("Cannot set Activate to %t (try %d/3) interface on %s: %s", state, i+1, context.ObjectPath, c.Err)
			if activationErrorNeedsWait(c.Err) {
				time.Sleep(2 * time.Second)
			}
		} else {
			// If it works we set it as preferred in ofono, provided it is not
			// a combined context.
			// TODO get rid of nuntium's internal preferred setting
			if !context.isPreferred() && context.isTypeMMS() {
				obj.Call(CONNECTION_CONTEXT_INTERFACE + ".SetProperty",
					0, "Preferred", dbus.MakeVariant(true))
			}
			// Refresh context properties
			context.getContextProperties(conn)
			return nil
		}
	}
	return errors.New("failed to activate context")
}

func (oContext OfonoContext) isTypeInternet() bool {
	if v, ok := oContext.Properties["Type"]; ok {
		return reflect.ValueOf(v.Value()).String() == contextTypeInternet
	}
	return false
}

func (oContext OfonoContext) isTypeMMS() bool {
	if v, ok := oContext.Properties["Type"]; ok {
		return reflect.ValueOf(v.Value()).String() == contextTypeMMS
	}
	return false
}

func (oContext OfonoContext) isActive() bool {
	return reflect.ValueOf(oContext.Properties["Active"].Value()).Bool()
}

func (oContext OfonoContext) isPreferred() bool {
	return reflect.ValueOf(oContext.Properties["Preferred"].Value()).Bool()
}

func (oContext OfonoContext) hasMessageCenter() bool {
	return oContext.messageCenter() != ""
}

func (oContext OfonoContext) messageCenter() string {
	if v, ok := oContext.Properties["MessageCenter"]; ok {
		return reflect.ValueOf(v.Value()).String()
	}
	return ""
}

func (oContext OfonoContext) messageProxy() string {
	if v, ok := oContext.Properties["MessageProxy"]; ok {
		return reflect.ValueOf(v.Value()).String()
	}
	return ""
}

func (oContext OfonoContext) name() string {
	if v, ok := oContext.Properties["Name"]; ok {
		return reflect.ValueOf(v.Value()).String()
	}
	return ""
}

func (oContext OfonoContext) settingsProxy() string {
	v, ok := oContext.Properties[PROP_SETTINGS]
	if !ok {
		return ""
	}

	settings, ok := v.Value().(map[string]dbus.Variant)
	if !ok {
		return ""
	}

	proxy_v, ok := settings[SETTINGS_PROXY]
	if !ok {
		return ""
	}

	proxy, ok := proxy_v.Value().(string)
	if !ok {
		return ""
	}

	return proxy
}

func (oContext OfonoContext) settingsProxyPort() uint64 {
	v, ok := oContext.Properties[PROP_SETTINGS]
	if !ok {
		return 80
	}

	settings, ok := v.Value().(map[string]dbus.Variant)
	if !ok {
		return 80
	}

	port_v, ok := settings[SETTINGS_PROXYPORT]
	if !ok {
		return 80
	}

	port, ok := port_v.Value().(uint16)
	if !ok {
		return 80
	}

	return uint64(port)
}

func (oContext OfonoContext) GetMessageCenter() (string, error) {
	if oContext.hasMessageCenter() {
		return oContext.messageCenter(), nil
	} else {
		return "", errors.New("context setting for the Message Center value is empty")
	}
}

func (oContext OfonoContext) GetProxy() (proxyInfo ProxyInfo, err error) {
	proxy := oContext.settingsProxy()
	// we need to support empty proxies
	if proxy == "" {
		log.Println("No proxy in ofono settings")
		return proxyInfo, nil
	}

	if strings.Contains(proxy, ":") {
		v := strings.Split(proxy, ":")
		host, port_str := v[0], v[1]
		port, err := strconv.ParseUint(port_str, 10, 16)
		if err != nil {
			port = 80
		}

		proxyInfo.Host = host
		proxyInfo.Port = uint64(port)
		return proxyInfo, nil
	}

	proxyInfo.Host = proxy
	proxyInfo.Port = oContext.settingsProxyPort()

	return proxyInfo, nil
}

//GetMMSContexts returns the contexts that are MMS capable; by convention it has
//been defined that for it to be MMS capable it either has to define a MessageProxy
//and a MessageCenter within the context.
//
//The following rules take place:
//- if current type=internet context, check for MessageProxy & MessageCenter;
//  if they exist and aren't empty AND the context is active, add it to the list
//- if current type=mms, add it to the list
//- if ofono's ConnectionManager.Preferred property is set, use only that context
//- prioritize active and recently successfully used contexts
//
//Returns either the type=internet context or the type=mms, if none is found
//an error is returned.
func (modem *Modem) GetMMSContexts(preferredContext dbus.ObjectPath) (mmsContexts []OfonoContext, err error) {
	contexts, err := getOfonoProps(modem.conn, modem.Modem, OFONO_SENDER, CONNECTION_MANAGER_INTERFACE, "GetContexts")
	if err != nil {
		return mmsContexts, err
	}

	for _, context := range contexts {
		if (context.isTypeInternet() && context.isActive() && context.hasMessageCenter()) || context.isTypeMMS() {
			if context.isPreferred() {
				mmsContexts = []OfonoContext{context}
				break
			} else if context.ObjectPath == preferredContext || context.isActive() {
				mmsContexts = append([]OfonoContext{context}, mmsContexts...)
			} else {
				mmsContexts = append(mmsContexts, context)
			}
		}
	}
	if len(mmsContexts) == 0 {
		log.Printf("non matching contexts:\n %+v", contexts)
		return mmsContexts, errors.New("No mms contexts found")
	}
	return mmsContexts, nil
}

func (modem *Modem) getProperty(interfaceName, propertyName string) (*dbus.Variant, error) {
	errorString := "Cannot retrieve %s from %s for %s: %s"
	rilObj := modem.conn.Object(OFONO_SENDER, modem.Modem)
	var property PropertiesType
	if err := rilObj.Call(interfaceName + "." + DBUS_CALL_GET_PROPERTIES, 0).Store(&property); err != nil {
		return nil, fmt.Errorf(errorString, propertyName, interfaceName, modem.Modem, err)
	}
	v, ok := property[propertyName]
	if !ok {
		return nil, fmt.Errorf(errorString, propertyName, interfaceName, modem.Modem, "property not found")
	}
	return &v, nil
}

func (modem *Modem) Delete() {
	defer func(){ modem.endWatch <- true }()

	if modem.identity != "" {
		modem.IdentityRemoved <- modem.identity
	}

	if err := modem.conn.RemoveMatchSignal(
		dbus.WithMatchObjectPath(modem.Modem),
		dbus.WithMatchInterface(MODEM_INTERFACE),
		dbus.WithMatchMember("PropertyChanged"),
		dbus.WithMatchSender(OFONO_SENDER),
		); err != nil {
		return
	}

	if err := modem.conn.RemoveMatchSignal(
		dbus.WithMatchObjectPath(modem.Modem),
		dbus.WithMatchInterface(SIM_MANAGER_INTERFACE),
		dbus.WithMatchMember("PropertyChanged"),
		dbus.WithMatchSender(OFONO_SENDER),
		); err != nil {
		return
	}

	modem.conn.RemoveSignal(modem.signal)
	close(modem.signal)
}

func (modem *Modem) Identity() string {
	return modem.identity
}
