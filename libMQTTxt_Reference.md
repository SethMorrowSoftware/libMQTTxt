# MQTT Client Library Reference

Version 2.11.8 - OXT MQTT 3.1.1 Implementation

## Table of Contents

- [Installation](#installation)
- [Configuration Functions](#configuration-functions)
- [Connection Management](#connection-management)
- [Publishing](#publishing)
- [Subscribing](#subscribing)
- [Utility Functions](#utility-functions)
- [Callbacks](#callbacks)
- [Error Handling](#error-handling)
- [Code Examples](#code-examples)

---

## Installation

Load the library in your stack script:

```OXT
on preOpenStack
   start using stack "script_only_stack_"
end preOpenStack
```

The library must be in `stacksInUse` for socket callbacks to function correctly.

---

## Configuration Functions

### mqttSetCallbackTarget

Set the target object for all callbacks.

```OXT
mqttSetCallbackTarget pTarget
```

**Parameters:**
- `pTarget` - Long ID of target object (typically `the long id of this card`)

**Example:**
```OXT
mqttSetCallbackTarget the long id of this card
```

---

### mqttSetMessageCallback

Define the handler name for incoming messages.

```OXT
mqttSetMessageCallback pHandlerName
```

**Parameters:**
- `pHandlerName` - Name of message handler in callback target

**Example:**
```OXT
mqttSetMessageCallback "onMQTTMessage"

-- Handler in callback target:
on onMQTTMessage pTopic, pMessage
   put "Received:" && pTopic && pMessage
end onMQTTMessage
```

---

### mqttSetLogCallback

Define the handler name for log messages.

```OXT
mqttSetLogCallback pHandlerName
```

**Parameters:**
- `pHandlerName` - Name of log handler in callback target

**Example:**
```OXT
mqttSetLogCallback "onMQTTLog"

on onMQTTLog pMessage
   put pMessage & return after field "Log"
end onMQTTLog
```

---

### mqttSetStateChangeCallback

Define the handler name for connection state changes.

```OXT
mqttSetStateChangeCallback pHandlerName
```

**Parameters:**
- `pHandlerName` - Name of state change handler

**Example:**
```OXT
mqttSetStateChangeCallback "onStateChange"

on onStateChange pState, pHost, pPort, pReason
   put pState && pHost && pPort && pReason
end onStateChange
```

**States:** `connecting`, `connected`, `disconnecting`, `disconnected`, `error`

---

### mqttSetReconnectCallback

Define the handler name for reconnection events.

```OXT
mqttSetReconnectCallback pHandlerName
```

**Parameters:**
- `pHandlerName` - Name of reconnect handler

**Example:**
```OXT
mqttSetReconnectCallback "onReconnect"

on onReconnect pEvent, pHost, pPort, pAttempts
   put pEvent && "attempt" && pAttempts
end onReconnect
```

**Events:** `attempting`, `success`, `failed`

---

### mqttSetDebugMode

Enable or disable debug logging.

```OXT
mqttSetDebugMode pDebug
```

**Parameters:**
- `pDebug` - Boolean (true/false)

**Example:**
```OXT
mqttSetDebugMode true
```

---

### mqttSetQuietMode

Suppress all logging output.

```OXT
mqttSetQuietMode pQuiet
```

**Parameters:**
- `pQuiet` - Boolean (true/false)

**Example:**
```OXT
mqttSetQuietMode false
```

---

### mqttSetKeepAliveThreshold

Set when to send PINGREQ as percentage of keep-alive interval.

```OXT
mqttSetKeepAliveThreshold pThreshold
```

**Parameters:**
- `pThreshold` - Decimal between 0 and 1 (default 0.75 = 75%)

**Example:**
```OXT
mqttSetKeepAliveThreshold 0.75
```

---

### mqttGetKeepAliveThreshold

Get current keep-alive threshold.

```OXT
function mqttGetKeepAliveThreshold()
```

**Returns:** Decimal between 0 and 1

**Example:**
```OXT
put mqttGetKeepAliveThreshold() into tThreshold
```

---

### mqttSetMaxBufferSize

Set maximum receive buffer size in bytes.

```OXT
mqttSetMaxBufferSize pBytes
```

**Parameters:**
- `pBytes` - Integer (minimum 65536, maximum 268435455)

**Example:**
```OXT
mqttSetMaxBufferSize 5242880  -- 5MB
```

---

### mqttSetPersistentStore

Enable persistent storage for QoS 1/2 messages.

```OXT
mqttSetPersistentStore pEnabled, pStorePath
```

**Parameters:**
- `pEnabled` - Boolean (true/false)
- `pStorePath` - Optional file path (default: documents/mqtt_store)

**Example:**
```OXT
mqttSetPersistentStore true, specialFolderPath("documents") & "/mqtt"
```

---

## Connection Management

### mqttConnect

Establish connection to MQTT broker.

```OXT
function mqttConnect(pHost, pPort, pClientID, pUsername, pPassword, \
                     pKeepAlive, pUseTLS, pCleanSession, pLWTTopic, \
                     pLWTMessage, pLWTQoS, pLWTRetain, pVerifyTLS, \
                     pAutoReconnect, pCACertPath)
```

**Parameters:**
- `pHost` - Broker hostname or IP
- `pPort` - Broker port (usually 1883 or 8883 for TLS)
- `pClientID` - Unique client identifier
- `pUsername` - Username (empty string if none)
- `pPassword` - Password (empty string if none)
- `pKeepAlive` - Keep-alive interval in seconds (default 60)
- `pUseTLS` - Boolean, enable TLS encryption (default false)
- `pCleanSession` - Boolean, start clean session (default true)
- `pLWTTopic` - Last Will Testament topic (empty if none)
- `pLWTMessage` - Last Will Testament message
- `pLWTQoS` - Last Will Testament QoS (0, 1, or 2)
- `pLWTRetain` - Last Will Testament retain flag
- `pVerifyTLS` - Boolean, verify TLS certificates (default false)
- `pAutoReconnect` - Boolean, enable automatic reconnection (default false)
- `pCACertPath` - Path to CA certificate file

**Returns:** "OK" on success, "ERROR: message" on failure

**Example:**
```OXT
put mqttConnect("broker.example.com", 8883, "client123", \
                "user", "pass", 60, true, true, "", "", \
                0, false, false, true, "") into tResult

if tResult is "OK" then
   -- Connection initiated
else
   answer "Connection failed:" && tResult
end if
```

---

### mqttConnectSimple

Simplified connection with common defaults.

```OXT
function mqttConnectSimple(pHost, pPort, pClientID, pUsername, pPassword)
```

**Parameters:**
- `pHost` - Broker hostname
- `pPort` - Broker port
- `pClientID` - Client ID
- `pUsername` - Username
- `pPassword` - Password

**Returns:** "OK" or "ERROR: message"

**Example:**
```OXT
put mqttConnectSimple("broker.hivemq.com", 1883, "myClient", "", "") into tResult
```

This uses defaults: keep-alive 60s, no TLS, clean session, no LWT, no auto-reconnect.

---

### mqttIsConnected

Check if connection is active.

```OXT
function mqttIsConnected(pHost, pPort)
```

**Parameters:**
- `pHost` - Broker hostname
- `pPort` - Broker port

**Returns:** Boolean (true/false)

**Example:**
```OXT
if mqttIsConnected("broker.example.com", 1883) then
   put "Connected"
else
   put "Not connected"
end if
```

---

### mqttReconnect

Manually reconnect to broker.

```OXT
function mqttReconnect(pHost, pPort)
```

**Parameters:**
- `pHost` - Broker hostname
- `pPort` - Broker port

**Returns:** "OK" or "ERROR: message"

**Example:**
```OXT
put mqttReconnect("broker.example.com", 1883) into tResult
```

---

### mqttDisconnect

Disconnect from broker.

```OXT
command mqttDisconnect pHost, pPort
```

**Parameters:**
- `pHost` - Broker hostname
- `pPort` - Broker port

**Example:**
```OXT
mqttDisconnect "broker.example.com", 1883
```

---

### mqttCleanupAll

Disconnect all connections and cleanup resources.

```OXT
command mqttCleanupAll
```

**Example:**
```OXT
on closeStack
   mqttCleanupAll
end closeStack
```

---

## Publishing

### mqttPublish

Publish a message to a topic.

```OXT
function mqttPublish(pHost, pPort, pTopic, pMessage, pQoS, pRetain)
```

**Parameters:**
- `pHost` - Broker hostname
- `pPort` - Broker port
- `pTopic` - Topic string (UTF-8, no wildcards)
- `pMessage` - Message payload (binary safe)
- `pQoS` - Quality of Service (0, 1, or 2)
- `pRetain` - Boolean, retain message on broker

**Returns:** "OK" or "ERROR: message"

**QoS Levels:**
- 0: At most once (fire and forget)
- 1: At least once (acknowledged)
- 2: Exactly once (assured delivery)

**Example:**
```OXT
-- QoS 0 message
put mqttPublish("broker.example.com", 1883, "sensor/temp", "23.5", 0, false) into tResult

-- QoS 1 with acknowledgment
put mqttPublish("broker.example.com", 1883, "alert/status", "critical", 1, false) into tResult

-- Retained message
put mqttPublish("broker.example.com", 1883, "device/status", "online", 0, true) into tResult
```

**Topic Restrictions:**
- Cannot be empty
- Cannot contain wildcard characters (+ or #)
- Maximum length depends on broker (typically 256 characters)

---

## Subscribing

### mqttSubscribe

Subscribe to a topic.

```OXT
function mqttSubscribe(pHost, pPort, pTopic, pQoS)
```

**Parameters:**
- `pHost` - Broker hostname
- `pPort` - Broker port
- `pTopic` - Topic filter (may include wildcards)
- `pQoS` - Maximum QoS level (0, 1, or 2)

**Returns:** "OK" or "ERROR: message"

**Wildcards:**
- `+` - Single level wildcard (e.g., `sensor/+/temp`)
- `#` - Multi-level wildcard (e.g., `sensor/#`)

**Example:**
```OXT
-- Subscribe to specific topic
put mqttSubscribe("broker.example.com", 1883, "sensor/temp", 0) into tResult

-- Subscribe with single-level wildcard
put mqttSubscribe("broker.example.com", 1883, "sensor/+/temp", 0) into tResult

-- Subscribe with multi-level wildcard
put mqttSubscribe("broker.example.com", 1883, "sensor/#", 1) into tResult
```

---

### mqttUnsubscribe

Unsubscribe from a topic.

```OXT
function mqttUnsubscribe(pHost, pPort, pTopic)
```

**Parameters:**
- `pHost` - Broker hostname
- `pPort` - Broker port
- `pTopic` - Topic filter (must match subscription)

**Returns:** "OK" or "ERROR: message"

**Example:**
```OXT
put mqttUnsubscribe("broker.example.com", 1883, "sensor/temp") into tResult
```

---

### mqttGetSubscriptions

Get list of active subscriptions.

```OXT
function mqttGetSubscriptions(pHost, pPort)
```

**Parameters:**
- `pHost` - Broker hostname
- `pPort` - Broker port

**Returns:** Line-delimited list of subscribed topics

**Example:**
```OXT
put mqttGetSubscriptions("broker.example.com", 1883) into tSubs
repeat for each line tTopic in tSubs
   put tTopic & return after field "Subscriptions"
end repeat
```

---

## Utility Functions

### mqttGetConnectionInfo

Get detailed connection information.

```OXT
function mqttGetConnectionInfo(pHost, pPort)
```

**Parameters:**
- `pHost` - Broker hostname
- `pPort` - Broker port

**Returns:** Array containing connection details

**Array Keys:**
- `host` - Broker hostname
- `port` - Broker port
- `clientID` - Client identifier
- `connected` - Boolean connection state
- `keepAlive` - Keep-alive interval
- `useTLS` - TLS enabled flag
- `sessionPresent` - Session persistence flag
- `autoReconnect` - Auto-reconnect enabled flag
- `reconnectAttempts` - Number of reconnection attempts

**Example:**
```OXT
put mqttGetConnectionInfo("broker.example.com", 1883) into tInfo
put tInfo["clientID"]  -- Access specific field
put tInfo["connected"]
```

---

### mqttGetStatistics

Get connection statistics.

```OXT
function mqttGetStatistics(pHost, pPort)
```

**Parameters:**
- `pHost` - Broker hostname
- `pPort` - Broker port

**Returns:** Array containing statistics

**Array Keys:**
- `messagesSent` - Total messages published
- `messagesReceived` - Total messages received
- `bytesSent` - Total bytes transmitted
- `bytesReceived` - Total bytes received
- `reconnections` - Number of reconnections

**Example:**
```OXT
put mqttGetStatistics("broker.example.com", 1883) into tStats
put "Sent:" && tStats["messagesSent"]
put "Received:" && tStats["messagesReceived"]
put "Data sent:" && tStats["bytesSent"] && "bytes"
```

---

### mqttResetStatistics

Reset statistics counters.

```OXT
command mqttResetStatistics pHost, pPort
```

**Parameters:**
- `pHost` - Broker hostname
- `pPort` - Broker port

**Example:**
```OXT
mqttResetStatistics "broker.example.com", 1883
```

---

### mqttGetSessionPresent

Check if broker has persistent session.

```OXT
function mqttGetSessionPresent(pHost, pPort)
```

**Parameters:**
- `pHost` - Broker hostname
- `pPort` - Broker port

**Returns:** Boolean (true/false)

**Example:**
```OXT
if mqttGetSessionPresent("broker.example.com", 1883) then
   put "Previous session restored"
end if
```

---

### mqttGetConnections

Get list of all active connections.

```OXT
function mqttGetConnections()
```

**Returns:** Line-delimited list of connections in "host:port" format

**Example:**
```OXT
put mqttGetConnections() into tConns
repeat for each line tConn in tConns
   put tConn & return after field "Connections"
end repeat
```

---

### mqttTestLibrary

Verify library is loaded.

```OXT
function mqttTestLibrary()
```

**Returns:** Version string

**Example:**
```OXT
put mqttTestLibrary()
-- Returns: "MQTT Library v2.11.8 loaded successfully (Test Suite 100% Compliance)"
```

---

### mqttSelfTest

Run internal library self-tests.

```OXT
function mqttSelfTest()
```

**Returns:** Test results string

**Example:**
```OXT
put mqttSelfTest() into field "TestResults"
```

---

### mqttBenchmark

Performance benchmark test.

```OXT
function mqttBenchmark(pHost, pPort, pMessageCount, pMessageSize)
```

**Parameters:**
- `pHost` - Broker hostname
- `pPort` - Broker port
- `pMessageCount` - Number of messages to send (default 1000)
- `pMessageSize` - Size of each message in bytes (default 100)

**Returns:** Formatted benchmark results

**Example:**
```OXT
put mqttBenchmark("broker.example.com", 1883, 1000, 100) into tResults
put tResults
```

---

## Callbacks

All callback handlers must be defined in the callback target object.

### Message Callback

Invoked when a message is received.

```OXT
on handlerName pTopic, pMessage
   -- pTopic: UTF-8 topic string
   -- pMessage: Binary-safe message payload
end handlerName
```

**Example:**
```OXT
mqttSetMessageCallback "onMessage"

on onMessage pTopic, pMessage
   switch pTopic
      case "sensor/temp"
         put pMessage into field "Temperature"
         break
      case "sensor/humidity"
         put pMessage into field "Humidity"
         break
   end switch
end onMessage
```

---

### Log Callback

Invoked for log messages.

```OXT
on handlerName pMessage
   -- pMessage: Log message string
end handlerName
```

**Example:**
```OXT
mqttSetLogCallback "onLog"

on onLog pMessage
   put pMessage & return after field "Log"
end onLog
```

---

### State Change Callback

Invoked when connection state changes.

```OXT
on handlerName pState, pHost, pPort, pReason
   -- pState: connecting, connected, disconnecting, disconnected, error
   -- pHost: Broker hostname
   -- pPort: Broker port
   -- pReason: State change reason (may be empty)
end handlerName
```

**Example:**
```OXT
mqttSetStateChangeCallback "onStateChange"

on onStateChange pState, pHost, pPort, pReason
   if pState is "connected" then
      put "Connected to" && pHost
   else if pState is "disconnected" then
      put "Disconnected:" && pReason
   end if
end onStateChange
```

---

### Reconnect Callback

Invoked during reconnection attempts.

```OXT
on handlerName pEvent, pHost, pPort, pAttempts
   -- pEvent: attempting, success, failed
   -- pHost: Broker hostname
   -- pPort: Broker port
   -- pAttempts: Reconnection attempt number
end handlerName
```

**Example:**
```OXT
mqttSetReconnectCallback "onReconnect"

on onReconnect pEvent, pHost, pPort, pAttempts
   if pEvent is "attempting" then
      put "Reconnect attempt" && pAttempts
   else if pEvent is "failed" then
      put "Reconnection failed after" && pAttempts && "attempts"
   end if
end onReconnect
```

---

## Error Handling

All connection and messaging functions return either "OK" or "ERROR: description".

**Common Error Messages:**
- `ERROR: Not connected` - Operation requires active connection
- `ERROR: Invalid QoS (use 0, 1, or 2)` - QoS value out of range
- `ERROR: Topic cannot be empty` - Empty topic string provided
- `ERROR: Topic cannot contain wildcards` - Wildcards in publish topic
- `ERROR: Socket not open` - Socket connection lost
- `ERROR: Buffer overflow` - Receive buffer exceeded limit

**Example Error Handling:**
```OXT
put mqttPublish("broker.example.com", 1883, "test", "msg", 0, false) into tResult
if tResult is not "OK" then
   answer "Publish failed:" && tResult
   exit to top
end if
```

---

## Code Examples

### Basic Connection and Publish

```OXT
on mouseUp
   -- Configure callbacks
   mqttSetCallbackTarget the long id of this card
   mqttSetMessageCallback "onMessage"
   mqttSetLogCallback "onLog"
   
   -- Connect to broker
   put mqttConnect("broker.hivemq.com", 1883, "myClient", "", "", \
                   60, false, true, "", "", 0, false, false, false, "") into tResult
   
   if tResult is not "OK" then
      answer "Connection failed:" && tResult
      exit mouseUp
   end if
   
   -- Wait for connection
   wait until mqttIsConnected("broker.hivemq.com", 1883) with messages
   
   -- Publish message
   put mqttPublish("broker.hivemq.com", 1883, "test/topic", "Hello MQTT", 0, false) into tResult
end mouseUp

on onMessage pTopic, pMessage
   put pTopic & ":" && pMessage & return after field "Messages"
end onMessage

on onLog pMessage
   -- Log messages
end onLog
```

---

### TLS Connection with Authentication

```OXT
on connectSecure
   local tResult
   
   -- Set up callbacks
   mqttSetCallbackTarget the long id of this card
   mqttSetMessageCallback "handleMessage"
   
   -- Connect with TLS
   put mqttConnect("secure.broker.com", 8883, "secureClient", \
                   "username", "password", 60, true, true, \
                   "", "", 0, false, false, false, "") into tResult
   
   if tResult is "OK" then
      put "Connecting..." into field "Status"
   else
      answer error tResult
   end if
end connectSecure

on handleMessage pTopic, pMessage
   put pMessage into field pTopic
end handleMessage
```

---

### Subscribe with Wildcards

```OXT
on setupSubscriptions
   local tResult
   
   -- Assume already connected
   
   -- Subscribe to all sensors
   put mqttSubscribe("broker.example.com", 1883, "sensor/#", 0) into tResult
   
   -- Subscribe to temperature from any room
   put mqttSubscribe("broker.example.com", 1883, "room/+/temperature", 1) into tResult
   
   -- Subscribe to specific device
   put mqttSubscribe("broker.example.com", 1883, "device/12345/status", 2) into tResult
end setupSubscriptions
```

---

### Auto-Reconnect Configuration

```OXT
on connectWithAutoReconnect
   local tResult
   
   mqttSetCallbackTarget the long id of this card
   mqttSetReconnectCallback "onReconnect"
   mqttSetStateChangeCallback "onStateChange"
   
   -- Enable auto-reconnect
   put mqttConnect("broker.example.com", 1883, "resilientClient", \
                   "user", "pass", 60, false, true, "", "", \
                   0, false, false, true, "") into tResult
   
   -- Auto-reconnect is now enabled
   -- Library will automatically reconnect on connection loss
end connectWithAutoReconnect

on onReconnect pEvent, pHost, pPort, pAttempts
   if pEvent is "attempting" then
      put "Reconnecting..." && pAttempts into field "Status"
   else if pEvent is "success" then
      put "Reconnected" into field "Status"
   end if
end onReconnect

on onStateChange pState, pHost, pPort, pReason
   put pState into field "ConnectionState"
end onStateChange
```

---

### Last Will and Testament

```OXT
on connectWithLWT
   local tResult
   
   -- Set LWT: if client disconnects unexpectedly, broker publishes this message
   put mqttConnect("broker.example.com", 1883, "device123", \
                   "", "", 60, false, true, \
                   "device/123/status", "offline", 1, true, \
                   false, false, "") into tResult
   
   -- If this client disconnects abnormally, 
   -- broker will publish "offline" to "device/123/status" with QoS 1, retained
end connectWithLWT
```

---

### QoS 2 Exactly-Once Delivery

```OXT
on publishCritical
   local tResult
   
   -- QoS 2 ensures exactly-once delivery
   put mqttPublish("broker.example.com", 1883, \
                   "critical/command", "SHUTDOWN", 2, false) into tResult
   
   if tResult is "OK" then
      -- Message will be delivered exactly once
      -- Full PUBLISH -> PUBREC -> PUBREL -> PUBCOMP handshake
      put "Command sent" into field "Status"
   end if
end publishCritical
```

---

### Retained Messages

```OXT
on publishStatus
   -- Publish retained message
   -- Last published message is retained by broker
   -- New subscribers immediately receive this message
   put mqttPublish("broker.example.com", 1883, \
                   "device/status", "online", 0, true) into tResult
end publishStatus

on clearRetainedMessage
   -- Clear retained message by publishing empty payload with retain flag
   put mqttPublish("broker.example.com", 1883, \
                   "device/status", "", 0, true) into tResult
end clearRetainedMessage
```

---

### Monitoring Connection Statistics

```OXT
on updateStatistics
   local tStats
   
   put mqttGetStatistics("broker.example.com", 1883) into tStats
   
   put "Messages Sent:" && tStats["messagesSent"] & return into field "Stats"
   put "Messages Received:" && tStats["messagesReceived"] & return after field "Stats"
   put "Bytes Sent:" && tStats["bytesSent"] & return after field "Stats"
   put "Bytes Received:" && tStats["bytesReceived"] & return after field "Stats"
   put "Reconnections:" && tStats["reconnections"] & return after field "Stats"
   
   -- Update every 5 seconds
   send "updateStatistics" to me in 5 seconds
end updateStatistics
```

---

### Multiple Connections

```OXT
on setupMultipleConnections
   -- Connect to first broker
   put mqttConnect("broker1.example.com", 1883, "client1", \
                   "", "", 60, false, true, "", "", \
                   0, false, false, false, "") into tResult1
   
   -- Connect to second broker
   put mqttConnect("broker2.example.com", 1883, "client2", \
                   "", "", 60, false, true, "", "", \
                   0, false, false, false, "") into tResult2
   
   -- Each connection is identified by host:port combination
   -- Can publish/subscribe to either broker independently
   
   wait until mqttIsConnected("broker1.example.com", 1883) with messages
   wait until mqttIsConnected("broker2.example.com", 1883) with messages
   
   -- Publish to first broker
   mqttPublish "broker1.example.com", 1883, "test", "msg1", 0, false
   
   -- Publish to second broker
   mqttPublish "broker2.example.com", 1883, "test", "msg2", 0, false
end setupMultipleConnections
```

---

### Complete Application Example

```OXT
-- Card Script

local sHost, sPort

on openCard
   put "broker.hivemq.com" into sHost
   put 1883 into sPort
   
   -- Configure library
   mqttSetCallbackTarget the long id of this card
   mqttSetMessageCallback "handleMessage"
   mqttSetLogCallback "handleLog"
   mqttSetStateChangeCallback "handleStateChange"
   mqttSetDebugMode false
   mqttSetQuietMode false
   
   -- Connect button
   send "connectToBroker" to button "Connect"
end openCard

on closeCard
   mqttDisconnect sHost, sPort
end closeCard

-- Button "Connect" Script
on mouseUp
   connectToBroker
end mouseUp

on connectToBroker
   local tResult
   
   put mqttConnect(sHost, sPort, "OXTClient", "", "", \
                   60, false, true, "", "", 0, false, \
                   false, false, "") into tResult
   
   if tResult is not "OK" then
      answer error "Connection failed:" && tResult
   end if
end connectToBroker

-- Button "Subscribe" Script
on mouseUp
   local tTopic, tQoS, tResult
   
   put field "Topic" into tTopic
   put field "QoS" into tQoS
   
   put mqttSubscribe(sHost, sPort, tTopic, tQoS) into tResult
   
   if tResult is "OK" then
      put tTopic & return after field "Subscriptions"
   end if
end mouseUp

-- Button "Publish" Script
on mouseUp
   local tTopic, tMessage, tQoS, tRetain, tResult
   
   put field "PubTopic" into tTopic
   put field "Message" into tMessage
   put field "PubQoS" into tQoS
   put the hilite of button "Retain" into tRetain
   
   put mqttPublish(sHost, sPort, tTopic, tMessage, tQoS, tRetain) into tResult
   
   if tResult is "OK" then
      put "Published" into field "Status"
   end if
end mouseUp

-- Card Callbacks
on handleMessage pTopic, pMessage
   put pTopic & ":" && pMessage & return after field "Messages"
end handleMessage

on handleLog pMessage
   if gMQTTDebugMode then
      put pMessage & return after field "Log"
   end if
end handleLog

on handleStateChange pState, pHost, pPort, pReason
   put pState into field "ConnectionStatus"
   
   if pState is "connected" then
      enable button "Subscribe"
      enable button "Publish"
   else
      disable button "Subscribe"
      disable button "Publish"
   end if
end handleStateChange
```

---

## Notes

### Thread Safety
The library is single-threaded. All operations occur on the main OXT thread.

### Binary Data
Message payloads are binary-safe. Use `binaryEncode`/`binaryDecode` for binary data.

### UTF-8 Encoding
Topics are automatically UTF-8 encoded. Ensure message payloads are properly encoded if sending text.

### Memory Management
The library manages socket buffers automatically. Configure `mqttSetMaxBufferSize` if handling very large messages.

### Persistent Sessions
When `pCleanSession` is false, the broker maintains subscriptions and QoS 1/2 messages between connections. Check `mqttGetSessionPresent()` after connecting.

### Keep-Alive
PINGREQ packets are sent automatically based on keep-alive interval and threshold. Default threshold is 75% of keep-alive interval.

### Packet Identifiers
The library automatically manages packet IDs for QoS 1 and 2 messages. Packet IDs cycle from 1 to 65535.

---

## Compliance

This library implements MQTT 3.1.1 specification with full support for:
- QoS 0, 1, and 2
- TLS encryption
- Clean and persistent sessions
- Retained messages
- Last Will and Testament
- Wildcard subscriptions
- Keep-alive mechanism
- Automatic reconnection



---


## Support

For issues or questions, refer to the library source code documentation or test suite implementation.
