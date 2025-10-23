# libMQTTxt MQTT Client Library for OXT

A pure OXT implementation of MQTT 3.1.1 protocol client with full QoS support, TLS encryption, and automatic reconnection.

## Features

- **Full MQTT 3.1.1 Compliance** - All packet types, QoS levels, and features
- **TLS/SSL Support** - Secure connections to encrypted brokers
- **Quality of Service** - QoS 0, 1, and 2 with proper handshaking
- **Wildcards** - Subscribe using `+` and `#` wildcards
- **Retained Messages** - Publish and receive retained messages
- **Last Will Testament** - Configure LWT for abnormal disconnections
- **Auto-Reconnect** - Automatic reconnection with exponential backoff
- **Persistent Sessions** - Resume sessions across reconnections
- **Keep-Alive** - Automatic PINGREQ/PINGRESP handling
- **Multiple Connections** - Connect to multiple brokers simultaneously
- **Statistics** - Track messages, bytes, and connection metrics
- **Binary Safe** - Full support for binary message payloads

## Requirements

- OXT 9.0 or higher
- Network access to MQTT broker

## Installation

1. Download `script_only_stack_` file
2. Place in your OXT project directory
3. Load the library in your stack:

```OXT
on preOpenStack
   start using stack "script_only_stack_"
end preOpenStack
```

## Quick Start

```OXT
-- Configure callbacks
mqttSetCallbackTarget the long id of this card
mqttSetMessageCallback "onMessage"

-- Connect to broker
put mqttConnect("broker.hivemq.com", 1883, "myClient", "", "", \
                60, false, true, "", "", 0, false, false, false, "") into tResult

-- Wait for connection
wait until mqttIsConnected("broker.hivemq.com", 1883) with messages

-- Subscribe to topic
mqttSubscribe "broker.hivemq.com", 1883, "test/topic", 0

-- Publish message
mqttPublish "broker.hivemq.com", 1883, "test/topic", "Hello MQTT", 0, false

-- Handle incoming messages
on onMessage pTopic, pMessage
   put pTopic & ":" && pMessage
end onMessage
```

## Documentation

- **[Complete API Reference](MQTT_LIBRARY_REFERENCE.md)** - Full function documentation with examples
- **[Test Suite](test_stack_card_script)** - Comprehensive test implementation

## Usage Examples

### Basic Connection

```OXT
-- Simple connection
put mqttConnectSimple("broker.example.com", 1883, "client1", "user", "pass") into tResult

if tResult is "OK" then
   put "Connected"
end if
```

### TLS Connection

```OXT
-- Connect with TLS encryption
put mqttConnect("broker.example.com", 8883, "secureClient", \
                "username", "password", 60, true, true, \
                "", "", 0, false, false, true, "") into tResult
```

### Subscribe with Wildcards

```OXT
-- Subscribe to all sensors
mqttSubscribe "broker.example.com", 1883, "sensor/#", 0

-- Subscribe to temperature from any room
mqttSubscribe "broker.example.com", 1883, "room/+/temperature", 0
```

### QoS Levels

```OXT
-- QoS 0 - Fire and forget
mqttPublish "broker.example.com", 1883, "data", "value", 0, false

-- QoS 1 - At least once delivery
mqttPublish "broker.example.com", 1883, "data", "value", 1, false

-- QoS 2 - Exactly once delivery
mqttPublish "broker.example.com", 1883, "critical", "command", 2, false
```

### Retained Messages

```OXT
-- Publish retained message
mqttPublish "broker.example.com", 1883, "device/status", "online", 0, true

-- Clear retained message
mqttPublish "broker.example.com", 1883, "device/status", "", 0, true
```

### Last Will Testament

```OXT
-- Configure LWT - published if client disconnects unexpectedly
put mqttConnect("broker.example.com", 1883, "device1", "", "", 60, \
                false, true, "device/status", "offline", 1, true, \
                false, false, "") into tResult
```

### Auto-Reconnect

```OXT
-- Enable automatic reconnection
mqttSetReconnectCallback "onReconnect"

put mqttConnect("broker.example.com", 1883, "resilient", "", "", 60, \
                false, true, "", "", 0, false, false, true, "") into tResult

on onReconnect pEvent, pHost, pPort, pAttempts
   if pEvent is "success" then
      put "Reconnected after" && pAttempts && "attempts"
   end if
end onReconnect
```

### Connection Statistics

```OXT
put mqttGetStatistics("broker.example.com", 1883) into tStats

put "Messages sent:" && tStats["messagesSent"]
put "Messages received:" && tStats["messagesReceived"]
put "Bytes sent:" && tStats["bytesSent"]
put "Reconnections:" && tStats["reconnections"]
```

## Configuration

### Callbacks

```OXT
-- Set callback target (required)
mqttSetCallbackTarget the long id of this card

-- Set message callback
mqttSetMessageCallback "handleMessage"

-- Set log callback
mqttSetLogCallback "handleLog"

-- Set state change callback
mqttSetStateChangeCallback "handleStateChange"

-- Set reconnect callback
mqttSetReconnectCallback "handleReconnect"
```

### Options

```OXT
-- Enable debug logging
mqttSetDebugMode true

-- Disable all logging
mqttSetQuietMode false

-- Set keep-alive threshold (75% default)
mqttSetKeepAliveThreshold 0.75

-- Set maximum buffer size (5MB default)
mqttSetMaxBufferSize 5242880

-- Enable persistent storage for QoS 1/2
mqttSetPersistentStore true, specialFolderPath("documents") & "/mqtt"
```

## API Overview

### Connection Management

| Function | Description |
|----------|-------------|
| `mqttConnect()` | Connect to MQTT broker with full options |
| `mqttConnectSimple()` | Connect with common defaults |
| `mqttIsConnected()` | Check connection status |
| `mqttReconnect()` | Manually reconnect |
| `mqttDisconnect()` | Disconnect from broker |
| `mqttCleanupAll()` | Disconnect all connections |

### Publishing & Subscribing

| Function | Description |
|----------|-------------|
| `mqttPublish()` | Publish message to topic |
| `mqttSubscribe()` | Subscribe to topic filter |
| `mqttUnsubscribe()` | Unsubscribe from topic |
| `mqttGetSubscriptions()` | List active subscriptions |

### Utilities

| Function | Description |
|----------|-------------|
| `mqttGetConnectionInfo()` | Get connection details |
| `mqttGetStatistics()` | Get message/byte counters |
| `mqttResetStatistics()` | Reset statistics |
| `mqttGetSessionPresent()` | Check session persistence |
| `mqttGetConnections()` | List all connections |
| `mqttTestLibrary()` | Verify library loaded |
| `mqttSelfTest()` | Run internal tests |
| `mqttBenchmark()` | Performance benchmark |

## QoS Levels

| QoS | Description | Use Case |
|-----|-------------|----------|
| 0 | At most once | Fire and forget, sensor data |
| 1 | At least once | Important data, duplicates OK |
| 2 | Exactly once | Critical commands, no duplicates |

## Wildcards

| Wildcard | Matches | Example |
|----------|---------|---------|
| `+` | Single level | `sensor/+/temp` matches `sensor/1/temp` |
| `#` | Multiple levels | `sensor/#` matches `sensor/1/temp/celsius` |

## Error Handling

All functions return `"OK"` on success or `"ERROR: description"` on failure.

```OXT
put mqttPublish("broker.example.com", 1883, "topic", "msg", 0, false) into tResult

if tResult is not "OK" then
   answer error tResult
   exit to top
end if
```

## Testing

The library includes a comprehensive test suite covering:

- Library initialization and self-tests
- Configuration functions
- TLS connections
- All QoS levels (0, 1, 2)
- Wildcard subscriptions
- Subscribe/unsubscribe lifecycle
- Rapid message bursts
- Large payload handling (1KB, 10KB, 50KB)
- Retained messages
- Topic validation
- Connection statistics
- Error handling and recovery
- Clean disconnection

Run the test suite by clicking the test button after loading the test stack.

## Performance

Tested performance metrics:

- **Throughput**: 200-400 messages/second
- **Latency**: < 5ms per message (QoS 0)
- **Large Messages**: Up to 50KB+ payloads
- **Concurrent**: 5+ simultaneous operations
- **Reliability**: 90%+ delivery in burst tests

## Tested Brokers

- HiveMQ Cloud
- Mosquitto
- AWS IoT Core
- Azure IoT Hub
- Eclipse Mosquitto

## Protocol Compliance

MQTT 3.1.1 Specification Implementation:

- ✅ CONNECT / CONNACK
- ✅ PUBLISH / PUBACK / PUBREC / PUBREL / PUBCOMP
- ✅ SUBSCRIBE / SUBACK
- ✅ UNSUBSCRIBE / UNSUBACK
- ✅ PINGREQ / PINGRESP
- ✅ DISCONNECT
- ✅ QoS 0, 1, 2 flows
- ✅ Session persistence
- ✅ Retained messages
- ✅ Last Will Testament
- ✅ Keep-alive mechanism
- ✅ Wildcard subscriptions
- ✅ TLS encryption

## Version History

### 2.11.8 (Current)
- Fixed empty topic validation in mqttPublish
- Fixed QoS 2 message statistics tracking
- Improved debug logging in message callback invocation
- Enhanced message reception validation

### 2.11.7
- Critical fix: Timer send uses 'me' instead of stack reference
- Fixed direct logging in timer handlers
- Improved global variable declarations

### 2.11.5
- Timer handlers no longer use parameters
- Connection ID stored in connection record
- Marker-based timer approach

## Architecture

The library is implemented as a script-only stack with:

- **Socket Management** - OXT native socket handling
- **Packet Encoding/Decoding** - Binary encoding for MQTT packets
- **State Machine** - Connection and QoS state tracking
- **Timer Management** - Keep-alive and reconnection timers
- **Buffer Management** - Automatic receive buffer handling
- **Callback System** - Event-driven message delivery

## Limitations

- Single-threaded (runs on main OXT thread)
- Maximum message size limited by `mqttSetMaxBufferSize` (default 5MB)
- Socket callbacks require library in `stacksInUse`
- Topic length limits depend on broker (typically 256 characters)

## Common Use Cases

### IoT Sensor Data
```OXT
-- Publish sensor readings with QoS 0
mqttPublish "broker.example.com", 1883, "sensor/temp", tReading, 0, false
```

### Command & Control
```OXT
-- Send critical commands with QoS 2
mqttPublish "broker.example.com", 1883, "device/command", "SHUTDOWN", 2, false
```

### Status Monitoring
```OXT
-- Publish retained status messages
mqttPublish "broker.example.com", 1883, "device/status", "online", 0, true
```

### Data Logging
```OXT
-- Subscribe to all device data
mqttSubscribe "broker.example.com", 1883, "device/#", 1
```

## Troubleshooting

### Connection Fails
- Verify broker hostname and port
- Check network connectivity
- Confirm credentials (username/password)
- Verify TLS settings match broker requirements

### Messages Not Received
- Confirm subscription was successful
- Check message callback is set
- Verify topic matches subscription filter
- Check QoS level compatibility

### Socket Callback Errors
- Ensure library is in `stacksInUse`
- Verify callback target is set correctly
- Check callback handler exists in target

### Performance Issues
- Increase buffer size with `mqttSetMaxBufferSize`
- Use QoS 0 for high-throughput scenarios
- Consider message batching for bulk data

## Contributing

This is a production-ready library with comprehensive test coverage. For issues or enhancements, ensure changes maintain MQTT 3.1.1 compliance and pass the full test suite.

## License

Open source. Free to use in commercial and non-commercial projects.

## Support

- Documentation: [MQTT_LIBRARY_REFERENCE.md](MQTT_LIBRARY_REFERENCE.md)
- Test Suite: [test_stack_card_script](test_stack_card_script)
- MQTT Specification: [MQTT 3.1.1](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/mqtt-v3.1.1.html)

## Acknowledgments

Implements MQTT 3.1.1 protocol specification as defined by OASIS.
