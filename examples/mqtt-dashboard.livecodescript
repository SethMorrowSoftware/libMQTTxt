script "MqttDashboard"

-- mqtt-dashboard.livecodescript - libMQTTxt's demo: connect to a broker,
-- subscribe, publish, and watch the traffic. One file, paste and open.
--
-- What to look at as a template:
--   * INITIALISE THE EMBEDDED LIBRARY YOURSELF. The engine sends
--     `libraryStack` only to a stack in stacksInUse; this demo CARRIES
--     libMQTTxt in its own script, so that message never arrives and
--     preOpenStack calls mqttInitialize instead. Every embedding stack owes
--     the library that one line.
--   * THE ENGINE'S SOCKET MESSAGES RIDE ALONG. socketError / socketClosed /
--     socketTimeout come from the embedded library and this demo defines none
--     of its own, so its `on socket*` wrappers must NOT be dropped - drop them
--     with nothing else listening and the sockets go quiet with no error, which
--     is the worst failure shape there is.
--   * CALLBACKS BEFORE CONNECT. mqttSetCallbackTarget then the four handler
--     names, so nothing that arrives during the handshake is dropped.
--   * closeStack calls mqttCleanupAll - the one mandatory teardown, or the
--     sockets and keep-alive timers outlive the window.
--
-- HOW TO USE:
--   1. Paste THIS file into a stack script and reopen the stack - opening the
--      file itself builds no window (OXT-ENGINE-NOTES 5.5). libMQTTxt is
--      embedded already, so there is nothing else to load.
--      REOPEN, not just Apply: applying a script to a stack that is already
--      open sends neither preOpenStack nor openStack, so nothing rebuilds and
--      the boot self-check below does not run (the sixth engine run applied a
--      new version this way and got no boot block at all - correct, and easy
--      to mistake for a check that passed silently).
--   2. The form is pre-filled for a public test broker. Press Connect, then
--      Subscribe, then Publish to the topic you subscribed to and watch it
--      come back.
--
-- TLS: tick TLS for port 8883. "Verify cert" is ON by default and should stay
-- that way; an unverified TLS socket is encrypted against a passive listener
-- and worthless against an active one. Port 8883 WITHOUT TLS is the most
-- common mistake and looks like a broker that drops you right after CONNECT;
-- the log says so when you try it.
--
-- HONESTY (the suite convention): this stack HAS run on an OXT engine, six
-- times on 2026-09-05/06, against a local mosquitto and broker.hivemq.com, in
-- the clear and over verified TLS. The full record is docs/ENGINE-NOTES.md
-- (section 3 lists what each run established). Observed: the window builds,
-- the embedded library connects, QoS 0/1/2, UTF-8 and binary payloads,
-- retained, unsubscribe, keep-alive, auto-reconnect back-off, and payloads to
-- 1 MB. Not yet observed: a successful reconnect (every attempt so far was to
-- a port that refused it), two connections at once, the persistent store, and
-- this boot check on a fresh engine since its 2.12.4 split. Each of those is
-- still "verified statically; needs an OXT pass".
--
-- THE LOOK is the suite UI kit, carried verbatim between the marker lines
-- (master: tools/ui-kit.livecodescript, vendored from xtalk-suite - see
-- tools/VENDORED.md; the drift gate holds every copy byte-identical). Change
-- the look in the master, never here.

-- Bump when the generated UI changes so a saved stack rebuilds its controls.

-- >>> BEGIN EMBEDDED LIBRARIES (tools/sync-demo-embeds.py) >>>
-- Pasted from the source below so this demo runs with NO `start
-- using` wiring. Do not edit inside these sentinels: run
-- `python3 tools/sync-demo-embeds.py` instead. The source stays the
-- single point of truth and the right dependency for a real project.
--
-- The library is placed ABOVE the demo's own code because OXT resolves
-- script-level declarations by lexical position.

-- ---- libMQTTxt.oxtstack ----

-- ========================================
-- MQTT Client Library for OXT
-- ========================================

-- ========================================
-- GLOBAL VARIABLES
-- ========================================

global gMQTTConnections           -- Array of all active connections indexed by "host:port"
global gMQTTConnIndex             -- Maps OXT socket IDs to connection IDs (for multi-connection)
global gMQTTQuietMode             -- Suppress debug logging when true
global gMQTTMessageCallback       -- Handler name for incoming messages
global gMQTTLogCallback           -- Handler name for log messages
global gMQTTReconnectCallback     -- Handler name for reconnection events
global gMQTTStateChangeCallback   -- Handler name for connection state changes
global gMQTTMaxBufferSize         -- Maximum receive buffer size in bytes
global gMQTTPersistentStoreEnabled -- Enable persistent storage for QoS 1/2 messages
global gMQTTPersistentStorePath   -- Path to persistent message store
global gMQTTDebugMode             -- Enable verbose debug logging
global gMQTTCallbackTarget        -- Target stack/object for callbacks
global gMQTTKeepAliveThreshold    -- When to send PINGREQ (0-1, default 0.75 = 75%)
global gMQTTTimerTokens           -- Maps a numeric timer token to its connection ID
global gMQTTTimerSeq              -- Monotonic counter that mints timer tokens
global gMQTTWriteChunkSize        -- Largest single write handed to the kernel (bytes)
global gMQTTWriteLocks            -- socket ID -> true while a multi-chunk write is in progress
global gMQTTWriteYieldMs          -- how long to yield between chunks of a large write
global gMQTTInitialized           -- true once mqttInitialize has run in this engine session

-- ========================================
-- LIBRARY INITIALIZATION
-- ========================================

-- Initialise the library's globals. Idempotent, and safe to call at any time.
--
-- THIS IS SPLIT OUT OF libraryStack SO THE LIBRARY CAN BE EMBEDDED. The engine
-- sends `libraryStack` only to a stack in stacksInUse; a demo that pastes this
-- source into its own stack script - the suite's one-file convention - never
-- receives it, so every global below stayed empty and the first connection
-- attempt ran against an uninitialised buffer limit and keep-alive threshold.
-- An embedding stack calls mqttInitialize from preOpenStack instead.
command mqttInitialize
   -- Initialize connection tracking
   if gMQTTConnections is empty then
      put empty into gMQTTConnections
   end if
   
   -- Initialize socket ID to connection ID mapping (CRITICAL for multi-connection)
   if gMQTTConnIndex is empty then
      put empty into gMQTTConnIndex
   end if
   
   -- Initialize configuration with sensible defaults
   if gMQTTQuietMode is empty then
      put false into gMQTTQuietMode
   end if
   
   if gMQTTDebugMode is empty then
      put false into gMQTTDebugMode
   end if
   
   if gMQTTMaxBufferSize is empty then
      put 5242880 into gMQTTMaxBufferSize  -- 5MB default
   end if
   
   if gMQTTPersistentStoreEnabled is empty then
      put false into gMQTTPersistentStoreEnabled
   end if

   if gMQTTKeepAliveThreshold is empty then
      put 0.75 into gMQTTKeepAliveThreshold  -- Default 75% of keep-alive interval
   end if

   -- Timer routing: every scheduled message carries an integer token, and this
   -- array maps that token back to the one connection it belongs to. Tokens are
   -- integers precisely so they survive `send "handler" && tToken` without any
   -- quoting; a connection ID contains a colon and does not.
   if gMQTTTimerTokens is empty then
      put empty into gMQTTTimerTokens
   end if

   if gMQTTTimerSeq is empty then
      put 0 into gMQTTTimerSeq
   end if

   -- 16 KB is the Linux kernel's DEFAULT initial TCP send buffer (tcp_wmem's
   -- middle value) and well under macOS's 128 KB, so a chunk this size fits a
   -- cold connection's buffer on both. See __writeSocket for why that matters.
   if gMQTTWriteChunkSize is empty then
      put 16384 into gMQTTWriteChunkSize
   end if

   -- Re-entrancy locks for the chunked write. A multi-chunk write yields to the
   -- engine between chunks (see __writeSocket), and application code that runs
   -- during that yield may call straight back in; a second packet interleaved
   -- into the middle of the first is exactly the stream corruption the whole
   -- write path exists to prevent. Cleared here, never carried across a session.
   put empty into gMQTTWriteLocks

   -- How long __writeSocket pauses between chunks of a large write.
   --
   -- 50 IS A MEASURED VALUE, NOT A GUESS, and it is deliberately conservative.
   -- The tenth engine run laddered 0, 5, 20 and 50 ms against a broker over
   -- the internet: the first three stalled at exactly 65536 bytes every time
   -- and 50 carried a megabyte. The pause is what keeps the write from
   -- outrunning the link (docs/ENGINE-NOTES.md 1.1), so the safe value is set
   -- by the SLOWEST link an application uses, and a default that is wrong is
   -- better wrong slow than wrong broken.
   --
   -- It costs nothing for ordinary traffic: a packet that fits one chunk never
   -- pauses at all, which is every control packet and almost every publish.
   -- It caps a MULTI-chunk write at chunk/pause = 320 KB/s. On a LAN that is
   -- far below what the link can do; lower it, or raise the chunk size, once
   -- you have measured your own path.
   if gMQTTWriteYieldMs is empty then
      put 50 into gMQTTWriteYieldMs
   end if

   put true into gMQTTInitialized
end mqttInitialize

on libraryStack
   mqttInitialize

   -- ONLY on this path. The stacksInUse warning below asks "did you forget
   -- `start using`?", which is a question about the library-stack deployment
   -- and nonsense for an embedded copy - where the answer is always "no, it is
   -- not in stacksInUse" and the setup is nonetheless correct.
   __validateLibraryStack
end libraryStack

on releaseStack
   mqttCleanupAll
end releaseStack

-- ========================================
-- PUBLIC API - CONFIGURATION
-- ========================================

command mqttSetCallbackTarget pTarget
   put pTarget into gMQTTCallbackTarget
   __mqttLog "Callback target set to:" && gMQTTCallbackTarget
   __validateCallbackEnvironment
end mqttSetCallbackTarget

command mqttSetKeepAliveThreshold pThreshold
   -- Guard the type before the range: a non-numeric value compares as TEXT here,
   -- so "high" would silently pass "> 1" and be stored as the threshold.
   if pThreshold is not a number then
      __mqttLog "ERROR: Keep-alive threshold must be a number between 0 and 1"
      exit mqttSetKeepAliveThreshold
   end if

   if pThreshold <= 0 or pThreshold > 1 then
      __mqttLog "ERROR: Keep-alive threshold must be greater than 0 and at most 1"
      exit mqttSetKeepAliveThreshold
   end if
   
   put pThreshold into gMQTTKeepAliveThreshold
   __mqttLog "Keep-alive threshold set to:" && (pThreshold * 100) & "%"
end mqttSetKeepAliveThreshold

function mqttGetKeepAliveThreshold
   return gMQTTKeepAliveThreshold
end mqttGetKeepAliveThreshold

command mqttSetDebugMode pDebug
   put pDebug into gMQTTDebugMode
   __mqttLog "Debug mode:" && pDebug
end mqttSetDebugMode

command mqttSetLogCallback pHandlerName
   put pHandlerName into gMQTTLogCallback
   __captureCallbackTarget
   __mqttLog "Log callback set to:" && pHandlerName
end mqttSetLogCallback

command mqttSetQuietMode pQuiet
   put pQuiet into gMQTTQuietMode
end mqttSetQuietMode

command mqttSetMessageCallback pHandlerName
   put pHandlerName into gMQTTMessageCallback
   __captureCallbackTarget
   __mqttLog "Message callback set to:" && pHandlerName
end mqttSetMessageCallback

command mqttSetReconnectCallback pHandlerName
   put pHandlerName into gMQTTReconnectCallback
   __captureCallbackTarget
   __mqttLog "Reconnect callback set to:" && pHandlerName
end mqttSetReconnectCallback

command mqttSetStateChangeCallback pHandlerName
   put pHandlerName into gMQTTStateChangeCallback
   __captureCallbackTarget
   __mqttLog "State change callback set to:" && pHandlerName
end mqttSetStateChangeCallback

command mqttSetMaxBufferSize pBytes
   if pBytes is not a number then
      __mqttLog "ERROR: Buffer size must be a number; keeping" && gMQTTMaxBufferSize
      exit mqttSetMaxBufferSize
   end if

   if (pBytes < 65536) then
      __mqttLog "WARNING: Buffer size too small, setting to 64KB minimum"
      put 65536 into gMQTTMaxBufferSize
   else if (pBytes > 268435455) then
      __mqttLog "WARNING: Buffer size exceeds MQTT spec limit (256MB), capping"
      put 268435455 into gMQTTMaxBufferSize
   else
      put pBytes into gMQTTMaxBufferSize
   end if
   __mqttLog "Maximum buffer size set to:" && gMQTTMaxBufferSize && "bytes"
end mqttSetMaxBufferSize

command mqttSetWriteChunkSize pBytes
   -- The knob the conformance ladder turns. Small enough to fit a cold kernel
   -- send buffer is the point; there is no upper limit worth enforcing beyond
   -- keeping it a sane number, because a huge value simply reproduces the
   -- single-write behaviour this replaced, which is a legitimate thing to want
   -- to demonstrate.
   if pBytes is not a number then
      __mqttLog "ERROR: Write chunk size must be a number; keeping" && gMQTTWriteChunkSize
      exit mqttSetWriteChunkSize
   end if

   if pBytes < 512 then
      __mqttLog "WARNING: Write chunk size below 512 bytes; using 512"
      put 512 into gMQTTWriteChunkSize
   else if pBytes > 268435455 then
      put 268435455 into gMQTTWriteChunkSize
   else
      put trunc(pBytes) into gMQTTWriteChunkSize
   end if
   __mqttLog "Write chunk size set to:" && gMQTTWriteChunkSize && "bytes"
end mqttSetWriteChunkSize

function mqttGetWriteChunkSize
   return gMQTTWriteChunkSize
end mqttGetWriteChunkSize

-- How long a large write pauses between chunks, in milliseconds. Default 50.
--
-- THIS IS A RATE LIMIT, and it is the difference between a large publish
-- working and failing. The engine's plaintext write hands the kernel one
-- non-blocking send and does not retry a socket that is momentarily full, so a
-- write that outruns the link fills the send buffer and the next chunk times
-- out with the stream half-sent (docs/ENGINE-NOTES.md 1.1, ten engine runs).
-- The pause is what keeps the write inside the link's drain rate.
--
-- THE ARITHMETIC IS chunk / pause. At the defaults that is 16384 bytes per
-- 50 ms, so 320 KB/s. Raise this and a large write goes slower and is safer on
-- a slow uplink; lower it and it goes faster until it outruns the link and
-- fails. The tenth run measured a broker over the internet stalling at 0, 5
-- and 20 ms and succeeding at 50.
--
-- ORDINARY TRAFFIC PAYS NOTHING. A packet that fits one chunk never pauses -
-- every control packet, and almost every publish. This only ever governs a
-- payload larger than mqttGetWriteChunkSize().
--
-- TO TUNE IT for your own path, run examples/mqtt-conformance-button: its
-- first stage ladders the pause upward and reports the fastest one that
-- carries a megabyte.
command mqttSetWriteYieldMs pMilliseconds
   if pMilliseconds is not a number or pMilliseconds < 0 then
      __mqttLog "ERROR: Write yield must be a non-negative number; keeping" \
            && gMQTTWriteYieldMs
      exit mqttSetWriteYieldMs
   end if

   if pMilliseconds > 1000 then
      __mqttLog "WARNING: Write yield above 1000ms; using 1000"
      put 1000 into gMQTTWriteYieldMs
   else
      put trunc(pMilliseconds) into gMQTTWriteYieldMs
   end if
   __mqttLog "Write yield set to:" && gMQTTWriteYieldMs && "ms per chunk"
end mqttSetWriteYieldMs

function mqttGetWriteYieldMs
   return gMQTTWriteYieldMs
end mqttGetWriteYieldMs

command mqttSetPersistentStore pEnabled, pStorePath
   put pEnabled into gMQTTPersistentStoreEnabled
   
   if pStorePath is not empty then
      put pStorePath into gMQTTPersistentStorePath
   else
      put specialFolderPath("documents") & "/mqtt_store" into gMQTTPersistentStorePath
   end if
   
   if gMQTTPersistentStoreEnabled then
      __mqttLog "Persistent store enabled at:" && gMQTTPersistentStorePath
      __ensurePersistentStoreDirectory
   else
      __mqttLog "Persistent store disabled"
   end if
end mqttSetPersistentStore

-- ========================================
-- PUBLIC API - CONNECTION MANAGEMENT
-- ========================================

function mqttConnect pHost, pPort, pClientID, pUsername, pPassword, pKeepAlive, pUseTLS, pCleanSession, pLWTTopic, pLWTMessage, pLWTQoS, pLWTRetain, pVerifyTLS, pAutoReconnect, pCACertPath
   -- Whatever the host did or did not do at open time, nothing below runs
   -- against empty globals. See __ensureInit for the run that made this rule.
   __ensureInit

   -- Validate the address BEFORE it is used to key anything: every table in this
   -- library is indexed by "host:port", so an empty host would mint the key
   -- ":1883" and quietly alias every other portless caller onto one record.
   if pHost is empty then
      return "ERROR: Host cannot be empty"
   end if

   if pPort is not a number then
      return "ERROR: Port must be a number (1-65535)"
   end if

   if (pPort < 1) or (pPort > 65535) then
      return "ERROR: Invalid port (must be 1-65535)"
   end if

   -- A bare IPv6 literal makes "host:port" ambiguous (engine note 6.2: splitting
   -- a socket id on ":" yields EMPTY for "::1:54321"). Require the bracket form,
   -- which is what the engine itself expects for an IPv6 socket.
   if (pHost contains ":") and not (pHost begins with "[") then
      return "ERROR: IPv6 host must be bracketed, e.g. [::1]"
   end if

   local tConnID
   put pHost & ":" & pPort into tConnID

   -- Set defaults for optional parameters
   if pKeepAlive is empty then put 60 into pKeepAlive
   if pClientID is empty then put "LC_" & the milliseconds into pClientID
   if pUseTLS is empty then put false into pUseTLS
   if pCleanSession is empty then put true into pCleanSession
   if pLWTQoS is empty then put 0 into pLWTQoS
   if pLWTRetain is empty then put false into pLWTRetain
   if pAutoReconnect is empty then put false into pAutoReconnect

   -- FAIL CLOSED: an unset pVerifyTLS now means VERIFY. Defaulting this to false
   -- made every "just turn TLS on" caller silently accept any certificate,
   -- which is an unauthenticated channel wearing a padlock. Callers that really
   -- want an unverified socket must now say so.
   if pVerifyTLS is empty then put true into pVerifyTLS

   -- Validate parameters according to MQTT 3.1.1 spec
   if pLWTQoS is not a number then
      return "ERROR: Invalid LWT QoS (must be 0, 1, or 2)"
   end if

   if (pLWTQoS < 0) or (pLWTQoS > 2) then
      return "ERROR: Invalid LWT QoS (must be 0, 1, or 2)"
   end if

   if pKeepAlive is not a number then
      return "ERROR: Invalid keep-alive (must be 0-65535)"
   end if

   if (pKeepAlive < 0) or (pKeepAlive > 65535) then
      return "ERROR: Invalid keep-alive (must be 0-65535)"
   end if

   -- The client identifier and LWT strings all go into 16-bit-prefixed fields
   if the number of bytes of textEncode(pClientID, "UTF-8") > 65535 then
      return "ERROR: Client ID exceeds 65535 bytes"
   end if

   if the number of bytes of textEncode(pLWTTopic, "UTF-8") > 65535 then
      return "ERROR: LWT topic exceeds 65535 bytes"
   end if

   if the number of bytes of textEncode(pLWTMessage, "UTF-8") > 65535 then
      return "ERROR: LWT message exceeds 65535 bytes"
   end if

   -- Tear down anything still running under this key before the record is reset,
   -- so a re-connect cannot leave the previous attempt's timers firing into it.
   if gMQTTConnections[tConnID] is not empty then
      __cancelPendingMessages tConnID
   end if

   -- Initialize connection structure with all required fields
   put pHost into gMQTTConnections[tConnID]["host"]
   put pPort into gMQTTConnections[tConnID]["port"]
   put pClientID into gMQTTConnections[tConnID]["clientID"]
   put pUsername into gMQTTConnections[tConnID]["username"]
   put pPassword into gMQTTConnections[tConnID]["password"]
   put false into gMQTTConnections[tConnID]["connected"]
   put pKeepAlive into gMQTTConnections[tConnID]["keepAlive"]
   put 0 into gMQTTConnections[tConnID]["packetID"]
   put empty into gMQTTConnections[tConnID]["receiveBuffer"]
   put the milliseconds into gMQTTConnections[tConnID]["lastActivity"]
   put 0 into gMQTTConnections[tConnID]["lastPingTime"]
   put pUseTLS into gMQTTConnections[tConnID]["useTLS"]
   put pVerifyTLS into gMQTTConnections[tConnID]["verifyTLS"]
   put pCACertPath into gMQTTConnections[tConnID]["caCertPath"]
   put pCleanSession into gMQTTConnections[tConnID]["cleanSession"]
   put false into gMQTTConnections[tConnID]["sessionPresent"]
   put pAutoReconnect into gMQTTConnections[tConnID]["autoReconnect"]
   put 0 into gMQTTConnections[tConnID]["reconnectAttempts"]
   put empty into gMQTTConnections[tConnID]["subscriptions"]
   put empty into gMQTTConnections[tConnID]["pendingAcks"]
   put empty into gMQTTConnections[tConnID]["pendingSubAcks"]
   put empty into gMQTTConnections[tConnID]["pendingUnsubAcks"]
   put empty into gMQTTConnections[tConnID]["pendingQoS2"]
   put empty into gMQTTConnections[tConnID]["reconnectMessageID"]
   put empty into gMQTTConnections[tConnID]["keepAliveMessageID"]
   put false into gMQTTConnections[tConnID]["closing"]

   -- Initialize statistics tracking
   put 0 into gMQTTConnections[tConnID]["stats"]["messagesSent"]
   put 0 into gMQTTConnections[tConnID]["stats"]["messagesReceived"]
   put 0 into gMQTTConnections[tConnID]["stats"]["bytesSent"]
   put 0 into gMQTTConnections[tConnID]["stats"]["bytesReceived"]
   put 0 into gMQTTConnections[tConnID]["stats"]["reconnections"]
   
   -- Store Last Will and Testament parameters
   put pLWTTopic into gMQTTConnections[tConnID]["lwt"]["topic"]
   put pLWTMessage into gMQTTConnections[tConnID]["lwt"]["message"]
   put pLWTQoS into gMQTTConnections[tConnID]["lwt"]["qos"]
   put pLWTRetain into gMQTTConnections[tConnID]["lwt"]["retain"]
   
   -- Log connection attempt
   __mqttLog "Connecting to" && pHost & ":" & pPort && "as" && pClientID && "(TLS:" && pUseTLS && "Verify:" && pVerifyTLS & ")"
   __mqttDebug "Full connection params - KeepAlive:" && pKeepAlive && "CleanSession:" && pCleanSession && "AutoReconnect:" && pAutoReconnect
   
   if pLWTTopic is not empty then
      __mqttLog "LWT configured:" && pLWTTopic && "(QoS" && pLWTQoS & ")"
   end if
   
   if pCACertPath is not empty then
      __mqttLog "CA certificate path:" && pCACertPath
   end if
   
   -- Load persistent messages if enabled
   if gMQTTPersistentStoreEnabled then
      __loadPersistentMessages tConnID
   end if
   
   -- Notify state change
   __invokeStateChangeCallback "connecting", pHost, pPort, ""
   
   local tError
   try
      -- Close existing socket if still open. The engine queues socketClosed for
      -- it, and that message arrives AFTER this handler returns - by which time
      -- the record has been rebuilt - so mark the close as ours and let
      -- mqttSocketClosed drop the event instead of scheduling a phantom
      -- reconnect against a connection that is mid-connect.
      if __isSocketOpen(tConnID) then
         __mqttDebug "Closing existing socket" && tConnID
         put true into gMQTTConnections[tConnID]["closing"]
         close socket tConnID
         wait 100 milliseconds
      end if

      -- Open new socket
      __mqttDebug "Opening socket to" && tConnID
      open socket to tConnID with message "mqttSocketConnected"

      -- CRITICAL: Store socket ID to connection ID mapping immediately
      put tConnID into gMQTTConnIndex[tConnID]

      return "OK"
   catch tError
      __mqttLog "ERROR opening socket:" && tError
      __invokeStateChangeCallback "error", pHost, pPort, tError
      return "ERROR:" && tError
   end try
end mqttConnect

function mqttConnectSimple pHost, pPort, pClientID, pUsername, pPassword
   -- TLS is off here, so the verification argument is moot; it is passed as true
   -- to match the library's fail-closed default rather than contradict it.
   return mqttConnect(pHost, pPort, pClientID, pUsername, pPassword, 60, false, true, "", "", 0, false, true, false, "")
end mqttConnectSimple

function mqttReconnect pHost, pPort
   local tConnID
   put pHost & ":" & pPort into tConnID

   -- Verify we have a previous connection to reconnect
   if gMQTTConnections[tConnID] is empty then
      return "ERROR: No previous connection to reconnect"
   end if

   __mqttLog "Attempting to reconnect to" && pHost & ":" & pPort

   -- Invoke reconnect callback
   __invokeReconnectCallback "attempting", pHost, pPort, gMQTTConnections[tConnID]["reconnectAttempts"] + 1

   -- Close existing socket if any. As in mqttConnect, flag the close as OURS so
   -- the queued socketClosed does not schedule a second reconnect on top of this
   -- one and start a doubling cascade of attempts.
   local tErr
   try
      if __isSocketOpen(tConnID) then
         put true into gMQTTConnections[tConnID]["closing"]
         close socket tConnID
         wait 100 milliseconds
      end if
   catch tErr
      -- Socket already closed, continue
   end try

   -- Reset state for reconnection
   put false into gMQTTConnections[tConnID]["connected"]
   put empty into gMQTTConnections[tConnID]["receiveBuffer"]
   put false into gMQTTConnections[tConnID]["closing"]
   add 1 to gMQTTConnections[tConnID]["reconnectAttempts"]
   add 1 to gMQTTConnections[tConnID]["stats"]["reconnections"]

   -- Notify state change
   __invokeStateChangeCallback "connecting", pHost, pPort, "reconnect attempt"

   -- Reopen socket
   local tError
   try
      open socket to tConnID with message "mqttSocketConnected"

      -- CRITICAL: Re-establish socket ID mapping
      put tConnID into gMQTTConnIndex[tConnID]

      return "OK"
   catch tError
      __mqttLog "ERROR reconnecting:" && tError
      __invokeReconnectCallback "failed", pHost, pPort, gMQTTConnections[tConnID]["reconnectAttempts"]
      __invokeStateChangeCallback "error", pHost, pPort, tError
      return "ERROR:" && tError
   end try
end mqttReconnect

function mqttIsConnected pHost, pPort
   local tConnID
   put pHost & ":" & pPort into tConnID
   return (gMQTTConnections[tConnID]["connected"] is true) and __isSocketOpen(tConnID)
end mqttIsConnected

command mqttDisconnect pHost, pPort
   local tConnID
   put pHost & ":" & pPort into tConnID
   
   -- Nothing to disconnect if no connection exists
   if gMQTTConnections[tConnID] is empty then
      exit mqttDisconnect
   end if
   
   __mqttLog "Disconnecting from" && pHost & ":" & pPort

   -- Notify state change
   __invokeStateChangeCallback "disconnecting", pHost, pPort, "user requested"

   -- Every step below runs whether or not the link is still live. The old code
   -- did all of this INSIDE `if mqttIsConnected(...)`, so disconnecting a
   -- connection whose socket had already dropped cancelled no timers, freed no
   -- socket-index entry and fired no "disconnected" callback - it left a record
   -- that the keep-alive sweep kept walking forever.
   local tWasConnected
   put mqttIsConnected(pHost, pPort) into tWasConnected

   -- Mark as disconnected and disable auto-reconnect FIRST, so the socketClosed
   -- that our own close provokes cannot turn a user disconnect into a reconnect.
   put false into gMQTTConnections[tConnID]["connected"]
   put false into gMQTTConnections[tConnID]["autoReconnect"]
   put true into gMQTTConnections[tConnID]["closing"]

   if tWasConnected then
      -- Save persistent messages if enabled
      if gMQTTPersistentStoreEnabled then
         __savePersistentMessages tConnID
      end if

      -- Send DISCONNECT packet (required by MQTT spec for clean disconnect)
      local tPacket, tErr
      put __numToByte(0xE0) & __numToByte(0) into tPacket

      if __isSocketOpen(tConnID) then
         put __writeSocket(tConnID, tPacket) into tErr
         if tErr is not empty then
            __mqttDebug "Error writing DISCONNECT packet:" && tErr
         end if
      end if
   end if

   -- Cancel any pending messages
   __cancelPendingMessages tConnID

   -- Close the socket
   if __isSocketOpen(tConnID) then
      local tCloseErr
      try
         close socket tConnID
      catch tCloseErr
         __mqttDebug "Error closing socket:" && tCloseErr
      end try
   end if

   -- Clean up socket mapping and any half-parsed inbound bytes
   delete variable gMQTTConnIndex[tConnID]
   put empty into gMQTTConnections[tConnID]["receiveBuffer"]

   __mqttLog "Disconnected from" && pHost & ":" & pPort

   -- Notify state change
   __invokeStateChangeCallback "disconnected", pHost, pPort, "clean disconnect"
end mqttDisconnect

-- ========================================
-- PUBLIC API - PUBLISHING
-- ========================================

function mqttPublish pHost, pPort, pTopic, pMessage, pQoS, pRetain
   local tConnID
   put pHost & ":" & pPort into tConnID
   
   -- Verify connection is active
   if not mqttIsConnected(pHost, pPort) then
      return "ERROR: Not connected"
   end if
   
   -- Set defaults
   if pQoS is empty then put 0 into pQoS
   if pRetain is empty then put false into pRetain
   
   -- Validate QoS level
   if (pQoS < 0) or (pQoS > 2) then
      return "ERROR: Invalid QoS (use 0, 1, or 2)"
   end if
   
   -- Validate topic is not empty (MQTT spec requirement)
   if pTopic is empty then
      return "ERROR: Topic cannot be empty"
   end if

   -- Validate topic doesn't contain wildcards (not allowed in PUBLISH). Asked of
   -- the TEXT, not of its UTF-8 bytes: "+" and "#" are single ASCII bytes that
   -- can never appear inside a multi-byte sequence, so the answer is the same,
   -- and asking the text keeps the check readable.
   if (pTopic contains "+") or (pTopic contains "#") then
      return "ERROR: Topic cannot contain wildcards"
   end if

   -- Encode topic as UTF-8 (MQTT spec requirement)
   local tTopicUTF8
   put textEncode(pTopic, "UTF-8") into tTopicUTF8

   -- The topic goes into a 16-bit length-prefixed field
   if the number of bytes of tTopicUTF8 > 65535 then
      return "ERROR: Topic exceeds 65535 bytes"
   end if

   -- Refuse a packet the protocol cannot frame BEFORE building it: Remaining
   -- Length is a 4-byte varint capped at 268435455, and __encodeRemainingLength
   -- would otherwise emit a fifth byte the broker reads as garbage.
   local tPayloadBytes
   put the number of bytes of pMessage into tPayloadBytes

   if (tPayloadBytes + the number of bytes of tTopicUTF8 + 4) > 268435455 then
      return "ERROR: Message exceeds MQTT maximum packet size"
   end if

   -- Build PUBLISH packet
   local tPacket
   put __buildPublishPacket(tConnID, tTopicUTF8, pMessage, pQoS, pRetain) into tPacket

   if tPacket is empty then
      return "ERROR: Failed to build packet"
   end if

   if not __isSocketOpen(tConnID) then
      return "ERROR: Socket not open"
   end if

   -- Send the packet
   local tError
   put __writeSocket(tConnID, tPacket) into tError
   if tError is not empty then
      -- The message never left. Drop the acknowledgment record with it: a
      -- pendingAcks entry for a PUBLISH that was not sent would be
      -- retransmitted as a DUP on the next reconnect, delivering a message the
      -- caller was already told had failed.
      if tPacketID is not empty and tPacketID > 0 then
         delete variable gMQTTConnections[tConnID]["pendingAcks"][tPacketID]
      end if
      return __failWrite(tConnID, "PUBLISH", tError)
   end if

   put the milliseconds into gMQTTConnections[tConnID]["lastActivity"]

   add 1 to gMQTTConnections[tConnID]["stats"]["messagesSent"]
   add the number of bytes of tPacket to gMQTTConnections[tConnID]["stats"]["bytesSent"]

   if (pQoS > 0) and gMQTTPersistentStoreEnabled then
      __savePersistentMessages tConnID
   end if

   __mqttLog "Published:" && pTopic && "(" & tPayloadBytes && "bytes, QoS" && pQoS & ")"
   return "OK"
end mqttPublish

-- ========================================
-- PUBLIC API - SUBSCRIPTION
-- ========================================

function mqttSubscribe pHost, pPort, pTopic, pQoS
   local tConnID
   put pHost & ":" & pPort into tConnID
   
   -- Verify connection is active
   if not mqttIsConnected(pHost, pPort) then
      return "ERROR: Not connected"
   end if
   
   -- Set defaults
   if pQoS is empty then put 0 into pQoS

   -- Validate QoS level
   if pQoS is not a number then
      return "ERROR: Invalid QoS (use 0, 1, or 2)"
   end if

   if (pQoS < 0) or (pQoS > 2) then
      return "ERROR: Invalid QoS (use 0, 1, or 2)"
   end if

   -- A SUBSCRIBE carrying no topic filter is a protocol error the broker answers
   -- by dropping the connection, so refuse it here where the caller can see it.
   if pTopic is empty then
      return "ERROR: Topic filter cannot be empty"
   end if

   -- Encode topic as UTF-8
   local tTopicUTF8
   put textEncode(pTopic, "UTF-8") into tTopicUTF8

   if the number of bytes of tTopicUTF8 > 65535 then
      return "ERROR: Topic filter exceeds 65535 bytes"
   end if

   -- Build SUBSCRIBE packet
   local tPacket
   put __buildSubscribePacket(tConnID, tTopicUTF8, pQoS) into tPacket

   if tPacket is empty then
      return "ERROR: Failed to build packet"
   end if

   if not __isSocketOpen(tConnID) then
      return "ERROR: Socket not open"
   end if

   -- Send the packet
   local tError
   put __writeSocket(tConnID, tPacket) into tError
   if tError is not empty then
      return __failWrite(tConnID, "SUBSCRIBE", tError)
   end if

   put the milliseconds into gMQTTConnections[tConnID]["lastActivity"]
   __mqttLog "Subscribed to:" && pTopic && "(QoS" && pQoS & ")"

   -- Store subscription for reconnection
   put pQoS into gMQTTConnections[tConnID]["subscriptions"][pTopic]

   return "OK"
end mqttSubscribe

function mqttUnsubscribe pHost, pPort, pTopic
   local tConnID
   put pHost & ":" & pPort into tConnID
   
   -- Verify connection is active
   if not mqttIsConnected(pHost, pPort) then
      return "ERROR: Not connected"
   end if
   
   if pTopic is empty then
      return "ERROR: Topic filter cannot be empty"
   end if

   -- Encode topic as UTF-8
   local tTopicUTF8
   put textEncode(pTopic, "UTF-8") into tTopicUTF8

   if the number of bytes of tTopicUTF8 > 65535 then
      return "ERROR: Topic filter exceeds 65535 bytes"
   end if

   -- Build UNSUBSCRIBE packet
   local tPacket
   put __buildUnsubscribePacket(tConnID, tTopicUTF8) into tPacket

   if tPacket is empty then
      return "ERROR: Failed to build packet"
   end if

   if not __isSocketOpen(tConnID) then
      return "ERROR: Socket not open"
   end if

   -- Send the packet
   local tError
   put __writeSocket(tConnID, tPacket) into tError
   if tError is not empty then
      return __failWrite(tConnID, "UNSUBSCRIBE", tError)
   end if

   put the milliseconds into gMQTTConnections[tConnID]["lastActivity"]
   __mqttLog "Unsubscribed from:" && pTopic

   -- Remove from subscription list
   delete variable gMQTTConnections[tConnID]["subscriptions"][pTopic]

   return "OK"
end mqttUnsubscribe

-- ========================================
-- PUBLIC API - UTILITIES
-- ========================================

function mqttGetConnectionInfo pHost, pPort
   local tConnID
   put pHost & ":" & pPort into tConnID
   
   if gMQTTConnections[tConnID] is empty then
      return empty
   end if
   
   local tInfo
   put gMQTTConnections[tConnID] into tInfo
   return tInfo
end mqttGetConnectionInfo

function mqttGetStatistics pHost, pPort
   local tConnID
   put pHost & ":" & pPort into tConnID
   
   if gMQTTConnections[tConnID] is empty then
      return empty
   end if
   
   return gMQTTConnections[tConnID]["stats"]
end mqttGetStatistics

command mqttResetStatistics pHost, pPort
   local tConnID
   put pHost & ":" & pPort into tConnID
   
   if gMQTTConnections[tConnID] is not empty then
      put 0 into gMQTTConnections[tConnID]["stats"]["messagesSent"]
      put 0 into gMQTTConnections[tConnID]["stats"]["messagesReceived"]
      put 0 into gMQTTConnections[tConnID]["stats"]["bytesSent"]
      put 0 into gMQTTConnections[tConnID]["stats"]["bytesReceived"]
      put 0 into gMQTTConnections[tConnID]["stats"]["reconnections"]
   end if
end mqttResetStatistics

function mqttGetSessionPresent pHost, pPort
   local tConnID
   put pHost & ":" & pPort into tConnID
   return gMQTTConnections[tConnID]["sessionPresent"]
end mqttGetSessionPresent

function mqttGetSubscriptions pHost, pPort
   local tConnID
   put pHost & ":" & pPort into tConnID
   
   if gMQTTConnections[tConnID]["subscriptions"] is empty then
      return empty
   end if
   
   return the keys of gMQTTConnections[tConnID]["subscriptions"]
end mqttGetSubscriptions

function mqttGetConnections
   if gMQTTConnections is empty then
      return empty
   end if
   return the keys of gMQTTConnections
end mqttGetConnections

command mqttCleanupAll
   __mqttLog "Cleaning up all MQTT connections..."
   
   local tPacket, tErr
   if gMQTTConnections is not empty then
      repeat for each key tConnID in gMQTTConnections
         -- Mark as disconnected and disable auto-reconnect
         put false into gMQTTConnections[tConnID]["connected"]
         put false into gMQTTConnections[tConnID]["autoReconnect"]
         put true into gMQTTConnections[tConnID]["closing"]

         -- Cancel pending messages
         __cancelPendingMessages tConnID

         -- Save persistent messages
         if gMQTTPersistentStoreEnabled then
            __savePersistentMessages tConnID
         end if

         -- Send DISCONNECT and close socket
         if __isSocketOpen(tConnID) then
            put __numToByte(0xE0) & __numToByte(0) into tPacket
            -- A failed DISCONNECT is not worth reporting on a teardown path,
            -- but it must not skip the close that follows it.
            put __writeSocket(tConnID, tPacket) into tErr
            __closeSocketQuietly tConnID
         end if
      end repeat

      __mqttLog "All connections cleaned up"
   end if

   -- Clear all connection data and mappings unconditionally: gMQTTConnIndex and
   -- the timer-token map can hold entries for a connection whose record has
   -- already gone, and leaving those behind is how a torn-down socket id keeps
   -- resolving to a live-looking connection.
   put empty into gMQTTConnections
   put empty into gMQTTConnIndex
   put empty into gMQTTTimerTokens
end mqttCleanupAll

function mqttTestLibrary
   return "MQTT Library v2.12.8 loaded successfully"
end mqttTestLibrary

function mqttSelfTest
   -- tFailures accumulates the detail lines; tResults is built at the END from
   -- them. They used to be ONE variable, and the summary block opened with
   -- `put "MQTT Library Self-Test" & return INTO tResults` - which discarded
   -- every failure line the run had just recorded. The counts still said
   -- "Failed: 3" and the report never said which three.
   local tResults, tFailures, tPassed, tFailed
   put 0 into tPassed
   put 0 into tFailed

   -- Test 1: Remaining length encoding
   local tEncoded
   put __encodeRemainingLength(127) into tEncoded
   if tEncoded is __numToByte(127) then
      add 1 to tPassed
   else
      add 1 to tFailed
      put "FAIL: encodeRemainingLength(127)" & return after tFailures
   end if
   
   put __encodeRemainingLength(128) into tEncoded
   if the number of bytes of tEncoded is 2 then
      add 1 to tPassed
   else
      add 1 to tFailed
      put "FAIL: encodeRemainingLength(128)" & return after tFailures
   end if
   
   -- Test 2: Remaining length decoding
   local tDecoded
   put __decodeRemainingLength(__numToByte(127) & "xxx", 1) into tDecoded
   if item 1 of tDecoded is 127 then
      add 1 to tPassed
   else
      add 1 to tFailed
      put "FAIL: decodeRemainingLength(127)" & return after tFailures
   end if
   
   -- Test 3: UTF-8 encoding
   local tUTF8
   put __encodeUTF8String("Hello") into tUTF8
   if the number of bytes of tUTF8 is 7 then
      add 1 to tPassed
   else
      add 1 to tFailed
      put "FAIL: encodeUTF8String" & return after tFailures
   end if
   
   -- Test 4: Socket validation
   if __isSocketOpen("nonexistent:1883") is false then
      add 1 to tPassed
   else
      add 1 to tFailed
      put "FAIL: isSocketOpen validation" & return after tFailures
   end if
   
   -- Test 5: Filename sanitization
   local tSanitized
   put __sanitizeFilename("test:host/path?query") into tSanitized
   local tHasColon, tHasSlash, tHasQuestion
   put ":" is in tSanitized into tHasColon
   put "/" is in tSanitized into tHasSlash
   put "?" is in tSanitized into tHasQuestion
   if not tHasColon and not tHasSlash and not tHasQuestion then
      add 1 to tPassed
   else
      add 1 to tFailed
      put "FAIL: sanitizeFilename" & return after tFailures
   end if
   
   -- Test 6: Keep-alive threshold
   if gMQTTKeepAliveThreshold is a number and gMQTTKeepAliveThreshold > 0 \
         and gMQTTKeepAliveThreshold <= 1 then
      add 1 to tPassed
   else
      add 1 to tFailed
      put "FAIL: keep-alive threshold validation" & return after tFailures
   end if
   
   -- Test 7: Remaining-length round trip across every multi-byte boundary.
   -- This replaces a test that asked `if gMQTTConnIndex is not empty or
   -- gMQTTConnIndex is empty` - a tautology that passed on every build and
   -- could never have reported the thing its own FAIL line named.
   -- The cases are held in an ARRAY, walked by index. A comma-delimited list
   -- would read correctly only while the host application happens to leave the
   -- itemDelimiter at comma - it is global mutable state, and the rest of this
   -- library deliberately avoids depending on it (see __parseIncomingData).
   local tRoundTrip, tCases, tCase, tRT, tRTComma, tIndex
   put "0" into tCases[1]
   put "1" into tCases[2]
   put "127" into tCases[3]
   put "128" into tCases[4]
   put "16383" into tCases[5]
   put "16384" into tCases[6]
   put "2097151" into tCases[7]
   put "2097152" into tCases[8]
   put "268435455" into tCases[9]

   put true into tRoundTrip
   repeat with tIndex = 1 to 9
      put tCases[tIndex] into tCase
      put __decodeRemainingLength(__encodeRemainingLength(tCase), 1) into tRT
      put offset(comma, tRT) into tRTComma
      if tRTComma = 0 or (char 1 to (tRTComma - 1) of tRT) is not tCase then
         put false into tRoundTrip
         put "FAIL: remaining-length round trip for" && tCase & return after tFailures
      end if
   end repeat

   if tRoundTrip then
      add 1 to tPassed
   else
      add 1 to tFailed
   end if

   -- Test 8: byte-exact framing. __encodeString must prefix the BYTE count, not
   -- the character count, or every non-ASCII topic desynchronises the broker.
   -- Two U+00E9 are 2 characters and 4 bytes; the prefix must read 4.
   local tEncodedStr
   put __encodeUTF8String(numToCodepoint(233) & numToCodepoint(233)) into tEncodedStr
   if (the number of bytes of tEncodedStr) is 6 and byteToNum(byte 2 of tEncodedStr) is 4 then
      add 1 to tPassed
   else
      add 1 to tFailed
      put "FAIL: encodeString byte length (got" && the number of bytes of tEncodedStr && "bytes)" & return after tFailures
   end if

   -- Summary
   put "MQTT Library Self-Test" & return into tResults
   put "=====================" & return after tResults
   put "Version: 2.12.8" & return after tResults
   put "Passed:" && tPassed & return after tResults
   put "Failed:" && tFailed & return after tResults

   if tFailed = 0 then
      put return & "All tests PASSED" after tResults
   else
      put return & "Some tests FAILED:" & return after tResults
      put tFailures after tResults
   end if

   return tResults
end mqttSelfTest

function mqttBenchmark pHost, pPort, pMessageCount, pMessageSize
   if pMessageCount is empty then put 1000 into pMessageCount
   if pMessageSize is empty then put 100 into pMessageSize

   if pMessageCount is not a number or pMessageCount < 1 then
      return "ERROR: Message count must be a positive number"
   end if

   if pMessageSize is not a number or pMessageSize < 1 then
      return "ERROR: Message size must be a positive number"
   end if

   local tConnID
   put pHost & ":" & pPort into tConnID

   if not mqttIsConnected(pHost, pPort) then
      return "ERROR: Not connected to" && pHost & ":" & pPort
   end if

   local tResults, tTestMessage
   
   -- Build test message
   repeat pMessageSize times
      put "X" after tTestMessage
   end repeat
   
   -- Reset statistics
   mqttResetStatistics pHost, pPort
   
   -- Benchmark header
   put "MQTT Performance Benchmark" & return into tResults
   put "=========================" & return after tResults
   put "Host:" && pHost & ":" & pPort & return after tResults
   put "Messages:" && pMessageCount & return after tResults
   put "Message Size:" && pMessageSize && "bytes" & return & return after tResults
   
   local tStartTime, tEndTime, tDuration
   put the milliseconds into tStartTime
   
   -- Send messages
   repeat pMessageCount times
      get mqttPublish(pHost, pPort, "benchmark/test", tTestMessage, 0, false)
   end repeat
   
   put the milliseconds into tEndTime
   put tEndTime - tStartTime into tDuration
   
   -- Calculate results. `the milliseconds` has millisecond granularity, so a
   -- small fast run really can measure zero elapsed time - and every rate below
   -- divides by it. Clamp to one tick rather than throwing a divide-by-zero out
   -- of a benchmark, and say so in the report.
   local tStats, tThroughput, tLatency, tBandwidth, tClamped
   put mqttGetStatistics(pHost, pPort) into tStats

   put false into tClamped
   if tDuration <= 0 then
      put 1 into tDuration
      put true into tClamped
   end if

   put (pMessageCount / (tDuration / 1000)) into tThroughput
   put (tDuration / pMessageCount) into tLatency
   put ((tStats["bytesSent"] / 1024) / (tDuration / 1000)) into tBandwidth

   put "Results:" & return after tResults
   put "--------" & return after tResults
   put "Duration:" && tDuration && "ms" & return after tResults
   put "Messages Sent:" && tStats["messagesSent"] & return after tResults
   put "Bytes Sent:" && tStats["bytesSent"] & return after tResults
   put "Throughput:" && format("%.2f", tThroughput) && "msg/sec" & return after tResults
   put "Latency:" && format("%.2f", tLatency) && "ms/msg" & return after tResults
   put "Bandwidth:" && format("%.2f", tBandwidth) && "KB/sec" & return after tResults

   if tClamped then
      put return & "NOTE: elapsed time measured as 0 ms; rates computed against" && \
            "a 1 ms floor and are a LOWER BOUND on real throughput." & return after tResults
   end if

   return tResults
end mqttBenchmark

-- ========================================
-- SOCKET CALLBACKS (PUBLIC - REQUIRED)
-- ========================================
--
-- Two DIFFERENT kinds of handler live in this section, and conflating them is
-- what made auto-reconnect dead code for the life of the library.
--
--   mqttSocketConnected / mqttSocketDataAvailable are OURS. The engine calls
--   them only because `open socket ... with message` and `read from socket ...
--   with message` name them explicitly, so any name would do.
--
--   socketClosed / socketError / socketTimeout are the ENGINE'S. It sends
--   those three names and no others when a socket drops, fails or goes idle.
--   This library used to define `on mqttSocketClosed` and `on mqttSocketError`
--   and nothing ever sent them - so a dropped connection fired no state-change
--   callback, freed no timer, and could not trigger the auto-reconnect the
--   library advertised. They are declared under the engine's names now.
--
-- The logic behind each engine message sits in an mqttSocket*-prefixed FUNCTION
-- answering one question - was that socket mine, and did I handle it? - with
-- the `on` handler reduced to dispatch. Those three names belong to the engine,
-- so every socket library in the message path declares them and no script may
-- define one twice; keeping the logic behind a name of its own lets an app that
-- runs its own sockets drop these three wrappers and call the functions from
-- its own handlers instead. A message that is not ours is PASSED, never
-- swallowed: eating another library's socketClosed is a silent hang for it.

on mqttSocketConnected pSocketID
   local tConnID

   -- Look up the connection ID from the socket ID mapping
   put gMQTTConnIndex[pSocketID] into tConnID

   -- If not found in mapping, assume socket ID equals connection ID
   if tConnID is empty then
      put pSocketID into tConnID
      put tConnID into gMQTTConnIndex[pSocketID]
   end if

   __mqttLog "Socket connected:" && tConnID && "(socket:" && pSocketID & ")"

   -- Validate connection record exists
   if gMQTTConnections[tConnID] is empty then
      __mqttLog "ERROR: No connection record for" && tConnID
      __closeSocketQuietly pSocketID
      delete variable gMQTTConnIndex[pSocketID]
      exit mqttSocketConnected
   end if

   -- Apply TLS if configured
   local tError
   if gMQTTConnections[tConnID]["useTLS"] is true then
      __mqttLog "Securing socket with TLS..."
      try
         if gMQTTConnections[tConnID]["verifyTLS"] is true then
            -- Honor certificate verification and optional CA bundle
            if gMQTTConnections[tConnID]["caCertPath"] is not empty then
               set the sslCertificates to gMQTTConnections[tConnID]["caCertPath"]
            end if
            secure socket pSocketID with verification
            __mqttLog "Socket secured with TLS (verified)"
         else
            -- Reachable only when the caller explicitly asked for it; the
            -- default is now verified. An unverified TLS socket is encrypted
            -- against a passive listener and worthless against an active one.
            secure socket pSocketID without verification
            __mqttLog "WARNING: socket secured with TLS but the certificate was NOT verified"
         end if
      catch tError
         -- A failed handshake used to close the socket and return, leaving the
         -- record marked "connecting" forever with nothing scheduled to change
         -- it. Tear it down through the normal path so the state-change
         -- callback fires and auto-reconnect gets its chance.
         __mqttLog "ERROR securing TLS:" && tError
         __cleanupConnection tConnID, "TLS error:" && tError
         __handleReconnect tConnID
         exit mqttSocketConnected
      end try
   end if

   -- Build and send CONNECT packet
   local tPacket
   put __buildConnectPacket(tConnID) into tPacket

   if tPacket is empty then
      __mqttLog "ERROR: could not build CONNECT packet for" && tConnID
      __cleanupConnection tConnID, "CONNECT packet could not be built"
      exit mqttSocketConnected
   end if

   put __writeSocket(pSocketID, tPacket) into tError
   if tError is empty then
      __mqttLog "CONNECT packet sent"
   else
      -- Without a CONNECT the broker will never answer, so there is nothing to
      -- read; the old code logged this and armed the read anyway, producing a
      -- connection that sat silent until the user gave up on it.
      __mqttLog "ERROR sending CONNECT:" && tError
      __cleanupConnection tConnID, "CONNECT write failed:" && tError
      __handleReconnect tConnID
      exit mqttSocketConnected
   end if

   -- Start reading from socket
   local tArmError
   put __armRead(pSocketID) into tArmError
   if tArmError is not empty then
      __mqttLog "ERROR starting read:" && tArmError
      __cleanupConnection tConnID, "read arm failed:" && tArmError
      __handleReconnect tConnID
   end if
end mqttSocketConnected

on mqttSocketDataAvailable pSocketID, pData
   local tConnID, tBytes

   -- Map socket ID to connection ID
   put gMQTTConnIndex[pSocketID] into tConnID
   if tConnID is empty then put pSocketID into tConnID

   put the number of bytes of pData into tBytes
   __mqttDebug ">>> SOCKET CALLBACK: mqttSocketDataAvailable" && tConnID && "(" & tBytes && "bytes)"

   -- Validate connection exists. The socket outlives the record when a
   -- disconnect races an in-flight read; leaving it open with no read armed
   -- leaks a file descriptor for the life of the process, so close it.
   if gMQTTConnections[tConnID] is empty then
      __mqttDebug "Connection gone, closing orphaned socket"
      __closeSocketQuietly pSocketID
      delete variable gMQTTConnIndex[pSocketID]
      exit mqttSocketDataAvailable
   end if

   if tBytes > 0 then
      -- Update statistics
      add tBytes to gMQTTConnections[tConnID]["stats"]["bytesReceived"]

      -- Parse incoming data
      __parseIncomingData tConnID, pData
   end if

   -- Continue reading. Both tests matter: the parse above can dispatch a
   -- message into application code, and that code is entitled to call
   -- mqttDisconnect - after which re-arming would resurrect the connection.
   if not __isSocketOpen(pSocketID) then
      __mqttDebug "Socket closed during read, stopping read loop"
      exit mqttSocketDataAvailable
   end if

   if gMQTTConnections[tConnID] is empty then
      __mqttDebug "Connection torn down by callback, stopping read loop"
      exit mqttSocketDataAvailable
   end if

   local tArmError
   put __armRead(pSocketID) into tArmError
   if tArmError is not empty then
      __mqttLog "ERROR continuing read:" && tArmError
      __cleanupConnection tConnID, "read error:" && tArmError
      __handleReconnect tConnID
   end if
end mqttSocketDataAvailable

-- Was pSocketID one of OUR sockets, and did we handle the close? True when
-- consumed, false when it belongs to somebody else.
function mqttSocketClosed pSocketID
   local tConnID
   put gMQTTConnIndex[pSocketID] into tConnID
   if tConnID is empty then
      -- Unknown id: not ours. Note the deliberate asymmetry with the two OUR-
      -- message handlers above, which fall back to "socket id is the connection
      -- id" - guessing here would let us consume another library's close.
      return false
   end if

   __mqttDebug ">>> ENGINE MESSAGE: socketClosed" && tConnID

   -- Clean up the socket ID mapping first, so nothing below can re-enter
   -- through a lookup that still resolves.
   delete variable gMQTTConnIndex[pSocketID]

   if gMQTTConnections[tConnID] is empty then
      return true
   end if

   -- A close WE asked for (mqttConnect reopening, mqttDisconnect, cleanup) is
   -- not a connection failure. Without this the queued socketClosed arrives
   -- after the record has been rebuilt and schedules a reconnect against a
   -- connection that is already mid-connect - two sockets, doubling per retry.
   if gMQTTConnections[tConnID]["closing"] is true then
      put false into gMQTTConnections[tConnID]["closing"]
      __mqttDebug "Ignoring socketClosed for an intentional close of" && tConnID
      return true
   end if

   local tHost, tPort
   put gMQTTConnections[tConnID]["host"] into tHost
   put gMQTTConnections[tConnID]["port"] into tPort
   __mqttLog "Socket closed:" && tHost & ":" & tPort
   __cleanupConnection tConnID, "socket closed"
   __handleReconnect tConnID
   return true
end mqttSocketClosed

on socketClosed pSocketID
   if mqttSocketClosed(pSocketID) then
      exit socketClosed
   end if
   -- NOT OURS: pass it on. Swallowing another socket library's close message
   -- is a silent hang for that library, not a tidy no-op.
   pass socketClosed
end socketClosed

-- Was pSocketID one of OUR sockets, and did we handle the error?
function mqttSocketError pSocketID, pError
   local tConnID
   put gMQTTConnIndex[pSocketID] into tConnID
   if tConnID is empty then
      return false
   end if

   __mqttDebug ">>> ENGINE MESSAGE: socketError" && tConnID && "-" && pError

   delete variable gMQTTConnIndex[pSocketID]

   if gMQTTConnections[tConnID] is empty then
      return true
   end if

   -- An error on a socket we were closing anyway is the close, not a failure.
   if gMQTTConnections[tConnID]["closing"] is true then
      put false into gMQTTConnections[tConnID]["closing"]
      __mqttDebug "Ignoring socketError during an intentional close of" && tConnID
      return true
   end if

   local tHost, tPort
   put gMQTTConnections[tConnID]["host"] into tHost
   put gMQTTConnections[tConnID]["port"] into tPort
   __mqttLog "Socket error:" && tHost & ":" & tPort && "-" && pError
   __cleanupConnection tConnID, "socket error:" && pError
   __handleReconnect tConnID
   return true
end mqttSocketError

on socketError pSocketID, pError
   if mqttSocketError(pSocketID, pError) then
      exit socketError
   end if
   -- NOT OURS: pass it on (see socketClosed above).
   pass socketError
end socketError

-- Was pSocketID one of OUR sockets, and did we handle the timeout?
function mqttSocketTimeout pSocketID
   local tConnID
   put gMQTTConnIndex[pSocketID] into tConnID
   if tConnID is empty then
      return false
   end if

   if gMQTTConnections[tConnID] is empty then
      return true
   end if

   -- socketTimeout REPEATS every interval while a read or write is pending, and
   -- this library keeps a read armed on every live connection - so on a
   -- CONNECTED socket it means "idle", nothing more, and tearing down on it
   -- would make a healthy connection kill itself on schedule. It is only fatal
   -- before CONNACK, where it means the handshake stalled. Liveness on a
   -- connected socket is the keep-alive's job, not this message's.
   if gMQTTConnections[tConnID]["connected"] is true then
      __mqttDebug "Ignoring socketTimeout on connected socket" && tConnID
      return true
   end if

   if gMQTTConnections[tConnID]["closing"] is true then
      return true
   end if

   __mqttLog "Connection handshake timed out:" && tConnID
   __cleanupConnection tConnID, "handshake timeout"
   __handleReconnect tConnID
   return true
end mqttSocketTimeout

on socketTimeout pSocketID
   if mqttSocketTimeout(pSocketID) then
      exit socketTimeout
   end if
   -- NOT OURS: pass it on (see socketClosed above).
   pass socketTimeout
end socketTimeout

-- ========================================
-- CONNECTION MANAGEMENT (PRIVATE)
-- ========================================

private command __cleanupConnection pConnID, pReason
   if gMQTTConnections[pConnID] is empty then
      exit __cleanupConnection
   end if

   local tHost, tPort
   put gMQTTConnections[pConnID]["host"] into tHost
   put gMQTTConnections[pConnID]["port"] into tPort

   -- Mark as disconnected
   put false into gMQTTConnections[pConnID]["connected"]

   -- Cancel the keep-alive and any in-flight reconnect BEFORE anything else.
   -- This used to be missing, so a connection torn down by a socket error kept
   -- its keep-alive timer: it woke every interval, found the connection not
   -- connected, and rescheduled itself - one orphan timer per failure, kept
   -- alive for the life of the process.
   __cancelPendingMessages pConnID

   -- Drop half-parsed inbound bytes: the next connection on this key starts a
   -- new byte stream, and a leftover partial packet would frame it wrong.
   put empty into gMQTTConnections[pConnID]["receiveBuffer"]

   -- Close socket if still open. Flag it first so the socketClosed this
   -- provokes is recognised as ours and does not re-enter the failure path.
   if __isSocketOpen(pConnID) then
      put true into gMQTTConnections[pConnID]["closing"]
      __closeSocketQuietly pConnID
   end if

   -- Notify state change LAST: the callback runs application code, which may
   -- call straight back into this library, and it should see a settled record.
   __invokeStateChangeCallback "disconnected", tHost, tPort, pReason
end __cleanupConnection

-- Write a packet, and REPORT whether it actually went out.
-- Returns empty on success, or the error text.
--
-- WHY THIS EXISTS, and it is the same lesson the persistent store already
-- carries one level up: a failed socket write does not always throw. LiveCode
-- sets `the result`, so a `try`/`catch` around `write ... to socket` catches
-- nothing and the caller reports success on a write that did not happen. Every
-- one of this library's twelve socket writes was shaped that way.
--
-- IT MATTERS MOST FOR A BIG PACKET, which is where it was found: a PUBLISH
-- whose bytes are only partly written leaves the broker holding a fixed header
-- that promises N bytes and a stream that never delivers them. The broker then
-- reads everything the client sends afterwards as the missing tail of that one
-- packet - so it stops acknowledging, stops echoing, and answers no PINGREQ,
-- while the client sees clean writes and a socket the engine still calls open.
-- The connection is dead in one direction and nothing on either side says so.
private function __writeSocket pSocketID, pPacket
   -- CHUNKED, AND IT YIELDS BETWEEN CHUNKS. The yield is the part that took
   -- seven engine runs to find; the chunking on its own does not fix anything.
   --
   -- WHAT THE RUNS SHOWED (docs/ENGINE-NOTES.md 1.1). A synchronous write of a
   -- large PUBLISH completes in a flat 12-16 ms up to some size and then blocks
   -- for the ENTIRE socketTimeoutInterval, with the failing size moving between
   -- runs on the same broker. v2.12.3 read that as "the engine makes ONE
   -- non-blocking send() and never re-sends the tail", and chunked every write
   -- to 16 KB so each write would fit the kernel's send buffer.
   --
   -- THE SEVENTH RUN REFUTED IT. On mosquitto over a LAN, with 16 KB chunks:
   -- six chunks went out (98304 of 131116 bytes), then one chunk blocked for
   -- the full ten seconds. A second run got eleven chunks out (180224 of
   -- 204844) before the same wall. A 16 KB write that succeeds six times and
   -- then stalls is not a write that was too big for the buffer: the socket
   -- became unwritable and STAYED unwritable, which means the peer stopped
   -- reading.
   --
   -- WHY THE PEER STOPS READING, and this is the reading the yield is built on:
   -- the client is subscribed to the topic tree it is publishing to, so the
   -- broker is pushing the echo back while we are still writing. A synchronous
   -- write does not return to the engine's event loop, so nothing services the
   -- armed `read from socket`; our receive buffer fills, the broker's writes to
   -- us block, and a broker that cannot flush to us stops draining what we
   -- send. Both directions stall on each other. The hivemq runs do not
   -- contradict this - there the whole payload fitted the send buffer that a
   -- long round-trip path autotunes large, so the write never blocked and the
   -- event loop got back to reading. INFERRED: the discriminating experiment is
   -- a large publish to a topic NOTHING echoes back, which the conformance run
   -- now performs before the ladder.
   --
   -- SO: between chunks, hand the engine one turn of its event loop. That lets
   -- the pending read deliver, which drains our receive buffer, which unblocks
   -- the broker, which resumes draining us. It also stops a megabyte publish
   -- from freezing the UI and starving the keep-alive timers, which is worth
   -- having whatever the mechanism turns out to be.
   --
   -- THE YIELD IS RE-ENTRANT AND THAT IS THE HAZARD. Application code can run
   -- during it - a message callback that publishes, or disconnects. A second
   -- packet written into the middle of this one is precisely the corruption
   -- this function exists to prevent, so a multi-chunk write takes a lock and a
   -- re-entrant write is REFUSED rather than interleaved. Small packets (every
   -- control packet, and any publish that fits one chunk) never yield, never
   -- take the lock, and are byte-for-byte the code that ran before.
   --
   -- The lock lives in its own global, NOT in the connection record: the
   -- re-entrant code may call mqttCleanupAll, and writing a key back into a
   -- deleted record would recreate it as a zombie the keep-alive sweep walks
   -- forever.
   --
   -- Synchronous on purpose. `write ... with message` would not block the
   -- engine, but the engine discards a queued write on `close socket`, and
   -- both disconnect paths write DISCONNECT then close on the next line - an
   -- async DISCONNECT would be dropped and every clean disconnect would fire
   -- the Last Will. DISCONNECT is two bytes, so it never yields and never
   -- takes the lock; the guarantee those paths rely on is unchanged.
   --
   -- Returns empty on success, or the error text WITH how much had gone out,
   -- because "timeout after 131072 of 204800 bytes" is a measurement and
   -- "timeout" is a symptom.
   local tError, tRes, tTotal, tPos, tEnd, tChunkSize, tYields
   put the number of bytes of pPacket into tTotal
   if tTotal is 0 then
      return empty
   end if

   put gMQTTWriteChunkSize into tChunkSize
   if tChunkSize is not a number or tChunkSize < 512 then
      put 16384 into tChunkSize
   end if

   -- One chunk: the old path exactly, with no yield and no lock. Raising
   -- gMQTTWriteChunkSize above your largest payload restores the pre-2.12.6
   -- behaviour for every write.
   if tTotal <= tChunkSize then
      try
         write pPacket to socket pSocketID
      catch tError
         return tError && "after 0 of" && tTotal && "bytes"
      end try
      put the result into tRes
      if tRes is not empty then
         return tRes && "after 0 of" && tTotal && "bytes"
      end if
      return empty
   end if

   if gMQTTWriteLocks[pSocketID] is true then
      return "a large write is already in progress on this connection" && \
            "(refused rather than interleaved into it)"
   end if
   put true into gMQTTWriteLocks[pSocketID]

   put 0 into tYields
   put 1 into tPos
   repeat while tPos <= tTotal
      put min(tPos + tChunkSize - 1, tTotal) into tEnd
      try
         write (byte tPos to tEnd of pPacket) to socket pSocketID
      catch tError
         delete variable gMQTTWriteLocks[pSocketID]
         return tError && "after" && (tPos - 1) && "of" && tTotal && "bytes"
      end try

      put the result into tRes
      if tRes is not empty then
         delete variable gMQTTWriteLocks[pSocketID]
         return tRes && "after" && (tPos - 1) && "of" && tTotal && "bytes"
      end if
      put tEnd + 1 into tPos

      if tPos <= tTotal then
         -- A turn of the event loop. At the default zero this is a yield and
         -- not a delay: pending messages get their run and no time passes.
         -- mqttSetWriteYieldMs makes it a real pause, which is a diagnostic
         -- for the one reading the eighth run left standing.
         wait __writeYieldMs() milliseconds with messages
         add 1 to tYields

         -- The yield ran application code. It may have disconnected us, and
         -- writing the rest of a packet into a socket that has gone is how a
         -- reconnected connection inherits a corrupt stream.
         if not __isSocketOpen(pSocketID) then
            delete variable gMQTTWriteLocks[pSocketID]
            return "the socket closed mid-write, after" && (tPos - 1) && \
                  "of" && tTotal && "bytes"
         end if
      end if
   end repeat

   delete variable gMQTTWriteLocks[pSocketID]
   __mqttDebug "Wrote" && tTotal && "bytes in" && (tYields + 1) && \
         "chunks with" && tYields && "yields"
   return empty
end __writeSocket

-- A FAILED WRITE CORRUPTS THE STREAM, NOT JUST THE PACKET, so the only safe
-- response is to reset the connection - which is what this does, and what
-- the first version of the write check did not.
--
-- OBSERVED 2026-09-05, mosquitto on a LAN: a 131072-byte PUBLISH failed with
-- "timeout". __writeSocket reported it - that half worked - but mqttPublish
-- returned the error and left the connection up. From that instant the broker
-- was holding a fixed header promising bytes that never came, so every later
-- write (a PUBLISH, an UNSUBSCRIBE, a PINGREQ) was read as the missing tail
-- of that one packet. Nothing was acknowledged, nothing was echoed, and the
-- keep-alive would not have noticed for another ~135 seconds. A connection in
-- that state is dead in one direction and looks alive in both.
--
-- We cannot tell a write that sent nothing from one that sent half; the safe
-- assumption is half. Tearing down costs one reconnect. Not tearing down costs
-- a silently dead connection. Returns the error text for the caller to pass up.
private function __failWrite pConnID, pWhat, pError
   __mqttLog "ERROR writing" && pWhat & ":" && pError && \
         "- the stream may be corrupt; resetting the connection"
   __cleanupConnection pConnID, pWhat && "write failed:" && pError
   __handleReconnect pConnID
   return "ERROR:" && pError && "(connection reset)"
end __failWrite

private command __closeSocketQuietly pSocketID
   -- Closing an already-closed socket is not an error worth propagating, but it
   -- must not escape either: every caller here is on a teardown path where a
   -- throw would skip the rest of the cleanup.
   local tErr
   try
      close socket pSocketID
   catch tErr
      __mqttDebug "Error closing socket" && pSocketID & ":" && tErr
   end try
end __closeSocketQuietly

private function __armRead pSocketID
   -- No quantifier: the engine streams whatever bytes are available as they
   -- arrive. The old `read ... for 1 ...` asked for exactly one byte and cost
   -- one engine message dispatch PER BYTE - a 1 MB retained payload arrived as
   -- a million round trips through the message queue.
   -- Returns empty on success, or the error text.
   local tError
   try
      read from socket pSocketID with message "mqttSocketDataAvailable"
   catch tError
      return tError
   end try
   return empty
end __armRead

private command __cancelPendingMessages pConnID
   local tErr

   -- Cancel keep-alive timer
   if gMQTTConnections[pConnID]["keepAliveMessageID"] is not empty then
      try
         cancel gMQTTConnections[pConnID]["keepAliveMessageID"]
      catch tErr
         __mqttDebug "Could not cancel keep-alive message (already executed or invalid)"
      end try
      put empty into gMQTTConnections[pConnID]["keepAliveMessageID"]
   end if

   -- Cancel reconnect timer
   if gMQTTConnections[pConnID]["reconnectMessageID"] is not empty then
      try
         cancel gMQTTConnections[pConnID]["reconnectMessageID"]
      catch tErr
         __mqttDebug "Could not cancel reconnect message (already executed or invalid)"
      end try
      put empty into gMQTTConnections[pConnID]["reconnectMessageID"]
   end if

   -- Retire this connection's timer tokens. A cancelled message never fires, so
   -- its token would otherwise sit in the map forever; and if one DID fire
   -- between the cancel and here, retiring the token is what makes it a no-op.
   __releaseTimerTokens pConnID
end __cancelPendingMessages

private function __isSocketOpen pSocketID
   return (pSocketID is among the lines of the openSockets)
end __isSocketOpen

-- Asked only from libraryStack: "is this library stack actually in use?" A
-- copy EMBEDDED in an application's own stack script is never in stacksInUse
-- and does not need to be, so this question is not asked on that path.
private command __validateLibraryStack
   local tStackList
   put the stacksInUse into tStackList

   -- `does not contain` is not xTalk - the parser errors on `does`, which took
   -- the WHOLE script down with it, not just this handler. Negate the operator
   -- that does exist instead.
   if not (tStackList contains "libMQTTxt") then
      __mqttLog "WARNING: libMQTTxt not in stacksInUse - socket callbacks may not work"
      __mqttLog "Add 'start using stack" && quote & "libMQTTxt.oxtstack" & quote && "' to your startup code"
   end if
end __validateLibraryStack

private command __validateCallbackEnvironment
   -- Verify the callback target at least LOOKS like an object reference. There
   -- is no type-agnostic existence test in xTalk - `there is a ...` needs the
   -- object type spelled out - so this is a shape check and says so, rather
   -- than a probe dressed up as one.
   if gMQTTCallbackTarget is not empty then
      if not (gMQTTCallbackTarget contains "stack") and \
            not (gMQTTCallbackTarget contains "card") and \
            not (gMQTTCallbackTarget contains "button") and \
            not (gMQTTCallbackTarget contains "field") and \
            not (gMQTTCallbackTarget contains "group") then
         __mqttLog "WARNING: Callback target does not look like an object reference:" && gMQTTCallbackTarget
      end if
   end if
end __validateCallbackEnvironment

-- The object callbacks are dispatched to when the caller never set one.
-- Captured at REGISTRATION time, not at dispatch time: a setter is called from
-- the application's own script, so the defaultStack is that script's stack;
-- when a socket event arrives later it can be anything at all.
private function __defaultCallbackTarget
   local tTarget
   put the long id of the defaultStack into tTarget
   return tTarget
end __defaultCallbackTarget

private command __captureCallbackTarget
   if gMQTTCallbackTarget is not empty then
      exit __captureCallbackTarget
   end if

   put __defaultCallbackTarget() into gMQTTCallbackTarget
   __mqttDebug "No callback target set; defaulting to" && gMQTTCallbackTarget
end __captureCallbackTarget

-- ========================================
-- TIMER ROUTING (PRIVATE)
-- ========================================
--
-- Every scheduled message carries an integer TOKEN, and gMQTTTimerTokens maps
-- that token back to the one connection that scheduled it.
--
-- What this replaces: both timer handlers used to take no argument at all and
-- instead scan EVERY connection for a marker field equal to its own key. With
-- one connection that reads like an indirection; with two it is a bug, because
-- both connections set their marker and whichever timer fires first services
-- BOTH - so a connection with a 10-second keep-alive was pinged on its
-- neighbour's 300-second schedule and dropped by the broker for being idle.
--
-- The marker existed to avoid passing "host:port" through `send`, where the
-- colon is a real parsing hazard. An integer has no such hazard, so the token
-- is an integer.

-- The PINGREQ threshold, guarded. libraryStack sets the global, but a handler
-- reached before initialisation would otherwise multiply by empty - which is
-- zero, and a zero threshold means "ping constantly".
-- THE LIBRARY INITIALISES ITSELF BEFORE IT TOUCHES THE NETWORK.
--
-- Fifth engine run, 2026-09-06: a fresh engine session opened the demo and its
-- boot self-check reported the keep-alive threshold EMPTY - the embedding
-- stack's preOpenStack had not initialised the library. Every earlier run had
-- inherited populated globals from a standalone-library session in the same
-- engine, so the embedded initialisation path had never actually been
-- exercised. Had that connection succeeded, the first inbound byte would have
-- met `the number of bytes of tBuffer > gMQTTMaxBufferSize` with an EMPTY
-- limit - a string comparison that is always true - and been torn down as a
-- buffer overflow. A library must not have a failure mode that depends on the
-- host calling one line in one specific handler; this is the backstop.
private command __ensureInit
   if gMQTTInitialized is not true then
      -- Say so. The sixth engine run (docs/ENGINE-NOTES.md 2.1) could only
      -- INFER that this path had saved the connection, because nothing logged
      -- it; an embedder whose preOpenStack did not take effect should be told
      -- in the log rather than left to deduce it from a missing first line.
      __mqttLog "Library initialised on first use - mqttInitialize had not" && \
            "run (an embedding stack should call it from preOpenStack)"
      mqttInitialize
   end if
end __ensureInit

-- The receive-buffer ceiling, guarded like the keep-alive threshold below and
-- for the same reason: an empty global compares as text, and "always true" on
-- an overflow check tears down every connection on its first byte.
private function __maxBufferSize
   if gMQTTMaxBufferSize is not a number or gMQTTMaxBufferSize < 1 then
      return 5242880
   end if
   return gMQTTMaxBufferSize
end __maxBufferSize

-- Guarded like the ceiling and the threshold: an empty global here would reach
-- `wait empty milliseconds`, and the write path is the last place that should
-- depend on a host having called an initialiser.
private function __writeYieldMs
   if gMQTTWriteYieldMs is not a number or gMQTTWriteYieldMs < 0 then
      return 50
   end if
   return gMQTTWriteYieldMs
end __writeYieldMs

private function __keepAliveThreshold
   if gMQTTKeepAliveThreshold is not a number then
      return 0.75
   end if

   if gMQTTKeepAliveThreshold <= 0 or gMQTTKeepAliveThreshold > 1 then
      return 0.75
   end if

   return gMQTTKeepAliveThreshold
end __keepAliveThreshold

private function __newTimerToken pConnID
   add 1 to gMQTTTimerSeq
   put pConnID into gMQTTTimerTokens[gMQTTTimerSeq]
   return gMQTTTimerSeq
end __newTimerToken

private command __releaseTimerTokens pConnID
   -- Collect first, delete after: `repeat for each key` walks the array being
   -- modified, and deleting from underneath it is undefined. The doomed list is
   -- one line per token, built with cr rather than a delimiter this library does
   -- not control (the lineDelimiter is global state, so the walk below uses
   -- `for each line`, which reads it once and consistently).
   local tToken, tDoomed
   repeat for each key tToken in gMQTTTimerTokens
      if gMQTTTimerTokens[tToken] is pConnID then
         put tToken & cr after tDoomed
      end if
   end repeat

   repeat for each line tToken in tDoomed
      delete variable gMQTTTimerTokens[tToken]
   end repeat
end __releaseTimerTokens

-- Resolve a fired timer's token to its connection, retiring the token as it
-- goes: a token is good for exactly one firing. Returns empty for a token that
-- has been cancelled, already consumed, or belongs to a connection since gone.
private function __claimTimerToken pToken
   local tConnID
   put gMQTTTimerTokens[pToken] into tConnID
   delete variable gMQTTTimerTokens[pToken]

   if tConnID is empty then
      return empty
   end if

   if gMQTTConnections[tConnID] is empty then
      return empty
   end if

   return tConnID
end __claimTimerToken

-- ========================================
-- RECONNECTION HANDLER (PRIVATE - except timer handler)
-- ========================================

private command __handleReconnect pConnID
   if gMQTTConnections[pConnID] is empty then
      exit __handleReconnect
   end if

   if gMQTTConnections[pConnID]["autoReconnect"] is not true then
      __mqttLog "Auto-reconnect disabled"
      exit __handleReconnect
   end if

   -- Already scheduled: leave the pending attempt alone rather than cancelling
   -- and re-scheduling it. socketClosed and socketError can both land for one
   -- failure, and re-scheduling on each would restart the backoff every time.
   if gMQTTConnections[pConnID]["reconnectMessageID"] is not empty then
      __mqttDebug "Reconnect already scheduled for" && pConnID
      exit __handleReconnect
   end if

   local tHost, tPort, tAttempts, tExponent, tDelay
   put gMQTTConnections[pConnID]["host"] into tHost
   put gMQTTConnections[pConnID]["port"] into tPort
   put gMQTTConnections[pConnID]["reconnectAttempts"] into tAttempts

   if tAttempts is not a number or tAttempts < 0 then
      put 0 into tAttempts
   end if

   -- Calculate delay with exponential backoff (capped at 30 seconds). The
   -- EXPONENT is clamped, not the count: the count grows without bound across
   -- a long outage, and 2 ^ a few thousand is an overflow, not a big number.
   -- They were one variable until the sixth engine run (docs/ENGINE-NOTES.md
   -- 1.5) logged "attempt 17" forty times in a row while the real count kept
   -- climbing - the clamp had been applied to the number being reported.
   put tAttempts into tExponent
   if tExponent > 16 then
      put 16 into tExponent
   end if

   put min(2 ^ tExponent, 30) into tDelay

   -- Add random jitter to prevent thundering herd
   local tJitter
   put (random(501) - 251) / 1000 into tJitter
   put tDelay + tJitter into tDelay

   -- Jitter is +/- 0.25s and the first backoff is 1s, so this cannot go
   -- negative today; clamp anyway so a future tuning of either number cannot
   -- turn into a `send ... in -0.2 seconds`.
   if tDelay < 0.1 then
      put 0.1 into tDelay
   end if

   __mqttLog "Scheduling reconnect attempt" && (tAttempts + 1) && "in" && tDelay && "seconds"

   __scheduleReconnect pConnID, tDelay
end __handleReconnect

private command __scheduleReconnect pConnID, pDelay
   local tDelayMS, tMsgID, tToken
   put pDelay * 1000 into tDelayMS
   put __newTimerToken(pConnID) into tToken
   put __scheduleMessage("__executeReconnectTimer" && tToken, tDelayMS) into tMsgID

   -- Store the message ID for cancellation
   if tMsgID is not empty then
      put tMsgID into gMQTTConnections[pConnID]["reconnectMessageID"]
      __mqttDebug "Stored reconnect message ID:" && tMsgID && "for" && pConnID
   end if
end __scheduleReconnect

-- NOTE: This MUST be a message handler (on), not a command, to receive send
-- messages. It also CANNOT be private as it needs to be in the message path.
on __executeReconnectTimer pToken
   local tConnID
   put __claimTimerToken(pToken) into tConnID

   __mqttDebug ">>> RECONNECT TIMER FIRED for token" && pToken

   if tConnID is empty then
      __mqttDebug "Reconnect token" && pToken && "is stale, ignoring"
      exit __executeReconnectTimer
   end if

   -- The message has fired, so its ID is spent; clearing it here is what lets
   -- __handleReconnect schedule the NEXT attempt if this one fails.
   put empty into gMQTTConnections[tConnID]["reconnectMessageID"]

   -- Check if auto-reconnect is still enabled
   if gMQTTConnections[tConnID]["autoReconnect"] is not true then
      __mqttDebug "Auto-reconnect disabled for" && tConnID & ", aborting"
      exit __executeReconnectTimer
   end if

   -- Already back up (a manual mqttConnect beat the timer to it)
   if gMQTTConnections[tConnID]["connected"] is true then
      __mqttDebug "Already connected, skipping reconnect for" && tConnID
      exit __executeReconnectTimer
   end if

   local tHost, tPort
   put gMQTTConnections[tConnID]["host"] into tHost
   put gMQTTConnections[tConnID]["port"] into tPort

   __mqttLog "Executing reconnect to" && tHost & ":" & tPort

   local tResult
   put mqttReconnect(tHost, tPort) into tResult

   if tResult is "OK" then
      __mqttLog "Reconnection initiated"
   else
      __mqttLog "Reconnection failed:" && tResult
      -- Schedule the next attempt: a failure here produces no socket, so no
      -- socketError will arrive to drive the retry and the chain would stop
      -- dead on the first refused connection.
      __handleReconnect tConnID
   end if
end __executeReconnectTimer

private command __resubscribeAll pConnID
   if gMQTTConnections[pConnID]["subscriptions"] is empty then
      exit __resubscribeAll
   end if
   
   __mqttLog "Resubscribing to topics after reconnect"
   
   local tHost, tPort, tTopic, tQoS, tResult
   put gMQTTConnections[pConnID]["host"] into tHost
   put gMQTTConnections[pConnID]["port"] into tPort

   -- Resubscribe to each topic
   repeat for each key tTopic in gMQTTConnections[pConnID]["subscriptions"]
      put gMQTTConnections[pConnID]["subscriptions"][tTopic] into tQoS
      put mqttSubscribe(tHost, tPort, tTopic, tQoS) into tResult
   end repeat
end __resubscribeAll

-- ========================================
-- PERSISTENT STORE (PRIVATE)
-- ========================================

private command __ensurePersistentStoreDirectory
   local tStorePath, tError
   put gMQTTPersistentStorePath into tStorePath

   if tStorePath is empty then
      __mqttLog "ERROR: persistent store enabled with no path; disabling"
      put false into gMQTTPersistentStoreEnabled
      exit __ensurePersistentStoreDirectory
   end if

   if there is not a folder tStorePath then
      try
         create folder tStorePath
         -- `create folder` reports failure in `the result`, it does NOT throw -
         -- so the try block around it caught nothing and the store stayed
         -- "enabled" against a directory that was never made. Every later save
         -- then failed the same silent way.
         if the result is not empty then
            __mqttLog "ERROR creating persistent store directory:" && the result
            put false into gMQTTPersistentStoreEnabled
            exit __ensurePersistentStoreDirectory
         end if
         __mqttLog "Created persistent store directory:" && tStorePath
      catch tError
         __mqttLog "ERROR creating persistent store directory:" && tError
         put false into gMQTTPersistentStoreEnabled
         exit __ensurePersistentStoreDirectory
      end try
   end if

   -- Prove the directory is actually writable now, while there is a caller to
   -- tell, rather than at the first QoS 1 publish.
   local tProbe
   put tStorePath & "/.mqttwrite" into tProbe
   put "ok" into URL ("binfile:" & tProbe)
   if the result is not empty then
      __mqttLog "ERROR: persistent store directory is not writable:" && the result
      put false into gMQTTPersistentStoreEnabled
      exit __ensurePersistentStoreDirectory
   end if
   delete file tProbe
end __ensurePersistentStoreDirectory

private command __savePersistentMessages pConnID
   if not gMQTTPersistentStoreEnabled then
      exit __savePersistentMessages
   end if

   local tFilePath, tBlob, tSafeConnID, tError
   put __sanitizeFilename(pConnID) into tSafeConnID

   put gMQTTPersistentStorePath & "/" & tSafeConnID & ".dat" into tFilePath

   try
      put arrayEncode(gMQTTConnections[pConnID]["pendingAcks"]) into tBlob
      put tBlob into URL ("binfile:" & tFilePath)
      -- A failed URL write sets `the result` instead of throwing, so without
      -- this the handler reported success on a full or read-only disk and the
      -- QoS 1/2 messages it claimed to have persisted were simply gone.
      if the result is not empty then
         __mqttLog "ERROR saving persistent messages:" && the result
         exit __savePersistentMessages
      end if
      __mqttDebug "Saved" && the number of lines of the keys of gMQTTConnections[pConnID]["pendingAcks"] && "pending messages"
   catch tError
      __mqttLog "ERROR saving persistent messages:" && tError
   end try
end __savePersistentMessages

private command __loadPersistentMessages pConnID
   if not gMQTTPersistentStoreEnabled then
      exit __loadPersistentMessages
   end if

   local tFilePath, tBlob, tArr, tSafeConnID, tError
   put __sanitizeFilename(pConnID) into tSafeConnID

   put gMQTTPersistentStorePath & "/" & tSafeConnID & ".dat" into tFilePath

   if there is not a file tFilePath then
      exit __loadPersistentMessages
   end if

   try
      put URL ("binfile:" & tFilePath) into tBlob
      if the result is not empty then
         __mqttLog "ERROR reading persistent messages:" && the result
         exit __loadPersistentMessages
      end if

      put arrayDecode(tBlob) into tArr

      -- Refuse a store that did not decode to an array. arrayDecode returns
      -- empty for a truncated or corrupt blob, and putting that into
      -- pendingAcks would replace real state with nothing.
      if tArr is not an array then
         __mqttLog "Persistent store for" && pConnID && "is unreadable; leaving it in place"
         exit __loadPersistentMessages
      end if

      put tArr into gMQTTConnections[pConnID]["pendingAcks"]
      __mqttLog "Loaded" && the number of lines of the keys of tArr && "pending messages"

      -- The file is deleted only AFTER the messages are safely in memory, and
      -- rewritten immediately so a crash between here and the first ack cannot
      -- lose them - the old order deleted first and held the only copy in RAM.
      delete file tFilePath
      __savePersistentMessages pConnID
   catch tError
      __mqttLog "ERROR loading persistent messages:" && tError
   end try
end __loadPersistentMessages

private function __sanitizeFilename pFilename
   local tSafe, tChar, tCode
   
   repeat for each char tChar in pFilename
      put charToNum(tChar) into tCode
      
      -- Keep alphanumeric, hyphen, underscore, period
      if (tCode >= 48 and tCode <= 57) or \
         (tCode >= 65 and tCode <= 90) or \
         (tCode >= 97 and tCode <= 122) or \
         tCode = 45 or tCode = 95 or tCode = 46 then
         put tChar after tSafe
      else
         -- URL encode other characters
         put "%" & format("%02X", tCode) after tSafe
      end if
   end repeat
   
   return tSafe
end __sanitizeFilename

-- ========================================
-- CALLBACK INVOCATORS (PRIVATE)
-- ========================================

-- One dispatch path for all four callbacks. It exists because the three
-- invokers below each hard-coded `dispatch <handler> to gMQTTCallbackTarget`,
-- and an EMPTY target is not "no target" to the engine - it is an unresolvable
-- one. Forgetting mqttSetCallbackTarget therefore turned every log line, every
-- state change and every reconnect notice into a caught-and-swallowed error,
-- with the debug channel that would have said so going through the same broken
-- path. With no target set the message now goes through the ordinary message
-- path instead, which reaches the frontscripts, the current card and the
-- backscripts - the useful default, and the one most callers expect.
-- Returns empty on success or the error text.
private function __dispatchCallback pHandler, pArg1, pArg2, pArg3, pArg4, pArgCount
   local tError, tTarget
   put gMQTTCallbackTarget into tTarget

   -- A last-resort target. `dispatch X` with NO target goes to the object whose
   -- script is running - this library - and the message path from a library
   -- stack does not reach the application's card, so it would be dropped just
   -- as silently as the empty target it replaced.
   if tTarget is empty then
      put __defaultCallbackTarget() into tTarget
   end if

   try
      switch pArgCount
         case 1
            dispatch pHandler to tTarget with pArg1
            break
         case 2
            dispatch pHandler to tTarget with pArg1, pArg2
            break
         default
            dispatch pHandler to tTarget with pArg1, pArg2, pArg3, pArg4
      end switch
   catch tError
      return tError
   end try
   return empty
end __dispatchCallback

private command __invokeReconnectCallback pEvent, pHost, pPort, pAttempts
   if gMQTTReconnectCallback is empty then
      exit __invokeReconnectCallback
   end if

   __mqttDebug "Invoking reconnect callback:" && gMQTTReconnectCallback

   local tError
   put __dispatchCallback(gMQTTReconnectCallback, pEvent, pHost, pPort, pAttempts, 4) into tError
   if tError is not empty then
      __mqttLog "ERROR invoking reconnect callback:" && tError
      __mqttLog "Check that callback target is valid and handler exists"
   end if
end __invokeReconnectCallback

private command __invokeStateChangeCallback pState, pHost, pPort, pReason
   if gMQTTStateChangeCallback is empty then
      exit __invokeStateChangeCallback
   end if

   __mqttDebug "Invoking state change callback:" && gMQTTStateChangeCallback

   local tError
   put __dispatchCallback(gMQTTStateChangeCallback, pState, pHost, pPort, pReason, 4) into tError
   if tError is not empty then
      __mqttLog "ERROR invoking state change callback:" && tError
      __mqttLog "Check that callback target is valid and handler exists"
   end if
end __invokeStateChangeCallback

-- ========================================
-- PACKET BUILDERS (PRIVATE)
-- ========================================

private function __buildConnectPacket pConnID
   local tPacket, tVarHeader, tPayload
   
   -- Fixed header: CONNECT packet type
   put __numToByte(0x10) into tPacket
   
   -- Variable Header: Protocol Name and Level
   put binaryEncode("n", 4) into tVarHeader
   put tVarHeader & "MQTT" into tVarHeader
   put tVarHeader & __numToByte(4) into tVarHeader
   
   -- Connect Flags
   local tFlags
   put 0 into tFlags
   
   -- Clean session flag
   if gMQTTConnections[pConnID]["cleanSession"] is true then
      put tFlags bitOr 0x02 into tFlags
   end if
   
   -- Last Will and Testament flags
   if gMQTTConnections[pConnID]["lwt"]["topic"] is not empty then
      put tFlags bitOr 0x04 into tFlags
      
      local tWillQoS
      put gMQTTConnections[pConnID]["lwt"]["qos"] into tWillQoS
      put tFlags bitOr (tWillQoS * 8) into tFlags
      
      if gMQTTConnections[pConnID]["lwt"]["retain"] is true then
         put tFlags bitOr 0x20 into tFlags
      end if
   end if
   
   -- Username flag
   if gMQTTConnections[pConnID]["username"] is not empty then
      put tFlags bitOr 0x80 into tFlags

      -- Password flag. MQTT 3.1.1 section 3.1.2.9: the password flag may only
      -- be set when the user name flag is. A password with no user name is a
      -- malformed CONNECT the broker is entitled to close on, so it is nested
      -- here rather than set independently as it used to be.
      if gMQTTConnections[pConnID]["password"] is not empty then
         put tFlags bitOr 0x40 into tFlags
      end if
   else if gMQTTConnections[pConnID]["password"] is not empty then
      __mqttLog "WARNING: password ignored - MQTT 3.1.1 does not allow one without a user name"
   end if

   put tVarHeader & __numToByte(tFlags) into tVarHeader
   
   -- Keep Alive timer
   put tVarHeader & binaryEncode("n", gMQTTConnections[pConnID]["keepAlive"]) into tVarHeader
   
   -- Payload: Client Identifier
   put __encodeUTF8String(gMQTTConnections[pConnID]["clientID"]) into tPayload
   
   -- Payload: Last Will and Testament
   if gMQTTConnections[pConnID]["lwt"]["topic"] is not empty then
      local tWillTopicUTF8, tWillMessageUTF8
      put textEncode(gMQTTConnections[pConnID]["lwt"]["topic"], "UTF-8") into tWillTopicUTF8
      put textEncode(gMQTTConnections[pConnID]["lwt"]["message"], "UTF-8") into tWillMessageUTF8
      put tPayload & __encodeString(tWillTopicUTF8) into tPayload
      put tPayload & __encodeString(tWillMessageUTF8) into tPayload
   end if
   
   -- Payload: Username, then Password. The payload fields must appear in
   -- exactly the order the flags above declared them, so the password is nested
   -- under the same user-name test that set its flag.
   if gMQTTConnections[pConnID]["username"] is not empty then
      put tPayload & __encodeUTF8String(gMQTTConnections[pConnID]["username"]) into tPayload

      if gMQTTConnections[pConnID]["password"] is not empty then
         put tPayload & __encodeUTF8String(gMQTTConnections[pConnID]["password"]) into tPayload
      end if
   end if

   -- Finalize packet with remaining length
   local tRemainingLength
   put the number of bytes of tVarHeader + the number of bytes of tPayload into tRemainingLength

   local tRemLenBytes
   put __encodeRemainingLength(tRemainingLength) into tRemLenBytes
   if tRemLenBytes is empty then
      return empty
   end if

   put tPacket & tRemLenBytes & tVarHeader & tPayload into tPacket
   
   return tPacket
end __buildConnectPacket

private function __buildPublishPacket pConnID, pTopic, pPayload, pQoS, pRetain
   local tPacket, tFixedHeader, tVarHeader
   
   -- Fixed Header: PUBLISH packet type with flags
   local tFirstByte
   put 0x30 into tFirstByte
   
   -- QoS bits
   if pQoS = 1 then
      put tFirstByte bitOr 0x02 into tFirstByte
   else if pQoS = 2 then
      put tFirstByte bitOr 0x04 into tFirstByte
   end if
   
   -- Retain bit
   if pRetain is true then
      put tFirstByte bitOr 0x01 into tFirstByte
   end if
   
   put __numToByte(tFirstByte) into tFixedHeader
   
   -- Variable Header: Topic
   put __encodeString(pTopic) into tVarHeader
   if tVarHeader is empty then
      return empty
   end if

   -- Variable Header: Packet ID (only for QoS > 0)
   local tPacketID
   put 0 into tPacketID

   if pQoS > 0 then
      put __generatePacketID(pConnID) into tPacketID
      if tPacketID is empty then
         return empty
      end if
      put tVarHeader & binaryEncode("n", tPacketID) into tVarHeader

      -- Store for acknowledgment tracking. The topic is kept as the UTF-8 BYTES
      -- that went on the wire, because __resendPendingAcks re-frames it as-is.
      put pTopic into gMQTTConnections[pConnID]["pendingAcks"][tPacketID]["topic"]
      put pPayload into gMQTTConnections[pConnID]["pendingAcks"][tPacketID]["payload"]
      put pQoS into gMQTTConnections[pConnID]["pendingAcks"][tPacketID]["qos"]
      put pRetain into gMQTTConnections[pConnID]["pendingAcks"][tPacketID]["retain"]
      put false into gMQTTConnections[pConnID]["pendingAcks"][tPacketID]["pubrelPending"]
      put the milliseconds into gMQTTConnections[pConnID]["pendingAcks"][tPacketID]["timestamp"]
   end if

   -- Build complete packet
   local tRemainingLength
   put the number of bytes of tVarHeader + the number of bytes of pPayload into tRemainingLength

   local tRemLenBytes
   put __encodeRemainingLength(tRemainingLength) into tRemLenBytes
   if tRemLenBytes is empty then
      return empty
   end if

   put tFixedHeader & tRemLenBytes & tVarHeader & pPayload into tPacket
   
   return tPacket
end __buildPublishPacket

private function __buildSubscribePacket pConnID, pTopic, pQoS
   local tPacket, tVarHeader, tPayload
   
   -- Fixed Header: SUBSCRIBE packet type (0x82 = QoS 1 required)
   put __numToByte(0x82) into tPacket
   
   -- Variable Header: Packet ID
   local tPacketID
   put __generatePacketID(pConnID) into tPacketID
   if tPacketID is empty then
      return empty
   end if
   put binaryEncode("n", tPacketID) into tVarHeader
   
   -- Payload: Topic filter and QoS
   local tEncodedTopic
   put __encodeString(pTopic) into tEncodedTopic
   if tEncodedTopic is empty then
      return empty
   end if
   put tEncodedTopic & __numToByte(pQoS) into tPayload

   -- Store for acknowledgment tracking, keyed by the topic TEXT. pTopic arrives
   -- here as UTF-8 bytes because that is what the packet needs; the
   -- subscriptions table is keyed by text, and __parseSubAck has to match it.
   put textDecode(pTopic, "UTF-8") into gMQTTConnections[pConnID]["pendingSubAcks"][tPacketID]["topic"]
   put pQoS into gMQTTConnections[pConnID]["pendingSubAcks"][tPacketID]["qos"]

   -- Build complete packet
   local tRemainingLength
   put the number of bytes of tVarHeader + the number of bytes of tPayload into tRemainingLength

   local tRemLenBytes
   put __encodeRemainingLength(tRemainingLength) into tRemLenBytes
   if tRemLenBytes is empty then
      return empty
   end if

   put tPacket & tRemLenBytes & tVarHeader & tPayload into tPacket
   
   return tPacket
end __buildSubscribePacket

private function __buildUnsubscribePacket pConnID, pTopic
   local tPacket, tVarHeader, tPayload
   
   -- Fixed Header: UNSUBSCRIBE packet type (0xA2 = QoS 1 required)
   put __numToByte(0xA2) into tPacket
   
   -- Variable Header: Packet ID
   local tPacketID
   put __generatePacketID(pConnID) into tPacketID
   if tPacketID is empty then
      return empty
   end if
   put binaryEncode("n", tPacketID) into tVarHeader
   
   -- Payload: Topic filter
   put __encodeString(pTopic) into tPayload
   if tPayload is empty then
      return empty
   end if

   -- Stored as TEXT, to match the subscriptions table (see __buildSubscribePacket)
   put textDecode(pTopic, "UTF-8") into gMQTTConnections[pConnID]["pendingUnsubAcks"][tPacketID]["topic"]

   -- Build complete packet
   local tRemainingLength
   put the number of bytes of tVarHeader + the number of bytes of tPayload into tRemainingLength

   local tRemLenBytes
   put __encodeRemainingLength(tRemainingLength) into tRemLenBytes
   if tRemLenBytes is empty then
      return empty
   end if

   put tPacket & tRemLenBytes & tVarHeader & tPayload into tPacket
   
   return tPacket
end __buildUnsubscribePacket

private function __buildPingReqPacket
   return __numToByte(0xC0) & __numToByte(0)
end __buildPingReqPacket

private function __buildPubRecPacket pPacketID
   local tPacket
   put __numToByte(0x50) into tPacket
   put tPacket & __numToByte(2) into tPacket
   put tPacket & binaryEncode("n", pPacketID) into tPacket
   return tPacket
end __buildPubRecPacket

private function __buildPubRelPacket pPacketID
   local tPacket
   put __numToByte(0x62) into tPacket
   put tPacket & __numToByte(2) into tPacket
   put tPacket & binaryEncode("n", pPacketID) into tPacket
   return tPacket
end __buildPubRelPacket

private function __buildPubCompPacket pPacketID
   local tPacket
   put __numToByte(0x70) into tPacket
   put tPacket & __numToByte(2) into tPacket
   put tPacket & binaryEncode("n", pPacketID) into tPacket
   return tPacket
end __buildPubCompPacket

-- ========================================
-- PACKET PARSERS (PRIVATE)
-- ========================================

private command __resendPendingAcks pConnID
   if gMQTTConnections[pConnID]["pendingAcks"] is empty then
      exit __resendPendingAcks
   end if

   __mqttLog "Resending" && the number of lines of the keys of gMQTTConnections[pConnID]["pendingAcks"] && "pending messages"

   local tPacket, tFixedHeader, tVarHeader, tPayload, tPacketID
   local tTopic, tQoS, tRetain, tFirstByte, tRemainingLength, tError

   repeat for each key tPacketID in gMQTTConnections[pConnID]["pendingAcks"]
      -- A QoS 2 message the broker has already PUBRECed is past the PUBLISH
      -- leg: what it is waiting for is PUBREL. Re-sending the PUBLISH restarts
      -- a handshake it has already half-completed, and a broker that treats the
      -- restarted exchange as a new message delivers the payload twice - which
      -- is the one thing QoS 2 exists to prevent.
      if gMQTTConnections[pConnID]["pendingAcks"][tPacketID]["pubrelPending"] is true then
         __mqttDebug "Resending PUBREL for packet" && tPacketID
         __sendPubRel pConnID, tPacketID
         next repeat
      end if

      put gMQTTConnections[pConnID]["pendingAcks"][tPacketID]["topic"] into tTopic
      put gMQTTConnections[pConnID]["pendingAcks"][tPacketID]["payload"] into tPayload
      put gMQTTConnections[pConnID]["pendingAcks"][tPacketID]["qos"] into tQoS
      put gMQTTConnections[pConnID]["pendingAcks"][tPacketID]["retain"] into tRetain

      -- Build with DUP flag set (indicates retransmission)
      put 0x30 into tFirstByte
      put tFirstByte bitOr 0x08 into tFirstByte  -- DUP flag

      if tQoS = 1 then
         put tFirstByte bitOr 0x02 into tFirstByte
      else if tQoS = 2 then
         put tFirstByte bitOr 0x04 into tFirstByte
      end if

      if tRetain is true then
         put tFirstByte bitOr 0x01 into tFirstByte
      end if

      put __numToByte(tFirstByte) into tFixedHeader

      -- Build Variable Header + Payload
      put __encodeString(tTopic) into tVarHeader
      if tVarHeader is empty then
         __mqttLog "Skipping resend of packet" && tPacketID & ": topic no longer encodable"
         next repeat
      end if
      put tVarHeader & binaryEncode("n", tPacketID) into tVarHeader

      -- Build complete packet
      put the number of bytes of tVarHeader + the number of bytes of tPayload into tRemainingLength

      local tRemLenBytes
      put __encodeRemainingLength(tRemainingLength) into tRemLenBytes
      if tRemLenBytes is empty then
         __mqttLog "Skipping resend of packet" && tPacketID & ": length not framable"
         next repeat
      end if

      put tFixedHeader & tRemLenBytes & tVarHeader & tPayload into tPacket

      if __isSocketOpen(pConnID) then
         put __writeSocket(pConnID, tPacket) into tError
         if tError is empty then
            __mqttDebug "Resent (DUP) packet" && tPacketID
         else
            -- The connection is reset by this call; every later write in this
            -- loop would go to a closed socket, so stop here.
            get __failWrite(pConnID, "PUBLISH (DUP " & tPacketID & ")", tError)
            exit __resendPendingAcks
         end if
      end if
   end repeat
end __resendPendingAcks

private command __parseIncomingData pConnID, pData
   -- EVERYTHING in this handler counts in BYTES. `char` and `the length of`
   -- count CHARACTERS, and the two agree only while every byte is ASCII - so
   -- the old code framed packets correctly right up until a payload carried a
   -- multi-byte sequence, then silently mis-split the stream. MQTT is a binary
   -- protocol; `byte`, `byteToNum` and `the number of bytes of` are the only
   -- correct tools for it.
   put pData after gMQTTConnections[pConnID]["receiveBuffer"]

   local tBuffer
   put gMQTTConnections[pConnID]["receiveBuffer"] into tBuffer

   -- Buffer overflow protection
   if the number of bytes of tBuffer > __maxBufferSize() then
      __mqttLog "CRITICAL: Buffer overflow (" & the number of bytes of tBuffer && "bytes)"
      __cleanupConnection pConnID, "buffer overflow"
      __handleReconnect pConnID
      exit __parseIncomingData
   end if

   -- Process complete packets in buffer
   local tFirstByte, tPacketType, tRemLenResult, tRemLen, tRemLenBytes
   local tCommaPos, tPacketSize, tPacketData
   repeat while the number of bytes of tBuffer >= 2
      -- Parse fixed header
      put byteToNum(byte 1 of tBuffer) into tFirstByte
      put tFirstByte bitAnd 0xF0 into tPacketType

      -- Decode remaining length
      put __decodeRemainingLength(tBuffer, 2) into tRemLenResult

      if tRemLenResult is empty then
         -- Incomplete remaining length, wait for more data
         exit repeat
      end if

      -- Split "value,bytes" without relying on the global itemDelimiter
      put offset(comma, tRemLenResult) into tCommaPos
      put char 1 to (tCommaPos - 1) of tRemLenResult into tRemLen
      put char (tCommaPos + 1) to -1 of tRemLenResult into tRemLenBytes

      -- Calculate total packet size
      put 1 + tRemLenBytes + tRemLen into tPacketSize

      -- Refuse an oversized packet AS SOON AS ITS LENGTH IS KNOWN rather than
      -- after buffering it. The overflow guard above only fires once the bytes
      -- have actually arrived, so a broker (or anything posing as one)
      -- announcing a 256 MB packet could hold the buffer growing until then.
      if tPacketSize > __maxBufferSize() then
         __mqttLog "CRITICAL: Declared packet size" && tPacketSize && \
               "exceeds buffer limit" && __maxBufferSize()
         __cleanupConnection pConnID, "oversized packet"
         __handleReconnect pConnID
         exit __parseIncomingData
      end if

      if the number of bytes of tBuffer < tPacketSize then
         -- Incomplete packet, wait for more data
         exit repeat
      end if

      -- Extract packet data (excluding fixed header and remaining length)
      put byte (2 + tRemLenBytes) to tPacketSize of tBuffer into tPacketData

      -- Consume the packet from the local buffer BEFORE dispatching it. A
      -- handler below can run application code that re-enters this library, and
      -- a packet still sitting at the head of the buffer would be parsed twice.
      delete byte 1 to tPacketSize of tBuffer
      put tBuffer into gMQTTConnections[pConnID]["receiveBuffer"]

      -- Update last activity timestamp
      put the milliseconds into gMQTTConnections[pConnID]["lastActivity"]

      __mqttDebug "Parsing packet type:" && tPacketType

      -- Dispatch to appropriate packet handler. tPacketType is the high nibble,
      -- so PUBLISH is always 48 here whatever its QoS and DUP flags say; the
      -- old `case 50` / `case 52` for "PUBLISH QoS 1 / QoS 2" could never match.
      switch tPacketType
         case 32  -- CONNACK
            __parseConnAck pConnID, tPacketData
            break
         case 48  -- PUBLISH (QoS carried in the low nibble of tFirstByte)
            __parsePublishReceived pConnID, tFirstByte, tPacketData
            break
         case 64  -- PUBACK
            __parsePubAck pConnID, tPacketData
            break
         case 80  -- PUBREC
            __parsePubRec pConnID, tPacketData
            break
         case 96  -- PUBREL
            __parsePubRel pConnID, tPacketData
            break
         case 112  -- PUBCOMP
            __parsePubComp pConnID, tPacketData
            break
         case 144  -- SUBACK
            __parseSubAck pConnID, tPacketData
            break
         case 176  -- UNSUBACK
            __parseUnsubAck pConnID, tPacketData
            break
         case 208  -- PINGRESP
            __parsePingResp pConnID
            break
         default
            __mqttLog "Unknown packet type:" && tPacketType
      end switch

      -- A dispatched handler may have torn the connection down (a message
      -- callback calling mqttDisconnect, a CONNACK refusal). Storing the buffer
      -- back unconditionally, as the old code did after the loop, RECREATED the
      -- connection as a record holding nothing but a receiveBuffer - a zombie
      -- that mqttGetConnections listed and the keep-alive sweep walked forever.
      if gMQTTConnections[pConnID] is empty then
         exit __parseIncomingData
      end if

      -- Re-read: a re-entrant call may have appended to or cleared the buffer.
      put gMQTTConnections[pConnID]["receiveBuffer"] into tBuffer
   end repeat

   -- Store updated buffer
   put tBuffer into gMQTTConnections[pConnID]["receiveBuffer"]
end __parseIncomingData

private command __parseConnAck pConnID, pData
   if the number of bytes of pData < 2 then
      __mqttLog "Malformed CONNACK (" & the number of bytes of pData && "bytes), dropping connection"
      __cleanupConnection pConnID, "malformed CONNACK"
      exit __parseConnAck
   end if

   local tSessionPresent, tReturnCode
   put byteToNum(byte 1 of pData) into tSessionPresent
   put byteToNum(byte 2 of pData) into tReturnCode

   if tReturnCode = 0 then
      -- Connection successful
      put true into gMQTTConnections[pConnID]["connected"]
      put 0 into gMQTTConnections[pConnID]["reconnectAttempts"]
      put false into gMQTTConnections[pConnID]["closing"]
      put 0 into gMQTTConnections[pConnID]["lastPingTime"]
      put the milliseconds into gMQTTConnections[pConnID]["lastActivity"]
      __mqttLog "Connected successfully"

      -- Cancel reconnect attempts
      local tCancelErr
      if gMQTTConnections[pConnID]["reconnectMessageID"] is not empty then
         try
            cancel gMQTTConnections[pConnID]["reconnectMessageID"]
         catch tCancelErr
            __mqttDebug "Could not cancel reconnect message (already executed or invalid)"
         end try
         put empty into gMQTTConnections[pConnID]["reconnectMessageID"]
      end if

      -- Start the keep-alive BEFORE the callbacks below. Those callbacks run
      -- application code that is entitled to publish, subscribe or disconnect,
      -- and the connection should be fully armed before it is announced.
      __scheduleKeepAlive pConnID

      -- Check for session present. Session state is read from the CONNACK, so
      -- record it before anything reacts to being connected.
      local tResumed
      put ((tSessionPresent bitAnd 0x01) = 1) into tResumed
      put tResumed into gMQTTConnections[pConnID]["sessionPresent"]

      -- Invoke callbacks
      __invokeReconnectCallback "success", gMQTTConnections[pConnID]["host"], gMQTTConnections[pConnID]["port"], 0
      __invokeStateChangeCallback "connected", gMQTTConnections[pConnID]["host"], gMQTTConnections[pConnID]["port"], ""

      -- A callback may have disconnected us again; do not restore session state
      -- onto a connection that is on its way out.
      if gMQTTConnections[pConnID] is empty then
         exit __parseConnAck
      end if

      if gMQTTConnections[pConnID]["connected"] is not true then
         exit __parseConnAck
      end if

      if tResumed then
         -- The broker still holds our subscriptions and QoS state; re-sending
         -- either would duplicate them.
         __mqttLog "Session present"
      else
         __mqttLog "New session"
         __resubscribeAll pConnID
         __resendPendingAcks pConnID
      end if
   else
      -- Connection refused
      local tErrorMsg
      switch tReturnCode
         case 1
            put "Unacceptable protocol version" into tErrorMsg
            break
         case 2
            put "Identifier rejected" into tErrorMsg
            break
         case 3
            put "Server unavailable" into tErrorMsg
            break
         case 4
            put "Bad username or password" into tErrorMsg
            break
         case 5
            put "Not authorized" into tErrorMsg
            break
         default
            put "Unknown error" && tReturnCode into tErrorMsg
      end switch

      __mqttLog "Connection refused:" && tErrorMsg
      __cleanupConnection pConnID, tErrorMsg

      -- Retry only what retrying can fix. Codes 1, 4 and 5 are the broker
      -- saying the credentials or the protocol are wrong; reconnecting on a
      -- timer re-presents the same rejected CONNECT forever, which for a bad
      -- password is an automated lockout against the user's own account.
      if tReturnCode = 2 or tReturnCode = 3 then
         __handleReconnect pConnID
      else
         __mqttLog "Not retrying: the broker rejected the connection itself"
         put false into gMQTTConnections[pConnID]["autoReconnect"]
      end if
   end if
end __parseConnAck

private command __parsePublishReceived pConnID, pFirstByte, pData
   local tLen
   put the number of bytes of pData into tLen

   -- A PUBLISH must carry at least the 2-byte topic length
   if tLen < 2 then
      __mqttLog "Malformed PUBLISH (too short), ignoring"
      exit __parsePublishReceived
   end if

   -- Extract QoS from flags
   local tQoS
   put (pFirstByte bitAnd 0x06) div 2 into tQoS

   -- QoS 3 is reserved and is a protocol violation; the low nibble carrying it
   -- means the framing cannot be trusted either.
   if tQoS > 2 then
      __mqttLog "Malformed PUBLISH (reserved QoS 3), dropping connection"
      __cleanupConnection pConnID, "protocol violation: QoS 3"
      __handleReconnect pConnID
      exit __parsePublishReceived
   end if

   -- Extract retain flag
   local tRetain
   put ((pFirstByte bitAnd 0x01) = 1) into tRetain

   -- Parse topic
   local tTopicLen, tPos, tBytesRead
   put binaryDecode("n", byte 1 to 2 of pData, tTopicLen) into tBytesRead
   put 3 into tPos

   -- Validate the declared topic length against what actually arrived. Without
   -- this a truncated or hostile packet silently produced a clamped topic and a
   -- payload read from the wrong offset - garbage delivered as a real message.
   if (tPos + tTopicLen - 1) > tLen then
      __mqttLog "Malformed PUBLISH (topic length" && tTopicLen && "exceeds packet), ignoring"
      exit __parsePublishReceived
   end if

   local tTopicUTF8
   put byte tPos to (tPos + tTopicLen - 1) of pData into tTopicUTF8
   add tTopicLen to tPos

   -- Decode topic from UTF-8
   local tTopic
   put textDecode(tTopicUTF8, "UTF-8") into tTopic

   -- Parse packet ID (only for QoS > 0)
   local tPacketID
   put 0 into tPacketID
   if tQoS > 0 then
      if (tPos + 1) > tLen then
         __mqttLog "Malformed PUBLISH (no room for packet ID), ignoring"
         exit __parsePublishReceived
      end if
      put binaryDecode("n", byte tPos to (tPos + 1) of pData, tPacketID) into tBytesRead
      add 2 to tPos

      -- Packet ID 0 is invalid for QoS > 0 and there is no valid acknowledgment
      -- to send for it.
      if tPacketID = 0 then
         __mqttLog "Malformed PUBLISH (packet ID 0 at QoS" && tQoS & "), ignoring"
         exit __parsePublishReceived
      end if
   end if

   -- Extract message payload (binary safe). A zero-length payload is legal and
   -- must come out as empty, not as a backwards byte range.
   local tMessage
   if tPos > tLen then
      put empty into tMessage
   else
      put byte tPos to tLen of pData into tMessage
   end if

   __mqttLog "Received:" && tTopic && "(" & the number of bytes of tMessage && "bytes, QoS" && tQoS & ")"

   -- Handle QoS acknowledgments. The ack goes out BEFORE the message is handed
   -- to application code: a callback that takes its time, or throws, must not
   -- cost us the acknowledgment the broker is waiting on.
   if tQoS = 1 then
      __sendPubAck pConnID, tPacketID
   else if tQoS = 2 then
      -- QoS 2 is delivered on PUBREL, exactly once. A re-sent PUBLISH (DUP)
      -- for a packet ID already held here is the duplicate the handshake
      -- exists to absorb: re-acknowledge it, but do not stage it twice.
      if gMQTTConnections[pConnID]["pendingQoS2"][tPacketID] is empty then
         put tTopic into gMQTTConnections[pConnID]["pendingQoS2"][tPacketID]["topic"]
         put tMessage into gMQTTConnections[pConnID]["pendingQoS2"][tPacketID]["message"]
      else
         __mqttDebug "Duplicate QoS 2 PUBLISH for packet" && tPacketID & ", re-acknowledging only"
      end if
      __sendPubRec pConnID, tPacketID
   end if

   -- Update statistics (QoS 2 is counted on PUBREL delivery)
   if tQoS < 2 then
      add 1 to gMQTTConnections[pConnID]["stats"]["messagesReceived"]
   end if

   -- Deliver message (QoS 2 delivered after PUBREL)
   if tQoS < 2 then
      __invokeMessageCallback pConnID, tTopic, tMessage
   end if
end __parsePublishReceived

-- Every ack parser below reads its packet ID from the SAME two bytes, so the
-- read is factored out - and bounded to `byte 1 to 2` rather than handed the
-- whole packet, which is what keeps a padded or malformed ack from decoding
-- something other than the field it names. Returns empty for an unusable ack.
private function __ackPacketID pData
   if the number of bytes of pData < 2 then
      return empty
   end if

   local tPacketID, tBytesRead
   put binaryDecode("n", byte 1 to 2 of pData, tPacketID) into tBytesRead

   if tBytesRead < 1 or tPacketID is empty then
      return empty
   end if

   -- Packet ID 0 never identifies a real in-flight message
   if tPacketID = 0 then
      return empty
   end if

   return tPacketID
end __ackPacketID

private command __parsePubAck pConnID, pData
   local tPacketID
   put __ackPacketID(pData) into tPacketID
   if tPacketID is empty then
      __mqttDebug "Malformed PUBACK, ignoring"
      exit __parsePubAck
   end if

   __mqttLog "PUBACK received for packet" && tPacketID

   if gMQTTConnections[pConnID]["pendingAcks"][tPacketID] is not empty then
      delete variable gMQTTConnections[pConnID]["pendingAcks"][tPacketID]

      if gMQTTPersistentStoreEnabled then
         __savePersistentMessages pConnID
      end if
   end if
end __parsePubAck

private command __parsePubRec pConnID, pData
   local tPacketID
   put __ackPacketID(pData) into tPacketID
   if tPacketID is empty then
      __mqttDebug "Malformed PUBREC, ignoring"
      exit __parsePubRec
   end if

   __mqttLog "PUBREC received for packet" && tPacketID

   -- The broker owns the message from here; ownership of the QoS 2 exchange has
   -- moved to the PUBREL leg. Mark it so, and drop the payload we no longer
   -- need to hold: a reconnect must now retransmit PUBREL, NOT the original
   -- PUBLISH. Resending the PUBLISH (what __resendPendingAcks used to do for
   -- every pending message alike) restarts a handshake the broker has already
   -- half-completed, which is how "exactly once" becomes "twice".
   if gMQTTConnections[pConnID]["pendingAcks"][tPacketID] is not empty then
      put true into gMQTTConnections[pConnID]["pendingAcks"][tPacketID]["pubrelPending"]
      put empty into gMQTTConnections[pConnID]["pendingAcks"][tPacketID]["payload"]

      if gMQTTPersistentStoreEnabled then
         __savePersistentMessages pConnID
      end if
   end if

   __sendPubRel pConnID, tPacketID
end __parsePubRec

private command __parsePubRel pConnID, pData
   local tPacketID
   put __ackPacketID(pData) into tPacketID
   if tPacketID is empty then
      __mqttDebug "Malformed PUBREL, ignoring"
      exit __parsePubRel
   end if

   __mqttLog "PUBREL received for packet" && tPacketID

   if gMQTTConnections[pConnID]["pendingQoS2"][tPacketID] is not empty then
      local tTopic, tMessage
      put gMQTTConnections[pConnID]["pendingQoS2"][tPacketID]["topic"] into tTopic
      put gMQTTConnections[pConnID]["pendingQoS2"][tPacketID]["message"] into tMessage

      -- Retire the staged message and send PUBCOMP BEFORE handing it to the
      -- application. A callback that throws, or that re-enters this library,
      -- must not be able to leave the entry in place for a repeat delivery -
      -- "exactly once" is this handler's promise, not the callback's.
      delete variable gMQTTConnections[pConnID]["pendingQoS2"][tPacketID]

      -- Update statistics for QoS 2 message delivery
      add 1 to gMQTTConnections[pConnID]["stats"]["messagesReceived"]

      __sendPubComp pConnID, tPacketID

      -- Deliver the message to application
      __invokeMessageCallback pConnID, tTopic, tMessage
      exit __parsePubRel
   end if

   -- No staged message: a duplicate PUBREL for one already delivered. The
   -- broker is waiting on PUBCOMP either way.
   __sendPubComp pConnID, tPacketID
end __parsePubRel

private command __parsePubComp pConnID, pData
   local tPacketID
   put __ackPacketID(pData) into tPacketID
   if tPacketID is empty then
      __mqttDebug "Malformed PUBCOMP, ignoring"
      exit __parsePubComp
   end if

   __mqttLog "PUBCOMP received for packet" && tPacketID

   if gMQTTConnections[pConnID]["pendingAcks"][tPacketID] is not empty then
      delete variable gMQTTConnections[pConnID]["pendingAcks"][tPacketID]

      if gMQTTPersistentStoreEnabled then
         __savePersistentMessages pConnID
      end if
   end if
end __parsePubComp

private command __parseSubAck pConnID, pData
   if the number of bytes of pData < 3 then
      __mqttDebug "Malformed SUBACK, ignoring"
      exit __parseSubAck
   end if

   local tPacketID
   put __ackPacketID(pData) into tPacketID
   if tPacketID is empty then
      __mqttDebug "Malformed SUBACK, ignoring"
      exit __parseSubAck
   end if

   local tTopic
   put gMQTTConnections[pConnID]["pendingSubAcks"][tPacketID]["topic"] into tTopic

   if tTopic is empty then
      __mqttDebug "SUBACK for unknown packet" && tPacketID & ", ignoring"
      exit __parseSubAck
   end if

   local tReturnCode
   put byteToNum(byte 3 of pData) into tReturnCode

   if tReturnCode = 0x80 then
      -- The subscriptions table is keyed by the ORIGINAL topic text, which is
      -- what pendingSubAcks now stores too. It used to store the UTF-8 BYTES
      -- the packet was built from, so this delete missed for any non-ASCII
      -- topic - and a subscription the broker had refused was re-sent on every
      -- reconnect, forever.
      __mqttLog "Subscription failed:" && tTopic
      delete variable gMQTTConnections[pConnID]["subscriptions"][tTopic]
   else
      -- The broker may grant a LOWER QoS than requested; record what it
      -- actually gave us so a resubscribe asks for the same thing.
      __mqttLog "Subscription confirmed:" && tTopic && "(QoS" && tReturnCode & ")"
      put tReturnCode into gMQTTConnections[pConnID]["subscriptions"][tTopic]
   end if

   delete variable gMQTTConnections[pConnID]["pendingSubAcks"][tPacketID]
end __parseSubAck

private command __parseUnsubAck pConnID, pData
   local tPacketID
   put __ackPacketID(pData) into tPacketID
   if tPacketID is empty then
      __mqttDebug "Malformed UNSUBACK, ignoring"
      exit __parseUnsubAck
   end if

   local tTopic
   put gMQTTConnections[pConnID]["pendingUnsubAcks"][tPacketID]["topic"] into tTopic

   __mqttLog "Unsubscribe confirmed:" && tTopic

   delete variable gMQTTConnections[pConnID]["pendingUnsubAcks"][tPacketID]
end __parseUnsubAck

private command __parsePingResp pConnID
   put 0 into gMQTTConnections[pConnID]["lastPingTime"]
   __mqttDebug "PINGRESP received"
end __parsePingResp

-- ========================================
-- QoS 2 ACKNOWLEDGMENT SENDERS (PRIVATE)
-- ========================================

private command __sendPubRec pConnID, pPacketID
   if pPacketID is empty or pPacketID is not a number then
      __mqttDebug "Cannot send PUBREC: invalid packet ID" && pPacketID
      exit __sendPubRec
   end if
   
   local tPacket, tError
   put __buildPubRecPacket(pPacketID) into tPacket
   
   if __isSocketOpen(pConnID) then
      put __writeSocket(pConnID, tPacket) into tError
      if tError is empty then
         __mqttDebug "PUBREC sent for packet" && pPacketID
      else
         get __failWrite(pConnID, "PUBREC", tError)
      end if
   end if
end __sendPubRec

private command __sendPubRel pConnID, pPacketID
   if pPacketID is empty or pPacketID is not a number then
      __mqttDebug "Cannot send PUBREL: invalid packet ID" && pPacketID
      exit __sendPubRel
   end if
   
   local tPacket, tError
   put __buildPubRelPacket(pPacketID) into tPacket
   
   if __isSocketOpen(pConnID) then
      put __writeSocket(pConnID, tPacket) into tError
      if tError is empty then
         __mqttDebug "PUBREL sent for packet" && pPacketID
      else
         get __failWrite(pConnID, "PUBREL", tError)
      end if
   end if
end __sendPubRel

private command __sendPubComp pConnID, pPacketID
   if pPacketID is empty or pPacketID is not a number then
      __mqttDebug "Cannot send PUBCOMP: invalid packet ID" && pPacketID
      exit __sendPubComp
   end if
   
   local tPacket, tError
   put __buildPubCompPacket(pPacketID) into tPacket
   
   if __isSocketOpen(pConnID) then
      put __writeSocket(pConnID, tPacket) into tError
      if tError is empty then
         __mqttDebug "PUBCOMP sent for packet" && pPacketID
      else
         get __failWrite(pConnID, "PUBCOMP", tError)
      end if
   end if
end __sendPubComp

private command __sendPubAck pConnID, pPacketID
   if pPacketID is empty or pPacketID is not a number then
      __mqttDebug "Cannot send PUBACK: invalid packet ID" && pPacketID
      exit __sendPubAck
   end if
   
   local tPacket, tError
   put __numToByte(0x40) & __numToByte(2) & binaryEncode("n", pPacketID) into tPacket
   
   if __isSocketOpen(pConnID) then
      put __writeSocket(pConnID, tPacket) into tError
      if tError is empty then
         __mqttDebug "PUBACK sent for packet" && pPacketID
      else
         get __failWrite(pConnID, "PUBACK", tError)
      end if
   end if
end __sendPubAck

-- ========================================
-- KEEP-ALIVE (PRIVATE)
-- ========================================

private command __scheduleKeepAlive pConnID
   -- The sweep interval is the SHORTER of the ping threshold and the ping
   -- deadline, so both are checked in time. Firing once per keep-alive period
   -- (the old behaviour) means a connection whose threshold is 75% can be up to
   -- a full period late in noticing it should have pinged, and a dead broker
   -- goes unnoticed for two periods rather than 1.5.
   local tKeepAlive, tIntervalMS, tMsgID, tToken
   put gMQTTConnections[pConnID]["keepAlive"] into tKeepAlive

   -- Keep-alive 0 means "no keep-alive" in MQTT 3.1.1: no PINGREQ is required
   -- and the broker applies no timeout, so there is nothing to schedule. The
   -- old code scheduled `send ... in 0 milliseconds` and spun a timer that
   -- re-armed itself on every event loop pass.
   if tKeepAlive is not a number or tKeepAlive <= 0 then
      __mqttDebug "Keep-alive disabled for" && pConnID
      exit __scheduleKeepAlive
   end if

   -- The sweep runs at most halfway through the keep-alive period, so both the
   -- ping threshold and the ping deadline are checked in time.
   put trunc(tKeepAlive * 1000 * min(__keepAliveThreshold(), 0.5)) into tIntervalMS

   if tIntervalMS < 250 then
      put 250 into tIntervalMS
   end if

   -- Cancel any previously scheduled keep-alive timer for this connection
   -- to prevent timer accumulation when multiple connections are active
   local tErr
   if gMQTTConnections[pConnID]["keepAliveMessageID"] is not empty then
      try
         cancel gMQTTConnections[pConnID]["keepAliveMessageID"]
      catch tErr
         -- Already executed or invalid, ignore
      end try
      put empty into gMQTTConnections[pConnID]["keepAliveMessageID"]
   end if

   put __newTimerToken(pConnID) into tToken
   put __scheduleMessage("__executeKeepAliveTimer" && tToken, tIntervalMS) into tMsgID

   -- Store the message ID for cancellation
   if tMsgID is not empty then
      put tMsgID into gMQTTConnections[pConnID]["keepAliveMessageID"]
      __mqttDebug "Stored keep-alive message ID:" && tMsgID && "for" && pConnID
   end if
end __scheduleKeepAlive

-- NOTE: This MUST be a message handler (on), not a command, to receive send
-- messages. It also CANNOT be private as it needs to be in the message path.
on __executeKeepAliveTimer pToken
   local tConnID
   put __claimTimerToken(pToken) into tConnID

   __mqttDebug ">>> KEEP-ALIVE TIMER FIRED for token" && pToken

   if tConnID is empty then
      __mqttDebug "Keep-alive token" && pToken && "is stale, ignoring"
      exit __executeKeepAliveTimer
   end if

   put empty into gMQTTConnections[tConnID]["keepAliveMessageID"]

   if gMQTTConnections[tConnID]["connected"] is not true then
      __mqttDebug "Not connected, stopping keep-alive for" && tConnID
      exit __executeKeepAliveTimer
   end if

   local tKeepAlive, tLastActivity, tNow, tThresholdMS
   put gMQTTConnections[tConnID]["keepAlive"] into tKeepAlive
   put gMQTTConnections[tConnID]["lastActivity"] into tLastActivity
   put the milliseconds into tNow

   -- Calculate threshold using configurable value
   put (tKeepAlive * 1000 * __keepAliveThreshold()) into tThresholdMS

   -- Check for a PINGRESP that never came, BEFORE deciding whether to ping
   -- again. Nesting this inside the "time to ping" branch (the old shape) meant
   -- a broker that stopped answering was only ever noticed on a tick that also
   -- wanted to send - so a stalled link could sit undetected indefinitely.
   local tLastPingTime
   put gMQTTConnections[tConnID]["lastPingTime"] into tLastPingTime

   if tLastPingTime > 0 and (tNow - tLastPingTime) > (tKeepAlive * 1500) then
      __mqttLog "ERROR: PINGRESP timeout for" && tConnID
      __cleanupConnection tConnID, "ping timeout"
      __handleReconnect tConnID
      exit __executeKeepAliveTimer
   end if

   -- Send PINGREQ if the link has been quiet past the threshold and we are not
   -- already waiting on a PINGRESP.
   if (tNow - tLastActivity) > tThresholdMS and tLastPingTime = 0 then
      local tPingPacket, tError
      put __buildPingReqPacket() into tPingPacket

      if __isSocketOpen(tConnID) then
         put __writeSocket(tConnID, tPingPacket) into tError
         if tError is empty then
            put the milliseconds into gMQTTConnections[tConnID]["lastActivity"]
            put the milliseconds into gMQTTConnections[tConnID]["lastPingTime"]
            __mqttDebug "PINGREQ sent (threshold:" && (__keepAliveThreshold() * 100) & "%)"
         else
            -- A failed write on a socket the engine still calls open means the
            -- link is gone; treat it as the disconnection it is instead of
            -- waiting for a keep-alive that can never be answered.
            __mqttLog "ERROR sending ping:" && tError
            __cleanupConnection tConnID, "ping write failed:" && tError
            __handleReconnect tConnID
            exit __executeKeepAliveTimer
         end if
      else
         __mqttLog "Keep-alive found socket closed for" && tConnID
         __cleanupConnection tConnID, "socket not open"
         __handleReconnect tConnID
         exit __executeKeepAliveTimer
      end if
   end if

   -- Schedule next keep-alive check
   __scheduleKeepAlive tConnID
end __executeKeepAliveTimer

-- ========================================
-- MESSAGE CALLBACK (PRIVATE)
-- ========================================

private command __invokeMessageCallback pConnID, pTopic, pMessage
   if gMQTTMessageCallback is empty then
      __mqttDebug "No message callback set"
      exit __invokeMessageCallback
   end if

   __mqttDebug "Invoking message callback:" && gMQTTMessageCallback && "for topic:" && pTopic && "(" & the number of bytes of pMessage && "bytes)"

   -- Dispatch the user-defined handler with the topic and payload. Parameters
   -- are passed by value and are binary-safe. With no explicit callback target
   -- this goes through the ordinary message path rather than being dropped:
   -- the old code returned early whenever gMQTTCallbackTarget was empty, so a
   -- caller who set a message callback but no target received nothing at all
   -- and the only trace was a debug line most callers never turn on.
   local tError
   put __dispatchCallback(gMQTTMessageCallback, pTopic, pMessage, empty, empty, 2) into tError
   if tError is not empty then
      __mqttLog "ERROR invoking message callback:" && tError
      __mqttLog "Callback target:" && gMQTTCallbackTarget
      __mqttLog "Callback handler:" && gMQTTMessageCallback
   else
      __mqttDebug "Message callback completed successfully"
   end if
end __invokeMessageCallback

-- ========================================
-- LOGGING (PRIVATE)
-- ========================================

private command __mqttLog pMessage
   if gMQTTQuietMode is true then
      exit __mqttLog
   end if

   if gMQTTLogCallback is empty then
      exit __mqttLog
   end if

   -- Failures are swallowed on purpose and WITHOUT recursing: this is the
   -- handler every other error path reports through, so it must never be able
   -- to raise one of its own.
   get __dispatchCallback(gMQTTLogCallback, pMessage, empty, empty, empty, 1)
end __mqttLog

private command __mqttDebug pMessage
   if gMQTTDebugMode is not true then
      exit __mqttDebug
   end if

   __mqttLog "[DEBUG]" && pMessage
end __mqttDebug

-- ========================================
-- UTILITY FUNCTIONS (PRIVATE)
-- ========================================

private function __encodeRemainingLength pLength
   -- Remaining Length is a 1-to-4 byte varint; 268435455 is the largest value
   -- it can carry. Past that the loop below just kept going and emitted a FIFTH
   -- continuation byte, which no broker can parse - it reads the extra byte as
   -- the start of the variable header and every packet after it is garbage.
   -- Return empty instead, and let the caller refuse the packet.
   if pLength is not a number or pLength < 0 or pLength > 268435455 then
      return empty
   end if

   local tResult, tByte
   put empty into tResult

   repeat
      put pLength mod 128 into tByte
      divide pLength by 128
      put trunc(pLength) into pLength

      if pLength > 0 then
         put tByte bitOr 0x80 into tByte
      end if

      put tResult & __numToByte(tByte) into tResult

      if pLength <= 0 then
         exit repeat
      end if
   end repeat

   return tResult
end __encodeRemainingLength

private function __decodeRemainingLength pBuffer, pStartPos
   local tMultiplier, tValue, tByte, tPos
   put 1 into tMultiplier
   put 0 into tValue
   put pStartPos into tPos
   
   repeat 4 times
      if tPos > the number of bytes of pBuffer then
         return empty
      end if
      
      put byteToNum(byte tPos of pBuffer) into tByte
      add ((tByte bitAnd 0x7F) * tMultiplier) to tValue
      multiply tMultiplier by 128
      
      add 1 to tPos
      
      if (tByte bitAnd 0x80) = 0 then
         return tValue & "," & (tPos - pStartPos)
      end if
   end repeat
   
   return empty
end __decodeRemainingLength

private function __encodeString pString
   -- MQTT length-prefixed fields are 16-bit BYTE counts. Counting characters
   -- here is the framing bug that only shows up on a non-ASCII topic: the
   -- prefix says 5, six bytes go out, and every packet after it is misread.
   local tLen
   put the number of bytes of pString into tLen

   -- The field cannot express more than 65535. Callers validate their own
   -- inputs; this is the backstop that keeps a truncated length from silently
   -- desynchronising the stream if one ever does not.
   if tLen > 65535 then
      return empty
   end if

   return binaryEncode("n", tLen) & pString
end __encodeString

private function __encodeUTF8String pString
   local tUTF8
   put textEncode(pString, "UTF-8") into tUTF8
   return __encodeString(tUTF8)
end __encodeUTF8String

private function __generatePacketID pConnID
   -- IDs run 1..65535 and wrap. After a wrap the counter can land on an ID that
   -- is still in flight, and reusing it makes the broker's next ack ambiguous -
   -- one acknowledgment, two messages claiming it. Skip anything still
   -- outstanding. The scan is bounded so a client that somehow filled the whole
   -- ID space fails loudly instead of spinning forever.
   local tTries
   put 0 into tTries

   repeat
      add 1 to gMQTTConnections[pConnID]["packetID"]

      if gMQTTConnections[pConnID]["packetID"] > 65535 then
         put 1 into gMQTTConnections[pConnID]["packetID"]
      end if

      if gMQTTConnections[pConnID]["packetID"] < 1 then
         put 1 into gMQTTConnections[pConnID]["packetID"]
      end if

      local tCandidate
      put gMQTTConnections[pConnID]["packetID"] into tCandidate

      if gMQTTConnections[pConnID]["pendingAcks"][tCandidate] is empty and \
            gMQTTConnections[pConnID]["pendingSubAcks"][tCandidate] is empty and \
            gMQTTConnections[pConnID]["pendingUnsubAcks"][tCandidate] is empty then
         return tCandidate
      end if

      add 1 to tTries
      if tTries >= 65535 then
         __mqttLog "ERROR: no free packet ID for" && pConnID
         return empty
      end if
   end repeat
end __generatePacketID

private function __numToByte pNum
   return binaryEncode("C", pNum)
end __numToByte

private function __scheduleMessage pHandler, pDelayMS
   -- Schedule a message and reliably return its message ID.
   -- Avoids the fragile "line -1 of the pendingMessages" approach, which can
   -- capture an unrelated pending message belonging to the host application.
   -- The pendingMessages id is the text before the first comma on each line.
   -- Extract it with offset() so we do not depend on the global itemDelimiter.
   --
   -- The before-set is an ARRAY, not a string. It used to be one long string
   -- searched with `tBefore does not contain (cr & tID & cr)` - which is not
   -- xTalk (the parser errors on `does`, taking the whole script with it) and
   -- which also re-scanned the accumulated text once per pending message. A
   -- host app with a busy timer queue paid that cost on every schedule.
   local tBefore, tLine, tID
   repeat for each line tLine in the pendingMessages
      put char 1 to (offset(comma, tLine & comma) - 1) of tLine into tID
      put true into tBefore[tID]
   end repeat

   send pHandler to me in pDelayMS milliseconds

   repeat for each line tLine in the pendingMessages
      put char 1 to (offset(comma, tLine & comma) - 1) of tLine into tID
      if tBefore[tID] is not true then
         return tID
      end if
   end repeat

   return empty
end __scheduleMessage

command mqttTestCallbackFromLibrary pTargetCard
   -- Try to send from library to card
   local tErr
   try
      send "testLibraryToCard" to pTargetCard
      return "OK - send succeeded"
   catch tErr
      return "ERROR:" && tErr
   end try
end mqttTestCallbackFromLibrary

-- <<< END EMBEDDED LIBRARIES <<<

constant kMdUiVersion = "mqtt-dashboard-kit2-1"

-- Defaults: a public test broker, so the form is useful before it is edited.
constant kMdHost = "broker.hivemq.com"
constant kMdPort = 1883
constant kMdTopic = "libmqttxt/demo"

-- The dashboard tick. 1 Hz is enough for counters; nothing here drives the
-- protocol, which runs entirely on the library's own socket and timer paths.
constant kMdDashMs = 1000

local sConnected        -- "true" once CONNACK landed
local sConnKey          -- "host:port" of the connection this window owns
local sDashOn           -- "true" while the 1 Hz dashboard chain may reschedule
local sTLS              -- "true" when the TLS checkbox is ticked
local sVerify           -- "true" when the verify-certificate checkbox is ticked
local sAuto             -- "true" when auto-reconnect is ticked
local sRetain           -- "true" when the publish retain checkbox is ticked
local sRecv             -- messages delivered to mdOnMessage this session
local sPreOpenRan       -- "true" once preOpenStack has fired; the boot check reads it

-- ==== SUITE UI KIT v2 BEGIN (verbatim copy; master: tools/ui-kit.livecodescript; gate: tools/check-ui-kit-drift.py) ====
-- The family look as one carried block: the kit owns colors, chrome, and
-- control construction; the adopting demo owns layout (explicit rects) and
-- logic. Builders are idempotent (create-if-missing), so a rebuild on
-- reopen is safe. Do not edit this block in place: change the master and
-- re-carry it.
constant kUiBg = "240,242,246"
constant kUiCard = "255,255,255"
constant kUiCardLine = "225,229,236"
constant kUiAccent = "44,90,160"
constant kUiAccentText = "255,255,255"
constant kUiAccentSub = "199,216,241"
constant kUiInk = "28,34,46"
constant kUiInkSoft = "95,104,118"
constant kUiMuted = "138,146,158"
constant kUiOk = "23,128,63"
constant kUiWarn = "150,110,40"
constant kUiBad = "176,58,46"
constant kUiFieldLine = "203,210,222"
constant kUiCodeBg = "244,246,250"
constant kUiCodeLine = "210,217,229"

-- The platform's own terminal face for codes, keys, tables and logs, so
-- the monospace surfaces look native instead of typewriter-Courier. A
-- missing face substitutes the engine default - a soft, safe failure.
function uiMonoFont
   if the platform is "MacOS" then
      return "Menlo"
   else if the platform is "Win32" then
      return "Consolas"
   end if
   return "DejaVu Sans Mono"
end uiMonoFont

-- The window chrome every adopter shares: stack size, page ground, the
-- title band, and the one status line under it (write it with uiStatus).
-- pBandH is optional (default 30); a taller band leaves room for an
-- adopter's tagline or status pill, placed by the adopter.
command uiChrome pTitle, pWidth, pHeight, pBandH
   local tFH
   if pBandH is empty then
      put 30 into pBandH
   end if
   set the width of this stack to pWidth
   set the height of this stack to pHeight
   set the backgroundColor of this card to kUiBg
   set the title of this stack to pTitle
   uiGfx "uiBand", "0,0," & pWidth & "," & pBandH
   set the style of graphic "uiBand" to "rectangle"
   set the filled of graphic "uiBand" to true
   set the backgroundColor of graphic "uiBand" to kUiAccent
   set the lineSize of graphic "uiBand" to 0
   uiLabel "uiTitle", "  " & pTitle, "0,0," & pWidth & "," & pBandH, 15
   set the foregroundColor of field "uiTitle" to kUiAccentText
   set the textStyle of field "uiTitle" to "bold"
   -- center the title on the band midline by MEASURED height (never guess
   -- font metrics - the family's OXT passes clipped hand-sized labels)
   put the formattedHeight of field "uiTitle" into tFH
   if tFH > 0 and tFH < pBandH then
      set the height of field "uiTitle" to tFH
      set the top of field "uiTitle" to ((pBandH - tFH) div 2)
   end if
   uiLabel "uiStatus", empty, "16," & (pBandH + 4) & "," & (pWidth - 16) & "," & (pBandH + 22), 11
end uiChrome

-- The status line, colored by what it reports: "ok", "warn", "bad", or
-- anything else for neutral/muted. Demos route probe results through this
-- so a stack that cannot run says so in red, at the top, immediately.
command uiStatus pText, pKind
   -- PIN THE STACK, THEN CHECK THE FIELD EXISTS. Both halves were missing and
   -- both are load-bearing on a TIMER-driven call.
   --
   -- An unqualified `field "uiStatus"` resolves against the DEFAULTSTACK, not
   -- against the stack whose script is running. Inside openStack those are the
   -- same thing, which is why every demo's startup status line always worked.
   -- A handler arriving from `send ... in` has no such guarantee: with a second
   -- stack in front, the write lands in the wrong stack or resolves to nothing.
   --
   -- THE PIN RESTS ON THE DOCUMENTED RESOLUTION RULE, NOT ON A THROW WE SAW.
   -- This comment used to cite enet-lan-chat's once-a-second dashboard throwing
   -- "Chunk: error in object expression" REPEATEDLY on 2026-08-18, and dht-chat
   -- hiding the same fault behind a guard. Neither half held up: that throw was
   -- the ARGUMENT at the call site (`the number of keys of sPeers`, which does
   -- not parse - OXT-ENGINE-NOTES 1.7) and never reached uiStatus at all, and
   -- the guard credited to the kit was dht-chat's own wxSetStatus wrapper - the
   -- master had no existence check until the same commit that added this pin.
   -- What a guarded call site WOULD do if the defaultStack diverged is fail
   -- silently, which is the argument for a gate rather than for more guards.
   -- See OXT-ENGINE-NOTES 5.3, now classed DOCUMENTED.
   --
   -- The pin is holde-em's idiom, engine-proven there, and its comment names
   -- this exact hazard. The existence check stays as well: a demo may call the
   -- status line before its chrome is built, and a cosmetic line must never
   -- take down a poll chain.
   set the defaultStack to the short name of this stack
   if there is no field "uiStatus" then
      exit uiStatus
   end if
   put pText into field "uiStatus"
   if pKind is "ok" then
      set the foregroundColor of field "uiStatus" to kUiOk
   else if pKind is "warn" then
      set the foregroundColor of field "uiStatus" to kUiWarn
   else if pKind is "bad" then
      set the foregroundColor of field "uiStatus" to kUiBad
   else
      set the foregroundColor of field "uiStatus" to kUiInkSoft
   end if
end uiStatus

-- A static caption at a FIXED rect (idempotent, borderless, transparent).
-- Use uiWrap instead when the text may wrap or the exact height matters.
command uiLabel pName, pText, pRect, pSize
   if there is no field pName then
      create field
      set the name of the last field to pName
   end if
   set the rect of field pName to pRect
   set the lockText of field pName to true
   set the borderWidth of field pName to 0
   set the opaque of field pName to false
   set the threeD of field pName to false
   set the textSize of field pName to pSize
   set the foregroundColor of field pName to kUiInk
   put pText into field pName
end uiLabel

-- A caption FIT to its measured text height (top pinned). The fit does
-- double duty: it prevents clipping (hand-sized rects clipped text on a
-- real Windows pass), and it makes `the height of field` equal the TEXT
-- height, which midline centering relies on. GUARD: only resize on a sane
-- measurement - a 0/blank formattedHeight (possible before the window is
-- fully realized) would collapse the label. Empty labels are skipped
-- (they receive their text later).
command uiWrap pName, pText, pRect, pSize
   local tOldDelim, tTop, tFH
   uiLabel pName, pText, pRect, pSize
   if pText is not empty then
      put the itemDelimiter into tOldDelim
      set the itemDelimiter to comma
      put item 2 of pRect into tTop
      set the itemDelimiter to tOldDelim
      put the formattedHeight of field pName into tFH
      if tFH > 0 then
         set the height of field pName to tFH
         set the top of field pName to tTop
      end if
   end if
end uiWrap

-- A small, muted caption (hints, toggle descriptions, empty states).
command uiCap pName, pText, pRect
   uiWrap pName, pText, pRect, 10
   set the foregroundColor of field pName to kUiMuted
end uiCap

-- A numbered bold section header with a hairline divider placed below the
-- label's ACTUAL (fit-to-text) bottom, so it tracks the real header
-- height on any platform's line metrics.
command uiSection pName, pText, pRect
   local tOldDelim, tL, tR, tB
   uiWrap pName, pText, pRect, 13
   set the textStyle of field pName to "bold"
   put the itemDelimiter into tOldDelim
   set the itemDelimiter to comma
   put item 1 of pRect into tL
   put item 3 of pRect into tR
   set the itemDelimiter to tOldDelim
   put the bottom of field pName into tB
   uiGfx (pName & "Line"), (tL & comma & (tB + 1) & comma & tR & comma & (tB + 2))
   set the style of graphic (pName & "Line") to "rectangle"
   set the filled of graphic (pName & "Line") to true
   set the backgroundColor of graphic (pName & "Line") to kUiCardLine
   set the lineSize of graphic (pName & "Line") to 0
end uiSection

-- A one-line entry field, mono and flat ("modern input" chrome instead of
-- the engine's sunken-3D default), because hex, addresses and paths are
-- what this suite's demos edit.
command uiInput pName, pRect
   if there is no field pName then
      create field
      set the name of the last field to pName
   end if
   set the rect of field pName to pRect
   set the lockText of field pName to false
   set the traversalOn of field pName to true
   set the threeD of field pName to false
   set the borderWidth of field pName to 1
   set the borderColor of field pName to kUiFieldLine
   set the opaque of field pName to true
   set the backgroundColor of field pName to kUiCard
   -- 4-item margins form ONLY: OXT ignored the single-number form on a
   -- real engine pass
   set the margins of field pName to "6,6,6,6"
   set the textSize of field pName to 11
   set the textFont of field pName to uiMonoFont()
end uiInput

-- A read-only output/log area: mono, flat, scrolling, hairline-bordered.
command uiArea pName, pRect
   if there is no field pName then
      create field
      set the name of the last field to pName
   end if
   set the rect of field pName to pRect
   set the lockText of field pName to true
   set the traversalOn of field pName to false
   set the threeD of field pName to false
   set the borderWidth of field pName to 1
   set the borderColor of field pName to kUiCardLine
   set the opaque of field pName to true
   set the backgroundColor of field pName to kUiCard
   set the margins of field pName to "6,6,6,6"
   set the vScrollbar of field pName to true
   set the textSize of field pName to 10
   set the textFont of field pName to uiMonoFont()
end uiArea

-- A row-selectable table: uiArea plus list behavior and tab stops
-- ("230,340"-style comma lists). Pair it with a uiLabel header carrying
-- the same tabStops, set by the adopter.
command uiTable pName, pRect, pTabStops
   uiArea pName, pRect
   set the listBehavior of field pName to true
   if pTabStops is not empty then
      set the tabStops of field pName to pTabStops
   end if
end uiTable

command uiButton pName, pLabel, pRect
   if there is no button pName then
      create button
      set the name of the last button to pName
   end if
   set the rect of button pName to pRect
   set the label of button pName to pLabel
end uiButton

-- A script-managed checkbox: autoHilite OFF because the family drives
-- toggle state from script locals (a mode the environment cannot honour
-- must never stay silently armed - the quickshare pill lesson), so the
-- hilite is set explicitly by the adopter's state machine.
command uiCheckbox pName, pLabel, pRect
   uiButton pName, pLabel, pRect
   set the style of button pName to "checkbox"
   set the autoHilite of button pName to false
end uiCheckbox

-- Create-or-reuse a named graphic: the chrome layer (cards, pills,
-- dividers, drop targets). Created BEFORE the controls that sit on it, so
-- it stays behind them in z-order. roundRect + antialiased by default;
-- callers restyle.
command uiGfx pName, pRect
   if there is no graphic pName then
      create graphic
      set the name of the last graphic to pName
   end if
   set the rect of graphic pName to pRect
   set the style of graphic pName to "roundRect"
   set the antialiased of graphic pName to true
end uiGfx

-- A dashboard card: a rounded, hairline-bordered, softly shadowed panel
-- placed BEHIND a group of controls. It only groups visually; clicks on
-- it fall through to the adopter's `default: pass mouseUp`.
command uiPanel pName, pRect
   local tShadow, tErr
   uiGfx pName, pRect
   set the roundRadius of graphic pName to 14
   set the filled of graphic pName to true
   set the backgroundColor of graphic pName to kUiCard
   set the lineSize of graphic pName to 1
   -- a graphic strokes its outline in the foregroundColor; borderColor is
   -- set too so whichever the engine honours, the hairline lands
   set the foregroundColor of graphic pName to kUiCardLine
   set the borderColor of graphic pName to kUiCardLine
   -- soft drop shadow is an LC-9-era feature, so it is a PURE
   -- enhancement: an engine without it just skips it (the try) and the
   -- hairline still separates the card from the page
   try
      put "90,98,114" into tShadow["color"]
      put 30 into tShadow["opacity"]
      put 10 into tShadow["size"]
      put 0 into tShadow["spread"]
      put 2 into tShadow["distance"]
      put 90 into tShadow["angle"]
      set the dropShadow of graphic pName to tShadow
   catch tErr
   end try
end uiPanel

-- A status pill: rounded chip + centered label, colored by state kind -
-- "ok" green, "wait" amber, "bad" red, anything else the muted on-band
-- blue. The adopter repaints it from ONE place so its handlers cannot
-- disagree about the colors (the nocloud Tor-pill discipline).
command uiPill pName, pText, pRect, pKind
   local tOldDelim, tTop, tBot, tFH
   put the itemDelimiter into tOldDelim
   set the itemDelimiter to comma
   put item 2 of pRect into tTop
   put item 4 of pRect into tBot
   set the itemDelimiter to tOldDelim
   uiGfx (pName & "Bg"), pRect
   set the roundRadius of graphic (pName & "Bg") to (tBot - tTop)
   set the filled of graphic (pName & "Bg") to true
   set the lineSize of graphic (pName & "Bg") to 0
   if pKind is "ok" then
      set the backgroundColor of graphic (pName & "Bg") to "205,240,214"
   else if pKind is "wait" then
      set the backgroundColor of graphic (pName & "Bg") to "250,240,199"
   else if pKind is "bad" then
      set the backgroundColor of graphic (pName & "Bg") to "243,215,213"
   else
      set the backgroundColor of graphic (pName & "Bg") to "31,64,116"
   end if
   uiLabel pName, pText, pRect, 10
   set the textAlign of field pName to "center"
   if pKind is "ok" then
      set the foregroundColor of field pName to "22,101,52"
   else if pKind is "wait" then
      set the foregroundColor of field pName to "133,98,14"
   else if pKind is "bad" then
      set the foregroundColor of field pName to "135,44,38"
   else
      set the foregroundColor of field pName to kUiAccentSub
   end if
   -- center the label on the chip midline by measured height
   put the formattedHeight of field pName into tFH
   if tFH > 0 and tFH < (tBot - tTop) then
      set the height of field pName to tFH
      set the top of field pName to (tTop + ((tBot - tTop - tFH) div 2))
   end if
end uiPill

-- Click-to-copy feedback: flash the button's label to "Copied!" for
-- 1200 ms, then restore the real label (stashed in uRealLabel).
command uiCopyFlash pBtn
   if there is no button pBtn then
      exit uiCopyFlash
   end if
   if the uRealLabel of button pBtn is empty then
      set the uRealLabel of button pBtn to the label of button pBtn
   end if
   set the label of button pBtn to "Copied!"
   send ("uiCopyFlashReset" && quote & pBtn & quote) to me in 1200 milliseconds
end uiCopyFlash

command uiCopyFlashReset pBtn
   -- Pinned for the same reason uiStatus is: this one arrives 1200ms later by
   -- `send`, so the defaultStack is whatever is current THEN, not the stack
   -- that flashed the button. Its existence guard means the failure is silent
   -- rather than loud - the label just never comes back, or comes back on
   -- another stack's button - which is why it needed finding rather than
   -- reporting itself.
   set the defaultStack to the short name of this stack
   if there is no button pBtn then
      exit uiCopyFlashReset
   end if
   if the uRealLabel of button pBtn is not empty then
      set the label of button pBtn to the uRealLabel of button pBtn
   end if
end uiCopyFlashReset

-- The honesty/custody footer every demo carries: what the stack is, what
-- it is not, and its engine-evidence state.
command uiFooter pText, pRect
   uiWrap "uiFooter", pText, pRect, 11
   set the textStyle of field "uiFooter" to "italic"
   set the foregroundColor of field "uiFooter" to kUiMuted
end uiFooter
-- ---- contextual menus -------------------------------------------------------
--
-- A RIGHT-CLICK MENU, as one hidden popup button per menu.
--
-- The engine's contextual menu is a BUTTON whose style is "menu" and whose
-- menuMode is "popup": its text is the item list, one per line, `popup`
-- shows it at a point, and choosing an item sends `menuPick` with the item's
-- LABEL. The button needs no script - with no handler of its own the message
-- walks the hierarchy and arrives at the card and then the stack, which is
-- where an adopter's router already lives.
--
-- OPT-IN, AND THAT IS DELIBERATE. This block is carried byte-identical by
-- every demo in the suite, so a kit that BUILT a menu would change eighteen
-- stacks to serve one. Nothing here runs unless an adopter calls
-- uiContextMenu, and a demo that never does is unchanged in behaviour.
--
-- NONE OF THIS HAS RUN ON AN ENGINE. `popup`, `menuMode` and `menuPick` are
-- documented LiveCode and OpenXTalk is a fork of it, but this suite has never
-- exercised a menu and the headless runners model no mouse button at all - so
-- an adopter's right-click handler belongs in a `try`, and losing the menu
-- must never cost the ordinary click. uiPopupMenu answers whether it worked
-- rather than throwing, so the caller can decide.

command uiContextMenu pName, pItems
   -- One hidden popup button, created once and refilled on every call so a
   -- menu can change with the screen it belongs to. The rect is small and
   -- inside the window because a stack's controls are measured by
   -- tools/check-stack-size.py whether they are visible or not.
   if there is no button pName then
      -- THE KIT'S OWN IDIOM, and not `create button pName`. Every other
      -- builder in this block creates the control unnamed and then names the
      -- last one, which is the spelling the family's headless runners model
      -- and the one every adopter has already been through an engine with.
      create button
      set the name of the last button to pName
   end if
   set the style of button pName to "menu"
   set the menuMode of button pName to "popup"
   set the text of button pName to pItems
   set the rect of button pName to "2,2,18,18"
   set the visible of button pName to false
end uiContextMenu

function uiPopupMenu pName
   -- WHICH menu, recorded before it opens. menuPick carries the item's label
   -- and nothing else, so an adopter with two menus would otherwise have to
   -- keep every label in the stack globally unique - a constraint that is
   -- invisible until the day two screens both want "Copy".
   --
   -- Answers true or false rather than throwing: on an engine without popup
   -- menus the right thing is for the click to do nothing, not for the
   -- adopter's mouseDown to die.
   local tError
   if there is no button pName then
      return false
   end if
   set the uUiMenu of this stack to pName
   try
      popup button pName at the clickLoc
   catch tError
      set the uUiMenu of this stack to empty
      return false
   end try
   return true
end uiPopupMenu

function uiMenuSource
   -- The name of the menu the last menuPick came from.
   return the uUiMenu of this stack
end uiMenuSource

-- ==== SUITE UI KIT v2 END ====

-- ---- lifecycle --------------------------------------------------------------

on preOpenStack
   -- THE ONE LINE AN EMBEDDING STACK OWES THE LIBRARY. See the header.
   put "true" into sPreOpenRan
   mqttInitialize
end preOpenStack

on openStack
   mdBuild
   mdStart
   -- LAST, not first: the check reads the state mdStart establishes (the
   -- callbacks, the armed dashboard), and asserting before it would test
   -- nothing.
   mdScRun
end openStack

on closeStack
   mdStop
end closeStack

-- ---- self-building UI (idempotent; rebuilds on a version bump) --------------

command mdBuild
   -- A stack SAVED with this UI already built does not need it built again, and
   -- rebuilding one that was saved by an OLDER version must start from a clean
   -- card - otherwise a control this version dropped stays on screen forever,
   -- wired to nothing.
   if the uMdUiVersion of this stack is kMdUiVersion \
         and there is a field "mdHost" then
      exit mdBuild
   end if

   mdClearGeneratedUI
   lock screen

   uiChrome "libMQTTxt - MQTT Dashboard", 1000, 620, 34

   -- Panels first, so they sit BEHIND the controls placed on them.
   uiPanel "mdBrokerPanel", "16,66,494,336"
   uiPanel "mdSubPanel", "16,348,494,556"
   uiPanel "mdPubPanel", "506,66,984,284"
   uiPanel "mdLogPanel", "506,296,984,556"

   -- 1. Broker
   uiSection "mdSecBroker", "1. Broker", "32,78,370,98"
   -- Through mdSetPill, not uiPill directly: the pill's rect and its four
   -- state colours get exactly ONE owner, so a rebuild here and a repaint from
   -- a callback cannot disagree about where it sits or what it says.
   mdSetPill "offline", "bad"

   uiCap "mdHostCap", "Host", "32,110,340,126"
   uiInput "mdHost", "32,128,340,152"
   uiCap "mdPortCap", "Port", "352,110,478,126"
   uiInput "mdPort", "352,128,478,152"

   uiCap "mdClientCap", "Client ID (blank generates one)", "32,160,478,176"
   uiInput "mdClient", "32,178,478,202"

   uiCap "mdUserCap", "Username (optional)", "32,210,250,226"
   uiInput "mdUser", "32,228,250,252"
   uiCap "mdPassCap", "Password (needs a username)", "262,210,478,226"
   uiInput "mdPass", "262,228,478,252"

   uiCheckbox "mdTLS", "TLS", "32,260,128,282"
   uiCheckbox "mdVerify", "Verify cert", "136,260,266,282"
   uiCheckbox "mdAuto", "Auto-reconnect", "274,260,430,282"

   uiButton "mdConnect", "Connect", "32,296,146,320"
   uiButton "mdDisconnect", "Disconnect", "154,296,268,320"
   uiButton "mdSelfTest", "Self-test", "276,296,390,320"

   -- 2. Subscribe
   uiSection "mdSecSub", "2. Subscribe", "32,360,478,380"
   uiCap "mdTopicCap", "Topic filter (+ and # allowed)", "32,392,340,408"
   uiInput "mdTopic", "32,410,340,434"
   uiCap "mdQoSCap", "QoS", "352,392,478,408"
   uiInput "mdQoS", "352,410,478,434"
   uiButton "mdSubscribe", "Subscribe", "32,442,146,466"
   uiButton "mdUnsubscribe", "Unsubscribe", "154,442,278,466"
   uiCap "mdSubsCap", "Active subscriptions", "32,474,478,490"
   uiArea "mdSubs", "32,492,478,544"

   -- 3. Publish
   uiSection "mdSecPub", "3. Publish", "522,78,968,98"
   uiCap "mdPubTopicCap", "Topic (no wildcards)", "522,110,968,126"
   uiInput "mdPubTopic", "522,128,968,152"
   uiCap "mdPubMsgCap", "Payload", "522,160,968,176"
   uiInput "mdPubMsg", "522,178,968,218"
   uiCap "mdPubQoSCap", "QoS", "522,226,640,242"
   uiInput "mdPubQoS", "522,244,640,268"
   uiCheckbox "mdPubRetain", "Retain", "652,246,790,266"
   uiButton "mdPublish", "Publish", "800,244,968,268"

   -- 4. Traffic
   uiSection "mdSecLog", "4. Traffic", "522,308,968,328"
   uiCap "mdStats", "no connection", "522,336,968,352"
   uiCap "mdMsgCap", "Received messages", "522,358,968,374"
   uiArea "mdMessages", "522,376,968,448"
   uiCap "mdLogCap", "Library log", "522,456,968,472"
   uiArea "mdLog", "522,474,968,544"

   uiFooter "libMQTTxt " & mdLibVersion() & " - a demo of the library, not a " \
         & "production client. Verified statically; needs an OXT pass: no " \
         & "line of this stack has run on an engine against a live broker.", \
         "16,566,984,608"

   -- Seed the form. Done here rather than in the builders so a rebuild on
   -- reopen does not wipe what the operator typed.
   if field "mdHost" is empty then
      put kMdHost into field "mdHost"
      put kMdPort into field "mdPort"
      put kMdTopic into field "mdTopic"
      put "0" into field "mdQoS"
      put kMdTopic into field "mdPubTopic"
      put "hello from libMQTTxt" into field "mdPubMsg"
      put "0" into field "mdPubQoS"
   end if

   set the uMdUiVersion of this stack to kMdUiVersion
   unlock screen
end mdBuild

-- Remove what a PREVIOUS version of mdBuild generated, so a version bump
-- starts from a clean card. Graphics are included, unlike the shortest form of
-- this handler in the family: this demo's look is built from uiPanel cards,
-- uiSection divider lines and a uiPill chip, and every one of those is a
-- graphic. Dropping only fields and buttons would leave the old layout's
-- cards and dividers behind the new one's controls.
command mdClearGeneratedUI
   local tI
   lock screen
   repeat with tI = the number of fields of this card down to 1
      if the short name of field tI of this card begins with "md" then
         delete field tI of this card
      end if
   end repeat
   repeat with tI = the number of buttons of this card down to 1
      if the short name of button tI of this card begins with "md" then
         delete button tI of this card
      end if
   end repeat
   repeat with tI = the number of graphics of this card down to 1
      if the short name of graphic tI of this card begins with "md" then
         delete graphic tI of this card
      end if
   end repeat
   unlock screen
end mdClearGeneratedUI

-- ---- start / stop -----------------------------------------------------------

command mdStart
   -- AGAIN, on purpose. The fifth engine run (2026-09-06) opened this demo on a
   -- fresh engine and the boot check found the library uninitialised:
   -- preOpenStack had not taken effect. Whether that was the engine or the way
   -- the stack was opened is not established, and it does not matter here - a
   -- demo that works only when one lifecycle message arrives is a demo that
   -- sometimes does not work. mqttInitialize is idempotent; calling it from
   -- the start handler as well costs nothing and closes the gap. The check
   -- below still reports whether preOpenStack fired, as its own line.
   mqttInitialize

   put false into sConnected
   put empty into sConnKey
   put 0 into sRecv
   put false into sTLS
   put false into sAuto
   put false into sRetain
   -- Verification defaults ON, matching the library's own fail-closed default.
   put true into sVerify
   mdPaintToggles

   -- Callbacks BEFORE anything can connect, so nothing that arrives during a
   -- handshake is dropped. The target is explicit even though the library would
   -- capture this stack anyway: an explicit target is one less thing to reason
   -- about when the handlers move.
   mqttSetCallbackTarget the long id of me
   mqttSetLogCallback "mdOnLog"
   mqttSetMessageCallback "mdOnMessage"
   mqttSetStateChangeCallback "mdOnState"
   mqttSetReconnectCallback "mdOnReconnect"

   put "true" into sDashOn
   send "mdDashOnce" to me in kMdDashMs milliseconds

   uiStatus "Ready - edit the broker details and press Connect.", empty
   mdLog "libMQTTxt " & mdLibVersion() & " ready (embedded in this stack)"
end mdStart

command mdStop
   put false into sDashOn
   -- The one mandatory teardown: without it the sockets and the keep-alive
   -- timers outlive this window.
   mqttCleanupAll
end mdStop

-- ---- the dashboard tick -----------------------------------------------------

command mdDashOnce
   -- PIN THE STACK FIRST. This handler arrives from `send ... in`, where an
   -- unqualified control reference resolves against the DEFAULTSTACK rather
   -- than the stack whose script is running (OXT-ENGINE-NOTES 5.3). Pinning at
   -- the timer ENTRY POINT covers everything downstream.
   set the defaultStack to the short name of this stack

   local tStats, tLine, tErr
   try
      if sConnKey is empty then
         put "no connection" into tLine
      else
         put mqttGetStatistics(mdHostOf(sConnKey), mdPortOf(sConnKey)) into tStats
         put "sent " & tStats["messagesSent"] & " (" & tStats["bytesSent"] \
               & "B)  received " & tStats["messagesReceived"] & " (" \
               & tStats["bytesReceived"] & "B)  reconnects " \
               & tStats["reconnections"] & "  delivered here " & sRecv into tLine
      end if
      put tLine into field "mdStats"
   catch tErr
      -- A cosmetic line must never cost the tick; the chain below is what
      -- keeps the window honest about the connection.
      put "dashboard error:" && tErr into field "mdStats"
   end try

   if sDashOn is "true" then
      send "mdDashOnce" to me in kMdDashMs milliseconds
   end if
end mdDashOnce

-- ---- input routing ----------------------------------------------------------

on mouseUp
   local tName
   put the short name of the target into tName

   switch tName
      case "mdConnect"
         mdConnectNow
         break
      case "mdDisconnect"
         mdDisconnectNow
         break
      case "mdSelfTest"
         mdRunSelfTest
         break
      case "mdSubscribe"
         mdSubscribeNow
         break
      case "mdUnsubscribe"
         mdUnsubscribeNow
         break
      case "mdPublish"
         mdPublishNow
         break
      case "mdTLS"
         mdToggle "sTLS"
         break
      case "mdVerify"
         mdToggle "sVerify"
         break
      case "mdAuto"
         mdToggle "sAuto"
         break
      case "mdPubRetain"
         mdToggle "sRetain"
         break
      default
         pass mouseUp
   end switch
end mouseUp

-- The checkboxes are script-driven (the kit sets autoHilite off), so state
-- lives here and the hilite is painted from ONE place - the handlers cannot
-- disagree about what is ticked.
command mdToggle pWhich
   switch pWhich
      case "sTLS"
         put (sTLS is not "true") into sTLS
         -- Moving to TLS with verification off is the combination worth a
         -- word: it looks secure and authenticates nobody.
         if sTLS is "true" and mdTrim(field "mdPort") is kMdPort then
            put 8883 into field "mdPort"
         end if
         break
      case "sVerify"
         put (sVerify is not "true") into sVerify
         if sVerify is not "true" then
            mdLog "WARNING: certificate verification OFF - encrypted, but " \
                  & "authenticates nobody"
         end if
         break
      case "sAuto"
         put (sAuto is not "true") into sAuto
         break
      case "sRetain"
         put (sRetain is not "true") into sRetain
         break
   end switch
   mdPaintToggles
end mdToggle

command mdPaintToggles
   set the hilite of button "mdTLS" to (sTLS is "true")
   set the hilite of button "mdVerify" to (sVerify is "true")
   set the hilite of button "mdAuto" to (sAuto is "true")
   set the hilite of button "mdPubRetain" to (sRetain is "true")
end mdPaintToggles

-- ---- actions ----------------------------------------------------------------

command mdConnectNow
   local tHost, tPort, tResult

   put mdTrim(field "mdHost") into tHost
   put mdTrim(field "mdPort") into tPort

   if tHost is empty then
      uiStatus "Enter a broker host first.", "bad"
      exit mdConnectNow
   end if

   if sConnected is "true" then
      uiStatus "Already connected - disconnect first.", "warn"
      exit mdConnectNow
   end if

   -- 8883 is the conventional TLS port. Connecting to it in the clear is legal
   -- and occasionally deliberate, but the usual outcome is a broker that
   -- accepts the TCP connection and closes it on the first plaintext byte -
   -- which reads in the log as a connection that simply dropped.
   if tPort is 8883 and sTLS is not "true" then
      mdLog "NOTE: port 8883 without TLS - if the socket closes right after " \
            & "CONNECT, tick TLS and try again"
   end if

   put tHost & ":" & tPort into sConnKey
   uiStatus "Connecting to" && sConnKey & "...", empty
   mdSetPill "connecting", "wait"

   put mqttConnect(tHost, tPort, mdTrim(field "mdClient"), \
         mdTrim(field "mdUser"), field "mdPass", 60, (sTLS is "true"), true, \
         empty, empty, 0, false, (sVerify is "true"), (sAuto is "true"), \
         empty) into tResult

   -- "OK" means the socket open was INITIATED. Connected is what CONNACK
   -- says, and that arrives at mdOnState.
   if tResult is not "OK" then
      uiStatus tResult, "bad"
      mdSetPill "offline", "bad"
      put empty into sConnKey
      mdLog tResult
   end if
end mdConnectNow

command mdDisconnectNow
   if sConnKey is empty then
      uiStatus "Nothing to disconnect.", "warn"
      exit mdDisconnectNow
   end if

   mqttDisconnect mdHostOf(sConnKey), mdPortOf(sConnKey)
   put false into sConnected
   put empty into sConnKey
   mdSetPill "offline", "bad"
   mdRefreshSubs
   uiStatus "Disconnected.", empty
end mdDisconnectNow

command mdSubscribeNow
   local tResult
   if sConnected is not "true" then
      uiStatus "Connect before subscribing.", "warn"
      exit mdSubscribeNow
   end if

   put mqttSubscribe(mdHostOf(sConnKey), mdPortOf(sConnKey), \
         mdTrim(field "mdTopic"), mdTrim(field "mdQoS")) into tResult

   if tResult is "OK" then
      uiStatus "Subscribed to" && mdTrim(field "mdTopic") & ".", "ok"
   else
      uiStatus tResult, "bad"
   end if
   mdRefreshSubs
end mdSubscribeNow

command mdUnsubscribeNow
   local tResult
   if sConnected is not "true" then
      uiStatus "Connect before unsubscribing.", "warn"
      exit mdUnsubscribeNow
   end if

   put mqttUnsubscribe(mdHostOf(sConnKey), mdPortOf(sConnKey), \
         mdTrim(field "mdTopic")) into tResult

   if tResult is "OK" then
      uiStatus "Unsubscribed from" && mdTrim(field "mdTopic") & ".", "ok"
   else
      uiStatus tResult, "bad"
   end if
   mdRefreshSubs
end mdUnsubscribeNow

command mdPublishNow
   local tResult
   if sConnected is not "true" then
      uiStatus "Connect before publishing.", "warn"
      exit mdPublishNow
   end if

   put mqttPublish(mdHostOf(sConnKey), mdPortOf(sConnKey), \
         mdTrim(field "mdPubTopic"), field "mdPubMsg", \
         mdTrim(field "mdPubQoS"), (sRetain is "true")) into tResult

   if tResult is "OK" then
      uiStatus "Published to" && mdTrim(field "mdPubTopic") & ".", "ok"
   else
      uiStatus tResult, "bad"
   end if
end mdPublishNow

command mdRunSelfTest
   local tReport
   put mqttSelfTest() into tReport
   mdLog "---- mqttSelfTest ----"
   mdLog tReport
   if tReport contains "All tests PASSED" then
      uiStatus "Library self-test passed (see the log).", "ok"
   else
      uiStatus "Library self-test FAILED - see the log.", "bad"
   end if
end mdRunSelfTest

command mdRefreshSubs
   local tSubs
   set the defaultStack to the short name of this stack

   if sConnKey is empty then
      put "(not connected)" into field "mdSubs"
      exit mdRefreshSubs
   end if

   -- One topic per line already: mqttGetSubscriptions answers `the keys of`
   -- the subscription array.
   put mqttGetSubscriptions(mdHostOf(sConnKey), mdPortOf(sConnKey)) into tSubs
   if tSubs is empty then
      put "(none)" into field "mdSubs"
      exit mdRefreshSubs
   end if
   put tSubs into field "mdSubs"
end mdRefreshSubs

-- ---- library callbacks ------------------------------------------------------

on mdOnMessage pTopic, pMessage
   set the defaultStack to the short name of this stack
   add 1 to sRecv
   put pTopic & "  " & mdOneLine(pMessage) & return after field "mdMessages"
   set the vscroll of field "mdMessages" to the formattedHeight of field "mdMessages"
end mdOnMessage

on mdOnLog pMessage
   mdLog pMessage
end mdOnLog

on mdOnState pState, pHost, pPort, pReason
   set the defaultStack to the short name of this stack

   switch pState
      case "connected"
         put "true" into sConnected
         mdSetPill "online", "ok"
         uiStatus "Connected to" && pHost & ":" & pPort & ".", "ok"
         mdRefreshSubs
         break
      case "connecting"
         mdSetPill "connecting", "wait"
         break
      case "disconnected"
         put false into sConnected
         mdSetPill "offline", "bad"
         uiStatus "Disconnected:" && pReason, "warn"
         mdRefreshSubs
         break
      case "error"
         put false into sConnected
         mdSetPill "error", "bad"
         uiStatus "Error:" && pReason, "bad"
         break
   end switch
end mdOnState

on mdOnReconnect pEvent, pHost, pPort, pAttempts
   set the defaultStack to the short name of this stack
   if pEvent is "attempting" then
      mdSetPill "retry " & pAttempts, "wait"
      uiStatus "Reconnecting to" && pHost & ":" & pPort && "(attempt" \
            && pAttempts & ")...", "warn"
   else if pEvent is "success" then
      uiStatus "Reconnected to" && pHost & ":" & pPort & ".", "ok"
   end if
end mdOnReconnect

-- ---- small helpers ----------------------------------------------------------

command mdSetPill pText, pKind
   set the defaultStack to the short name of this stack
   uiPill "mdConnPill", pText, "386,78,478,98", pKind
end mdSetPill

command mdLog pMessage
   -- Pinned: the library's keep-alive and reconnect timers reach this handler
   -- through mdOnLog, and a delayed handler's unqualified field reference
   -- resolves against the defaultStack (OXT-ENGINE-NOTES 5.3).
   set the defaultStack to the short name of this stack
   if there is no field "mdLog" then
      exit mdLog
   end if
   put pMessage & return after field "mdLog"
   set the vscroll of field "mdLog" to the formattedHeight of field "mdLog"
end mdLog

function mdLibVersion
   local tText, tPos
   put mqttTestLibrary() into tText
   -- "MQTT Library v2.12.0 loaded successfully" -> "v2.12.0"
   put offset(" v", tText) into tPos
   if tPos = 0 then
      return "(version unknown)"
   end if
   return word 1 of (char (tPos + 1) to -1 of tText)
end mdLibVersion

-- The connection key is "host:port" and the host may itself be a bracketed
-- IPv6 literal, so the split is at the LAST colon rather than the first
-- (OXT-ENGINE-NOTES 6.2: splitting a socket id on ":" and taking item 1 yields
-- EMPTY for a bare IPv6 id like ::1:54321, and a guard built that way fails
-- OPEN). The scan lives in one place so the two halves cannot disagree about
-- where the key divides.
function mdLastColon pKey
   local tPos, tI
   put 0 into tPos
   repeat with tI = 1 to the number of chars of pKey
      if char tI of pKey is ":" then
         put tI into tPos
      end if
   end repeat
   return tPos
end mdLastColon

function mdHostOf pKey
   local tPos
   put mdLastColon(pKey) into tPos
   if tPos = 0 then
      return pKey
   end if
   return char 1 to (tPos - 1) of pKey
end mdHostOf

function mdPortOf pKey
   local tPos
   put mdLastColon(pKey) into tPos
   if tPos = 0 then
      return empty
   end if
   return char (tPos + 1) to -1 of pKey
end mdPortOf

function mdTrim pText
   local tOut
   put pText into tOut
   repeat while tOut begins with " "
      delete char 1 of tOut
   end repeat
   repeat while tOut ends with " "
      delete char -1 of tOut
   end repeat
   -- A field's contents end in a return the operator never typed; left in, it
   -- becomes part of a topic name or a host and the failure reads as a typo
   -- nobody made.
   repeat while tOut ends with return
      delete char -1 of tOut
   end repeat
   return tOut
end mdTrim

-- Payloads are binary-safe, so a message may carry returns and tabs that would
-- break the one-line-per-message log. Flatten for display only.
function mdOneLine pText
   local tOut, tChar, tCode
   repeat for each char tChar in pText
      put charToNum(tChar) into tCode
      if tCode < 32 then
         put "." after tOut
      else
         put tChar after tOut
      end if
   end repeat
   if the number of chars of tOut > 120 then
      return char 1 to 117 of tOut & "..."
   end if
   return tOut
end mdOneLine

-- ==== DEMO SELF-CHECK v1 BEGIN (verbatim copy; master: tools/demo-selfcheck.livecodescript; gate: tools/check-demo-selfcheck-drift.py) ====
-- The sentinel is a CONSTANT so the probe compares against one spelling; a
-- literal repeated at the write and the read is a check that can pass while
-- disagreeing with itself.
constant kScProbe = "self-check: delayed write reached the status line"
local sScLines          -- the whole block, kept so a Copy button has something
local sScPassed, sScFailed, sScSkipped
local sScLogField       -- the adopting demo's own log field, set by scBegin

command scBegin pLogField
   put pLogField into sScLogField
   put empty into sScLines
   put 0 into sScPassed
   put 0 into sScFailed
   put 0 into sScSkipped
   scLine "== boot self-check (nothing is hosted, connected, or bound) =="
end scBegin

-- Each line is LOGGED as it is made, not accumulated and flushed at the end.
-- The first draft did both - a flush after the synchronous half and another
-- after the probe - which printed every synchronous line twice.
command scLine pText
   put pText & return after sScLines
   -- pin the stack: scFinish reaches here from a `send ... in` tick, and the
   -- field is named unqualified (OXT-ENGINE-NOTES 5.3)
   set the defaultStack to the short name of this stack
   if sScLogField is empty or there is no field sScLogField then
      exit scLine
   end if
   put pText & return after field sScLogField
   set the vscroll of field sScLogField to the formattedHeight of field sScLogField
end scLine

command scAssert pName, pOk
   if pOk then
      add 1 to sScPassed
      scLine "PASS  " & pName
   else
      add 1 to sScFailed
      scLine "FAIL  " & pName
   end if
end scAssert

-- SKIP, never FAIL, for an absent extension: a missing member is an
-- environment, and only a present-and-broken member is a defect.
command scSkip pName, pReason
   add 1 to sScSkipped
   scLine "SKIP  " & pName & " (" & pReason & ")"
end scSkip

-- An empty value prints as nothing, which reads as a missing check rather than
-- an answer of empty; every observed value in a self-check goes through here.
-- This is the playLoudness lesson (OXT-ENGINE-NOTES 5.4) as a helper: a check
-- that reports only its own name costs a whole engine pass to diagnose.
function scShow pValue
   if pValue is empty then
      return "empty"
   end if
   return pValue
end scShow

-- Returns the comma-list of control names in pList that do not exist, or empty.
-- The adopter passes EVERY control name its own script references, derived from
-- the source rather than hand-picked: a missing control is not cosmetic, it is
-- the "Chunk: error in object expression" class, and finding it at boot beats
-- finding it when somebody clicks.
function scMissing pList
   local tName, tMissing, tC, tFound
   set the defaultStack to the short name of this stack
   repeat for each item tName in pList
      -- ALL FOUR CONTROL TYPES. The kit builds fields (uiLabel/uiWrap/uiCap/
      -- uiSection/uiInput/uiArea/uiTable), buttons (uiButton/uiCheckbox) AND
      -- GRAPHICS (uiGfx/uiPanel/uiPill). The first version of this asked only
      -- about fields and buttons, so every uiPill a demo builds - the Tor
      -- pills in both Quick Shares, for one - reported as a missing control on
      -- a stack where it exists.
      --
      -- AND IMAGES (2026-08-31), for the same reason one step further out. A
      -- demo may build a control the KIT does not: coinxt's wallet paints a
      -- QR into an image, which is the only way a BMP this family builds
      -- becomes something a phone can read. Registered and built, it reported
      -- as missing - found by coinxt/tools/check-wallet-boot.py, which boots
      -- the stack headlessly, so this one cost no engine time. The lesson is
      -- the one this block already carries twice: the list of types a demo
      -- can build is not the list of types the KIT builds.
      --
      -- AND EVERY CARD (2026-08-29). Unqualified `there is a field X` answers
      -- for the CURRENT card only (engine notes 5.6) - OBSERVED when
      -- riptide's five-card boot reported all 63 off-card controls missing
      -- from a stack where every one existed. A single-card demo walks its
      -- one card here, exactly the old behaviour.
      put false into tFound
      repeat with tC = 1 to the number of cards of this stack
         if there is a field tName of card tC \
               or there is a button tName of card tC \
               or there is a graphic tName of card tC \
               or there is an image tName of card tC then
            put true into tFound
            exit repeat
         end if
      end repeat
      if not tFound then
         put tName & comma after tMissing
      end if
   end repeat
   if tMissing is empty then
      return empty
   end if
   return char 1 to -2 of tMissing
end scMissing

-- Call LAST in <pfx>ScRun. It arms the probe below; 400ms lands it before the
-- usual 800ms dashboard tick, so nothing races for the status line.
--
-- SAY THAT THE REPORT IS UNFINISHED, for the reason the suite harness carries a
-- RUN NOT FINISHED trailer: a report that simply stops reads exactly like a
-- report that ended, and the difference cost that harness a round trip on
-- 2026-08-19. One line is cheaper than the ambiguity.
command scArmProbe
   scLine "      ...one delayed-write check still to come; the count line " \
         & "below it is the end of the block"
   send "scTickProbe" to me in 400 milliseconds
end scArmProbe

-- THE ONE CHECK THAT NEEDS A TIMER.
--
-- OXT-ENGINE-NOTES 5.3 says an unqualified control reference inside a delayed
-- handler resolves against THE DEFAULTSTACK, not the stack whose script is
-- running - and that entry is classed DOCUMENTED, not OBSERVED, because this
-- tree has never actually watched the unpinned form fail. It also names the run
-- that would settle it: open a second stack in front and let a `send ... in`
-- handler write an unqualified field.
--
-- So this probe READS the defaultStack before pinning and PRINTS what it found.
-- On an ordinary run that line is context. On a run with another window in front
-- it is the measurement 5.3 is missing, taken for free and at no risk, because
-- the pin lands immediately afterwards and the write is verified.
command scTickProbe
   local tWas, tPrev, tPrevColor, tGot, tErr
   put the defaultStack into tWas
   set the defaultStack to the short name of this stack
   if there is no field "uiStatus" then
      scSkip "a delayed handler writes the status line", "the chrome is not built"
      scFinish
      exit scTickProbe
   end if
   -- RESTORE THE COLOUR TOO, not just the text. uiStatus takes a KIND, and
   -- handing it back "ok" would repaint a "bad" line green - so a stack whose
   -- extension is missing would end this probe claiming to be fine.
   put field "uiStatus" into tPrev
   put the foregroundColor of field "uiStatus" into tPrevColor
   try
      uiStatus kScProbe, empty
      put field "uiStatus" into tGot
   catch tErr
      put "THREW:" && tErr into tGot
   end try
   uiStatus tPrev, empty
   set the foregroundColor of field "uiStatus" to tPrevColor
   scAssert "a delayed handler writes the status line " \
         & "(defaultStack on entry: " & scShow(tWas) & ")", (tGot is kScProbe)
   scFinish
end scTickProbe

command scFinish
   scLine "      " & sScPassed & " passed, " & sScFailed & " failed, " \
         & sScSkipped & " skipped - select this block and copy it into the " \
         & "OXT pass record"
   if sScFailed is 0 then
      scLine "      boot self-check GREEN (this is the single-machine claim; " \
            & "the live legs are the runbook's)"
      exit scFinish
   end if
   scLine "      boot self-check has FAILURES above - report them before " \
         & "running any live leg"
   -- ONLY ON FAILURE does this touch the status line, and the asymmetry is
   -- deliberate. Green, the demo's own "Ready ..." line is the more useful
   -- thing to be reading and the detail is in the log a few lines up. Red, the
   -- log may be on a tab nobody has clicked - so the one surface a person is
   -- certainly looking at has to say so. Fail loud, pass quiet.
   uiStatus "Boot self-check: " & sScFailed & " FAILED (see the log)", "bad"
end scFinish
-- ==== DEMO SELF-CHECK v1 END ====

-- Every control this script names, derived from the source by
-- tools/check-demo-control-lists.py. Never hand-edited.
constant kMdScControls = "mdAuto,mdBrokerPanel,mdClient,mdClientCap,mdConnPill,mdConnect,mdDisconnect,mdHost,mdHostCap,mdLog,mdLogCap,mdLogPanel,mdMessages,mdMsgCap,mdPass,mdPassCap,mdPort,mdPortCap,mdPubMsg,mdPubMsgCap,mdPubPanel,mdPubQoS,mdPubQoSCap,mdPubRetain,mdPubTopic,mdPubTopicCap,mdPublish,mdQoS,mdQoSCap,mdSecBroker,mdSecLog,mdSecPub,mdSecSub,mdSelfTest,mdStats,mdSubPanel,mdSubs,mdSubsCap,mdSubscribe,mdTLS,mdTopic,mdTopicCap,mdUnsubscribe,mdUser,mdUserCap,mdVerify,uiFooter,uiStatus,uiTitle"

-- THE BOOT SELF-CHECK. Nothing below connects, binds, or publishes: the
-- operator is about to press Connect for real, and a check that took the
-- socket would be indistinguishable from a broker that refused it.
command mdScRun
   local tMissing, tVer, tReport, tConns, tRc, tThresh
   scBegin "mdLog"

   put scMissing(kMdScControls) into tMissing
   scAssert "all 49 controls this script names exist (missing: " \
         & scShow(tMissing) & ")", (tMissing is empty)

   -- The library is EMBEDDED, so "is it installed?" is not the question -
   -- "did the embed carry a working copy, and did preOpenStack initialise
   -- it?" is. Both are answered here, because a stale or uninitialised embed
   -- is exactly what this demo's one-file convention risks.
   put mqttTestLibrary() into tVer
   scAssert "the embedded libMQTTxt answers (" & scShow(tVer) & ")", \
         (tVer contains "MQTT Library")

   -- Two questions the first version asked as one. On a fresh engine on
   -- 2026-09-06 the combined line failed, and it took the log's FIRST line to
   -- work out which half: preOpenStack had not initialised the library, and
   -- every earlier run had passed only because globals were inherited from a
   -- standalone-library session. Asked separately, a failure names its cause.
   scAssert "preOpenStack fired before openStack (marker: " \
         & scShow(sPreOpenRan) & ")", (sPreOpenRan is "true")

   put mqttGetKeepAliveThreshold() into tThresh
   scAssert "the library is initialised: keep-alive threshold set (" \
         & scShow(tThresh) & ")", \
         (tThresh is a number and tThresh > 0 and tThresh <= 1)

   -- The library's own pure-compute suite, run here so a stale embed cannot
   -- pass unnoticed. It touches no socket.
   put mqttSelfTest() into tReport
   scAssert "the embedded library's own self-test passes", \
         (tReport contains "All tests PASSED")

   put mqttGetConnections() into tConns
   scAssert "no connections at boot (" & scShow(tConns) & ")", (tConns is empty)
   scAssert "no connection key held at boot (" & scShow(sConnKey) & ")", \
         (sConnKey is empty)
   scAssert "not connected at boot", (sConnected is not "true")
   scAssert "the 1 Hz dashboard chain is armed", (sDashOn is "true")

   -- Three refusals, each proving a guard fires BEFORE anything is opened.
   -- Port 0 is invalid, so this returns without touching a socket.
   put mqttConnect("example.invalid", 0, "sc", empty, empty, 60, false, true, \
         empty, empty, 0, false, true, false, empty) into tRc
   scAssert "an invalid port is refused before any socket opens (" \
         & scShow(tRc) & ")", (tRc begins with "ERROR")

   -- A bare IPv6 literal makes the "host:port" key ambiguous, so it is
   -- refused rather than silently keyed wrong (OXT-ENGINE-NOTES 6.2).
   put mqttConnect("::1", 1883, "sc", empty, empty, 60, false, true, \
         empty, empty, 0, false, true, false, empty) into tRc
   scAssert "an unbracketed IPv6 host is refused (" & scShow(tRc) & ")", \
         (tRc begins with "ERROR")

   put mqttPublish("example.invalid", 1883, "t", "m", 0, false) into tRc
   scAssert "publishing while disconnected is refused (" & scShow(tRc) & ")", \
         (tRc begins with "ERROR")

   -- And nothing above left a socket behind.
   scAssert "no socket was opened by this check", \
         (mqttGetConnections() is empty)

   scArmProbe
end mdScRun
