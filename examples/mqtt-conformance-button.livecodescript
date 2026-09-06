-- mqtt-conformance-button.livecodescript
--
-- PASTE THIS INTO A BUTTON on the mqtt-dashboard stack, then Connect and click
-- it. It drives a closed loop through your real broker and prints a record you
-- can paste into the pass log.
--
-- WHY A BUTTON AND NOT A HANDLER IN THE STACK SCRIPT: a button script can call
-- every handler in the stack (messages travel UP the path), which is all this
-- needs - but it cannot read the stack script's `local` variables, so this file
-- reads the broker from the FORM FIELDS and keeps all of its own state here.
-- Nothing in the dashboard needs editing.
--
-- WHY IT IS A STATE MACHINE AND NOT A LIST OF ASSERTIONS: MQTT is
-- asynchronous. A publish returns as soon as the bytes are written; the message
-- comes back through a socket callback some milliseconds later. A button that
-- published and then immediately checked would report FAIL on a working
-- library, every time. So each stage acts once, then polls for its result with
-- a deadline, and a stage that never completes is recorded as a TIMEOUT rather
-- than hanging the run.
--
-- IT SAYS WHEN IT IS NOT FINISHED, for the reason the suite's harness carries a
-- trailer: a report that simply stops reads exactly like a report that ended.
-- Every render carries "RUN NOT FINISHED" until the last stage closes it.
--
-- NOTHING HERE IS DESTRUCTIVE TO YOUR BROKER. Every topic lives under
-- libmqttxt/ct/<milliseconds>/, libmqttxt/ctquiet/<milliseconds>/ or
-- libmqttxt/ctprobe/<milliseconds>, unique per run, and the one retained
-- message it sets is cleared before the run ends. The ctquiet tree is
-- deliberately OUTSIDE the run's own subscription; see ctStageNoEcho for what
-- that is for. The ctprobe topic goes to a second broker entirely.

-- The second broker for the multi-connection stage. The library keys a
-- connection by "host:port", so TWO connections to one broker are impossible by
-- construction - a second connection needs a different host or port.
--
-- Against a single local mosquitto you can still run this: put "127.0.0.1"
-- here while the form says "localhost" (or the reverse). Same broker, two keys,
-- two independent keep-alive chains - which is exactly what the stage is for.
-- Leave it empty and the stage records SKIP with the reason.
constant kCtHost2 = ""
constant kCtPort2 = 1883

-- THE PACING LADDER, and it is the first thing the run does.
--
-- The ninth run settled WHERE the large-write stall lives. The same megabyte,
-- in the clear, stalled against a local mosquitto over a LAN (after 442368
-- bytes) AND against broker.hivemq.com over the internet (after 65536). Two
-- brokers, two paths, same failure - while the same engine carried a megabyte
-- to that same broker over TLS without a pause. The broker is not the variable
-- and the network is not the variable. Plaintext is.
--
-- WHAT THAT LEAVES, and it is the reading the numbers fit: the engine's
-- plaintext write does one non-blocking send and does not retry a socket that
-- is momentarily full. Chunking never addressed that, because chunks written
-- back to back at a yield of ZERO cost no elapsed time - the kernel's send
-- buffer is filled faster than the wire drains it, and the first chunk that
-- meets a full buffer times out. The byte counts are simply how much fitted
-- before that happened, which is why they move with the path (65536 over a
-- slow internet link, 442368 on a LAN) and between runs.
--
-- SO THE EXPERIMENT IS PACING, not size. The same megabyte, over and over, at
-- an increasing pause between chunks, on its own connection to a second
-- broker. The answer wanted is the smallest pause at which a megabyte gets
-- through.
--
-- IT TESTS THE SYNCHRONOUS PATH, AND IT HAS TO SAY SO. Since 2.13.0 the
-- library writes asynchronously by default and the pause is not consulted at
-- all on that path - so a ladder run against it measures nothing and every rung
-- is identical. The thirteenth run did exactly that and reported "no pacing
-- needed on this path" from a rung that never tested pacing. The stage now
-- turns asynchronous writes OFF for its own duration and puts the previous
-- settings back however it exits, which also makes it the only exercise the
-- synchronous fallback gets on an engine.
--
-- It runs FIRST because a stall later resets the connection and aborts the
-- rest, which is exactly when this answer is wanted. Set kCtAltHost to empty
-- to skip it.
--
-- THE PAUSE IT FINDS BELONGS TO THIS PATH, not to yours. A public broker over a
-- home uplink needs far more pacing than a broker on the same LAN, so a value
-- measured against hivemq is conservative for a local deployment. To measure
-- YOUR broker instead, point kCtAltHost at it by a second address the same way
-- kCtHost2 does - "127.0.0.1" while the form says "localhost", or its hostname
-- while the form has its IP. The library keys connections by "host:port", so a
-- different spelling is a different connection and the stage will not refuse it.
constant kCtAltHost = "broker.hivemq.com"
constant kCtAltPort = 1883
constant kCtAltSize = 1048576

-- Milliseconds of pause per 16 KB chunk to try, in order. 0 is the current
-- default and the one that has failed on every plaintext path so far; 50 gives
-- a megabyte 3.2 seconds of deliberate pacing, which is slow and would still be
-- an answer.
constant kCtYieldLadder = "0,5,20,50"

-- How long the idle hold runs. It must exceed the ping threshold (keep-alive x
-- mqttGetKeepAliveThreshold, so 45s at the dashboard's defaults) with room for
-- the PINGRESP, and stay under the broker's 1.5x cutoff at 90s. 100s covers a
-- full ping cycle plus the proof the link survived it.
constant kCtHoldSecs = 100

constant kCtTickMs = 250
constant kCtDeadline = 24        -- ticks per ordinary stage (6s)
constant kCtDeadlineBig = 80     -- ticks for the large-payload stage (20s)
constant kCtQuiet = 12           -- ticks a negative test waits to see nothing

local sCtRunning
local sCtStage, sCtTicks, sCtEntered
local sCtPass, sCtFail, sCtSkip
local sCtLog
local sCtBase, sCtQuiet, sCtHost, sCtPort
local sCtSeen            -- topic -> payload, filled by ctOnMessage
local sCtSeenAt          -- topic -> the milliseconds it ARRIVED, for real timings
local sCtCount           -- topic -> how many times it arrived (QoS 2 dedup)
local sCtUtf8, sCtBin, sCtBig
local sCtHoldEnd, sCtKey2
local sCtPhase           -- sub-state within a multi-step stage
local sCtBigStep, sCtBigOk   -- the large-payload ladder's position and high-water mark
local sCtWriteMs             -- how long the current rung's write call took
local sCtT0                  -- when the current rung was published, for the ack time
local sCtProbeStep           -- the pacing ladder's position in kCtYieldLadder
local sCtAsyncWas, sCtYieldWas  -- the write settings to put back after the ladder
local sCtEndedEarly          -- why the run stopped before its last stage, if it did

on mouseUp
   if sCtRunning is "true" then
      ctSay "-- run cancelled by a second click --"
      put "cancelled by a second click" into sCtEndedEarly
      ctFinish
      exit mouseUp
   end if
   ctBegin
end mouseUp

-- ---------------------------------------------------------------------------

command ctBegin
   set the defaultStack to the short name of this stack

   put mdTrim(field "mdHost") into sCtHost
   put mdTrim(field "mdPort") into sCtPort

   if not mqttIsConnected(sCtHost, sCtPort) then
      answer "Connect to a broker first."
      exit ctBegin
   end if

   put "true" into sCtRunning
   put 0 into sCtPass
   put 0 into sCtFail
   put 0 into sCtSkip
   put empty into sCtLog
   put empty into sCtSeen
   put empty into sCtSeenAt
   put empty into sCtCount
   put 1 into sCtStage
   put 0 into sCtTicks
   put false into sCtEntered
   put 0 into sCtPhase
   put 1 into sCtBigStep
   put 0 into sCtBigOk
   put 1 into sCtProbeStep
   -- The pacing ladder turns the write settings upside down to do its job.
   -- Remember them here so it can put them back whichever way it exits.
   put mqttGetAsyncWrites() into sCtAsyncWas
   put mqttGetWriteYieldMs() into sCtYieldWas
   put empty into sCtEndedEarly
   put empty into sCtKey2

   -- Unique per run, so a retained message left by an earlier run cannot be
   -- mistaken for this one's.
   local tStamp
   put the milliseconds into tStamp
   put "libmqttxt/ct/" & tStamp into sCtBase

   -- A SECOND TREE, deliberately outside the "<base>/#" this run subscribes to,
   -- so a publish there comes back to nobody. ctStageNoEcho is the reason.
   put "libmqttxt/ctquiet/" & tStamp into sCtQuiet

   -- The payloads whose ROUND TRIP is the actual proof. Built once here so a
   -- stage compares against the same bytes it published.
   --   utf8: 2-byte, 3-byte and 4-byte UTF-8 sequences. Their character count
   --         and byte count differ, which is precisely what the framing fix
   --         changed - a character count here produces a Remaining Length the
   --         broker disagrees with, and the stream desynchronises.
   put textEncode(numToCodepoint(233) & numToCodepoint(8364) \
         & numToCodepoint(128512) & "-end", "UTF-8") into sCtUtf8

   --   bin: every byte value, null included. "Binary safe" is a claim about
   --        all 256, not about the printable ones.
   local tI
   put empty into sCtBin
   repeat with tI = 0 to 255
      put numToByte(tI) after sCtBin
   end repeat

   -- Take over the message callback for the duration. The TARGET is this
   -- button: a dispatch to the button that this script does not handle still
   -- travels up to the card and the stack, so the dashboard's own log and
   -- status callbacks keep working throughout. Both are restored in ctFinish.
   mqttSetCallbackTarget the long id of me
   mqttSetMessageCallback "ctOnMessage"

   ctSay "== libMQTTxt conformance run =="
   ctSay "   broker " & sCtHost & ":" & sCtPort
   ctSay "   topics under " & sCtBase & "/"
   ctSay "   library " & mqttTestLibrary()
   -- The engine property that bounds a synchronous socket write. A large
   -- PUBLISH that exceeds it fails with "timeout" (engine notes 1.1), so the
   -- value belongs in the record beside any size that fails.
   ctSay "   socketTimeoutInterval " & the socketTimeoutInterval & "ms"
   ctSay "   write chunk size " & mqttGetWriteChunkSize() & " bytes"
   -- WHICH WRITE PATH IS RUNNING changes what the timings below MEAN. Under
   -- asynchronous writes a publish returns as soon as it is queued, so the
   -- "write call" is an enqueue and the honest measure of throughput is the
   -- time to the broker's acknowledgment. Both are printed.
   if mqttGetAsyncWrites() then
      ctSay "   writes ASYNCHRONOUS (queued; the ack time is the real measure)"
   else
      ctSay "   writes synchronous, paced " & mqttGetWriteYieldMs() \
            & "ms per chunk"
   end if

   send "ctTick" to me in kCtTickMs milliseconds
end ctBegin

-- Every message that arrives during the run lands here. Recorded by topic, with
-- a COUNT, because "exactly once" is a claim about the count and not only about
-- the payload.
-- THE ARRIVAL TIME IS STAMPED HERE, not where the stage notices it. A stage
-- polls every kCtTickMs, so "elapsed" measured at the poll is the true time
-- rounded UP to the next tick: the twelfth run reported a 4 KB round trip as
-- 262ms and 15 KB/s, when every one of its timings was within 15ms of a whole
-- number of 250ms ticks. Those were readings of the instrument, not of the
-- library. This callback runs the moment the message lands.
on ctOnMessage pTopic, pMessage
   put the milliseconds into sCtSeenAt[pTopic]
   put pMessage into sCtSeen[pTopic]
   add 1 to sCtCount[pTopic]
end ctOnMessage

-- ---------------------------------------------------------------------------

command ctTick
   -- PIN FIRST. This handler arrives from `send ... in`, where an unqualified
   -- control reference resolves against the defaultStack rather than the stack
   -- whose script is running (OXT-ENGINE-NOTES 5.3).
   set the defaultStack to the short name of this stack

   if sCtRunning is not "true" then
      exit ctTick
   end if

   -- A dropped link mid-run invalidates everything after it, so say so and stop
   -- rather than reporting a cascade of timeouts that all have one cause.
   if sCtStage < 13 and not mqttIsConnected(sCtHost, sCtPort) then
      ctSay "ABORT  the connection dropped mid-run; nothing below was tested"
      add 1 to sCtFail
      put "the connection dropped mid-run" into sCtEndedEarly
      ctFinish
      exit ctTick
   end if

   add 1 to sCtTicks

   switch sCtStage
      case 1
         ctStageAltWrite
         break
      case 2
         ctStageSubscribe
         break
      case 3
         ctStageRoundTrip "q0", 0, "QoS 0 round trip"
         break
      case 4
         ctStageRoundTrip "q1", 1, "QoS 1 round trip (PUBACK)"
         break
      case 5
         ctStageRoundTrip "q2", 2, "QoS 2 round trip (PUBREC/PUBREL/PUBCOMP)"
         break
      case 6
         ctStageUtf8
         break
      case 7
         ctStageBinary
         break
      case 8
         ctStageNoEcho
         break
      case 9
         ctStageBig
         break
      case 10
         ctStageRetained
         break
      case 11
         ctStageUnsubscribe
         break
      case 12
         ctStageSecondConnection
         break
      case 13
         ctStageHold
         break
      default
         ctFinish
         exit ctTick
   end switch

   if sCtRunning is "true" then
      send "ctTick" to me in kCtTickMs milliseconds
   end if
end ctTick

-- ---- stages ---------------------------------------------------------------

-- THE PACING LADDER. See kCtAltHost at the top for why this exists.
--
-- Each rung is a whole attempt on a FRESH connection to the second broker: set
-- the pause, connect, publish a megabyte, wait for its PUBACK, disconnect. A
-- rung that stalls moves to the next pause. A rung that succeeds ends the
-- stage AND LEAVES THAT PAUSE SET, so everything after it in the run - the
-- ladder included - runs with the pacing that worked. That makes the rest of
-- the run a second, independent test of the same answer.
--
-- Fresh connection per rung on purpose: a stalled write leaves a stream the
-- broker can no longer parse, so the library resets it. Retrying on the wreck
-- would measure the wreck.
--
-- A broker that cannot be reached is a SKIP: no internet is not a finding about
-- this library. A broker that accepts the connection and then stalls IS one.
command ctStageAltWrite
   local tRc, tInfo, tT0, tWriteMs, tYield, tRungs

   if kCtAltHost is empty then
      ctSkip "the pacing ladder", "no kCtAltHost set at the top of this script"
      ctNext
      exit ctStageAltWrite
   end if

   -- The library keys a connection by "host:port", so the probe cannot open a
   -- second one to the address the run is already using. That is a real
   -- constraint and not a policy, which is why the remedy is spelled out: any
   -- OTHER spelling of the same broker is a different key and works fine.
   if kCtAltHost is sCtHost and kCtAltPort is sCtPort then
      ctSkip "the pacing ladder", "kCtAltHost is " & sCtHost & ":" & sCtPort \
            & ", the same address this run is connected to, and one connection " \
            & "per host:port is all the library can hold - set kCtAltHost to " \
            & "ANOTHER SPELLING of the same broker (127.0.0.1 against a form " \
            & "saying localhost or its LAN IP, or the reverse) to pace this path"
      ctNext
      exit ctStageAltWrite
   end if

   put the number of items of kCtYieldLadder into tRungs
   if sCtProbeStep > tRungs then
      ctFail "the pacing ladder", kCtAltSize & " bytes never went out at any " \
            & "pause in " & kCtYieldLadder & "ms with SYNCHRONOUS writes - " \
            & "pacing is not the answer for this path, or not the whole answer"
      ctProbeRestore
      ctNext
      exit ctStageAltWrite
   end if
   put item sCtProbeStep of kCtYieldLadder into tYield

   switch sCtPhase
      case 0
         -- The ladder measures the SYNCHRONOUS path. Asynchronous writes never
         -- read the pause, so leaving them on would make every rung identical.
         mqttSetAsyncWrites false
         mqttSetWriteYieldMs tYield
         put mqttConnect(kCtAltHost, kCtAltPort, \
               "libmqttxt-ctprobe-" & the milliseconds, empty, empty, 60, \
               false, true, empty, empty, 0, false, true, false, empty) into tRc
         if tRc is not "OK" then
            ctSkip "the pacing ladder", tRc
            ctProbeRestore
            ctNext
            exit ctStageAltWrite
         end if
         put 1 into sCtPhase
         put 0 into sCtTicks
         break

      case 1
         if mqttIsConnected(kCtAltHost, kCtAltPort) then
            put 2 into sCtPhase
            put 0 into sCtTicks
            exit ctStageAltWrite
         end if
         if sCtTicks >= kCtDeadline then
            ctSkip "the pacing ladder", kCtAltHost & " never reached CONNACK"
            mqttDisconnect kCtAltHost, kCtAltPort
            ctProbeRestore
            ctNext
         end if
         break

      case 2
         put ctPayload(kCtAltSize) into sCtBig
         put the milliseconds into tT0
         put tT0 into sCtT0
         put mqttPublish(kCtAltHost, kCtAltPort, \
               "libmqttxt/ctprobe/" & the milliseconds, sCtBig, 1, false) into tRc
         put the milliseconds - tT0 into tWriteMs
         put tWriteMs into sCtWriteMs

         if tRc is not "OK" then
            ctSay "      synchronous, pause " & tYield & "ms per chunk: " & tRc \
                  & " (after " & tWriteMs & "ms)"
            mqttDisconnect kCtAltHost, kCtAltPort
            add 1 to sCtProbeStep
            put 0 into sCtPhase
            put 0 into sCtTicks
            exit ctStageAltWrite
         end if
         put 3 into sCtPhase
         put 0 into sCtTicks
         break

      case 3
         put mqttGetConnectionInfo(kCtAltHost, kCtAltPort) into tInfo
         if tInfo["pendingAcks"] is empty then
            ctPass kCtAltSize & " bytes in the clear to " & kCtAltHost \
                  & ", SYNCHRONOUS writes at a " & tYield & "ms pause per " \
                  & "chunk, PUBACKed within " & (the milliseconds - sCtT0) \
                  & "ms (polled every " & kCtTickMs & "ms, so an upper bound; " \
                  & "the write call itself took " & sCtWriteMs & "ms)"
            if tYield is 0 then
               ctSay "      NOTE: the SYNCHRONOUS path needed no pacing at all " \
                     & "here - a megabyte of plaintext went straight through " \
                     & "with mqttSetWriteYieldMs 0. That is the opposite of " \
                     & "what engine notes 1.1 saw on Kubuntu, so record the " \
                     & "platform beside this line."
            else
               ctSay "      NOTE: " & tYield & "ms per chunk is what the " \
                     & "SYNCHRONOUS path needs on this link. Set it with " \
                     & "mqttSetWriteYieldMs if you turn asynchronous writes off."
            end if
            ctProbeRestore
            mqttDisconnect kCtAltHost, kCtAltPort
            ctNext
            exit ctStageAltWrite
         end if
         if sCtTicks >= kCtDeadlineBig then
            ctSay "      pause " & tYield & "ms per chunk: written, but no " \
                  & "PUBACK within " & (kCtDeadlineBig * kCtTickMs) & "ms"
            mqttDisconnect kCtAltHost, kCtAltPort
            add 1 to sCtProbeStep
            put 0 into sCtPhase
            put 0 into sCtTicks
         end if
         break
   end switch
end ctStageAltWrite

command ctStageSubscribe
   local tRc
   if not sCtEntered then
      put mqttSubscribe(sCtHost, sCtPort, sCtBase & "/#", 2) into tRc
      if tRc is not "OK" then
         ctFail "subscribe to " & sCtBase & "/# ", tRc
         ctFinish
         exit ctStageSubscribe
      end if
      put true into sCtEntered
      exit ctStageSubscribe
   end if

   -- SUBACK is what makes the subscription real; publishing before it arrives
   -- is a race the broker is allowed to lose.
   --
   -- ASKED OF pendingSubAcks, NOT OF THE SUBSCRIPTION TABLE. mqttSubscribe adds
   -- the filter to that table the moment it WRITES the packet, so checking it
   -- here would pass on the first tick whether or not the broker ever answered
   -- - a false pass in the first assertion of the run. __parseSubAck is what
   -- clears the pending entry, so its absence is the acknowledgment.
   local tInfo
   put mqttGetConnectionInfo(sCtHost, sCtPort) into tInfo
   if tInfo["pendingSubAcks"] is empty then
      ctPass "SUBSCRIBE acknowledged by the broker for " & sCtBase & "/#"
      ctNext
      exit ctStageSubscribe
   end if
   ctDeadline "SUBACK never arrived", kCtDeadline
end ctStageSubscribe

-- The shape every round-trip stage shares: publish once, then wait for the same
-- bytes to come back through the broker.
command ctStageRoundTrip pLeaf, pQoS, pLabel
   local tTopic, tRc, tGot
   put sCtBase & "/" & pLeaf into tTopic

   if not sCtEntered then
      put mqttPublish(sCtHost, sCtPort, tTopic, "payload-" & pLeaf, pQoS, false) \
            into tRc
      if tRc is not "OK" then
         ctFail pLabel, tRc
         ctNext
         exit ctStageRoundTrip
      end if
      put true into sCtEntered
      exit ctStageRoundTrip
   end if

   -- phase 1: wait for the echo
   if sCtPhase < 2 then
      put sCtSeen[tTopic] into tGot
      if tGot is empty then
         ctDeadline pLabel & ": nothing came back", kCtDeadline
         exit ctStageRoundTrip
      end if

      if tGot is ("payload-" & pLeaf) then
         ctPass pLabel & " (delivered " & sCtCount[tTopic] & "x)"
      else
         ctFail pLabel, "payload came back as " & tGot
      end if

      -- Only QoS 2 promises EXACTLY once. QoS 0 and 1 are allowed to
      -- duplicate, so asserting a count of 1 for them would be asserting
      -- something the protocol does not say.
      if pQoS is 2 then
         if sCtCount[tTopic] is 1 then
            ctPass "  QoS 2 delivered it exactly once"
         else
            ctFail "  QoS 2 exactly-once", "delivered " & sCtCount[tTopic] \
                  & " times"
         end if
      end if

      if pQoS is 0 then
         ctNext
         exit ctStageRoundTrip
      end if

      -- QoS > 0: the acknowledgment leg is a SEPARATE wait, below.
      put 2 into sCtPhase
      put 0 into sCtTicks
      exit ctStageRoundTrip
   end if

   -- phase 2: wait for the outbound acknowledgment leg to drain.
   --
   -- THE ECHO ARRIVING DOES NOT MEAN OUR HANDSHAKE IS DONE, and the first
   -- version of this stage assumed it did. At QoS 2 there are TWO exchanges in
   -- flight with independent packet IDs: ours (PUBLISH -> PUBREC -> PUBREL ->
   -- PUBCOMP, which clears pendingAcks) and the broker's delivery back to us
   -- (PUBLISH -> PUBREC -> PUBREL, which hands over the message). The echo is
   -- delivered on the broker's PUBREL, and on a real run (2026-09-05, hivemq)
   -- our PUBCOMP arrived one line AFTER the check - so a correct library was
   -- reported as leaving an ack outstanding. Observed, and the reason this is
   -- a poll with a deadline rather than an assertion.
   local tInfo
   put mqttGetConnectionInfo(sCtHost, sCtPort) into tInfo
   if tInfo["pendingAcks"] is empty then
      ctPass "  the QoS " & pQoS & " acknowledgment leg drained"
      ctNext
      exit ctStageRoundTrip
   end if
   ctDeadline "  the QoS " & pQoS & " acknowledgment leg: pendingAcks still " \
         & "holds an entry", kCtDeadline
end ctStageRoundTrip

-- THE FRAMING PROOF. A payload whose byte count and character count differ is
-- the case the old code got wrong: it declared a Remaining Length counted in
-- characters, sent more bytes than it announced, and every packet after it was
-- misread. Compared byte-for-byte, not by eye.
command ctStageUtf8
   local tTopic, tRc, tGot
   put sCtBase & "/utf8" into tTopic

   if not sCtEntered then
      put mqttPublish(sCtHost, sCtPort, tTopic, sCtUtf8, 1, false) into tRc
      if tRc is not "OK" then
         ctFail "multi-byte payload", tRc
         ctNext
         exit ctStageUtf8
      end if
      put true into sCtEntered
      exit ctStageUtf8
   end if

   put sCtSeen[tTopic] into tGot
   if tGot is not empty then
      if (the number of bytes of tGot) is (the number of bytes of sCtUtf8) \
            and tGot is sCtUtf8 then
         ctPass "multi-byte UTF-8 payload survives byte-for-byte (" \
               & the number of bytes of sCtUtf8 & " bytes, " \
               & the number of chars of textDecode(sCtUtf8, "UTF-8") \
               & " characters)"
      else
         ctFail "multi-byte UTF-8 payload", "sent " \
               & the number of bytes of sCtUtf8 & " bytes, got back " \
               & the number of bytes of tGot
      end if
      ctNext
      exit ctStageUtf8
   end if
   ctDeadline "multi-byte payload: nothing came back", kCtDeadline
end ctStageUtf8

command ctStageBinary
   local tTopic, tRc, tGot
   put sCtBase & "/bin" into tTopic

   if not sCtEntered then
      put mqttPublish(sCtHost, sCtPort, tTopic, sCtBin, 1, false) into tRc
      if tRc is not "OK" then
         ctFail "binary payload", tRc
         ctNext
         exit ctStageBinary
      end if
      put true into sCtEntered
      exit ctStageBinary
   end if

   put sCtSeen[tTopic] into tGot
   if tGot is not empty then
      if (the number of bytes of tGot) is 256 and tGot is sCtBin then
         ctPass "all 256 byte values survive intact (null and high bits included)"
      else
         ctFail "binary payload", "sent 256 bytes, got back " \
               & the number of bytes of tGot
      end if
      ctNext
      exit ctStageBinary
   end if
   ctDeadline "binary payload: nothing came back", kCtDeadline
end ctStageBinary

-- THE DISCRIMINATING EXPERIMENT, and it runs BEFORE the ladder because the
-- ladder can take the connection down with it.
--
-- Seven engine runs (docs/ENGINE-NOTES.md 1.1) have shown a large PUBLISH
-- stalling on the mosquitto LAN path: with 16 KB chunking, six chunks went out
-- and the seventh blocked for the whole socketTimeoutInterval. A 16 KB write
-- that works six times and then stops is not a write that was too big; the
-- socket became unwritable and stayed unwritable, which means the peer stopped
-- reading.
--
-- The candidate explanation is that WE stopped reading first. Every other big
-- publish in this run goes to a topic this run is subscribed to, so the broker
-- is pushing the echo back at us while we are inside a synchronous write that
-- never returns to the event loop to read it. Our receive buffer fills, the
-- broker's writes to us block, and a broker that cannot flush to us stops
-- draining what we send.
--
-- So: the same size, to a topic NOBODY is subscribed to. Nothing to echo.
--   this passes, the ladder fails -> the echo is the cause
--   this passes, the ladder passes -> the cause was the echo and the yield in
--                                     __writeSocket 2.12.6 fixes it
--   this fails                     -> the echo is exonerated; the write path
--                                     itself is the suspect and async is next
--
-- THE PUBACK IS THE PROOF. Nothing comes back on this topic, so a round trip
-- cannot be the criterion. A QoS 1 PUBACK is the broker saying it parsed a
-- complete PUBLISH of that length - the same evidence, one step earlier.
command ctStageNoEcho
   local tRc, tInfo, tT0, tWriteMs, tSize, tLabel
   put 1048576 into tSize
   put tSize & " bytes to a topic nothing echoes back" into tLabel

   if not sCtEntered then
      put ctPayload(tSize) into sCtBig

      put the milliseconds into tT0
      put tT0 into sCtT0
      put mqttPublish(sCtHost, sCtPort, sCtQuiet & "/big", sCtBig, 1, false) \
            into tRc
      put the milliseconds - tT0 into tWriteMs
      put tWriteMs into sCtWriteMs

      if tRc is not "OK" then
         ctFail tLabel, tRc & " (the write call took " & tWriteMs & "ms)"
         ctSay "      NOTE: this publish had NO echo to contend with, so the " \
               & "stall in engine notes 1.1 is NOT the broker blocking on an " \
               & "echo we were not reading. The write path itself is the suspect."
         ctNext
         exit ctStageNoEcho
      end if
      put true into sCtEntered
      exit ctStageNoEcho
   end if

   put mqttGetConnectionInfo(sCtHost, sCtPort) into tInfo
   if tInfo["pendingAcks"] is empty then
      -- NO RATE HERE, and the reason is worth stating. Nothing calls back when
      -- a PUBACK lands, so this stage can only notice it at the next poll:
      -- the figure is the true time rounded up to kCtTickMs, and dividing a
      -- payload by it would report the poll interval dressed up as throughput.
      -- The echo-based ladder below has a real arrival stamp; this one says
      -- what it actually knows.
      ctPass tLabel & " was PUBACKed within " & (the milliseconds - sCtT0) \
            & "ms (polled every " & kCtTickMs & "ms, so an upper bound; the " \
            & "write call itself took " & sCtWriteMs & "ms)"
      ctNext
      exit ctStageNoEcho
   end if
   ctDeadline "no PUBACK for the un-echoed " & tSize & "-byte publish", \
         kCtDeadlineBig
end ctStageNoEcho

-- Put the write settings back the way the run found them. Called from EVERY
-- exit of the pacing ladder, because a stage that leaves the library in its
-- test configuration has changed what every stage after it is measuring - and
-- the ladder deliberately turns off the write path the rest of the run uses.
command ctProbeRestore
   mqttSetAsyncWrites sCtAsyncWas
   mqttSetWriteYieldMs sCtYieldWas
end ctProbeRestore

-- One filler payload, built the same way wherever a size is needed. 1 KB of
-- hex digits repeated: cheap to build, and every byte differs from its
-- neighbours enough that a truncated or duplicated block shows up in a
-- byte-for-byte compare.
function ctPayload pSize
   local tChunk, tPayload
   put empty into tChunk
   repeat 64 times
      put "0123456789abcdef" after tChunk
   end repeat
   put empty into tPayload
   repeat (pSize div 1024) times
      put tChunk after tPayload
   end repeat
   return tPayload
end ctPayload

-- THE STREAMING-READ PROOF, AS A LADDER RATHER THAN ONE SHOT.
--
-- The first version published 200 KB once and reported TIMEOUT, which says the
-- payload did not come back and nothing about WHERE the limit is. A ladder
-- answers that: it reports the largest size that completed a round trip, so a
-- failure names a threshold instead of a symptom.
--
-- It also stops at the first size that fails. Past that point the broker may be
-- holding a partial packet, and every later result would be a consequence of
-- this one rather than a measurement of its own.
command ctStageBig
   local tTopic, tRc, tGot, tSize

   put ctBigSize(sCtBigStep) into tSize
   if tSize is 0 then
      -- The PASS states what was seen and nothing about why. An earlier version
      -- added "so the chunked write is doing its job" at 1 MB, and the first
      -- 1 MB pass then came over TLS to hivemq - a path that had carried 200 KB
      -- BEFORE chunking existed. Whether the chunking is what made 1 MB possible
      -- is a question for the mosquitto LAN path where a single write failed,
      -- and for docs/ENGINE-NOTES.md 1.1, not for a line in a test log.
      ctPass "large payloads round-trip intact up to " & sCtBigOk & " bytes (" \
            & "writes chunked at " & mqttGetWriteChunkSize() & " bytes)"
      ctNext
      exit ctStageBig
   end if

   put sCtBase & "/big" & tSize into tTopic

   if not sCtEntered then
      put ctPayload(tSize) into sCtBig
      put the milliseconds into sCtT0

      -- TIME THE WRITE. This is the measurement that separates the two
      -- explanations for a size that fails: if the write time grows roughly
      -- in step with the size and the failing rung hits the socketTimeoutInterval,
      -- the engine's synchronous write is throughput-bound and a longer
      -- interval would let it finish; if the smaller rungs are instant and one
      -- rung fails outright, something is refusing the size, and no timeout
      -- setting will help.
      local tT0, tWriteMs
      put the milliseconds into tT0
      put mqttPublish(sCtHost, sCtPort, tTopic, sCtBig, 1, false) into tRc
      put the milliseconds - tT0 into tWriteMs
      put tWriteMs into sCtWriteMs

      if tRc is not "OK" then
         -- A refusal is an ANSWER, not a timeout: the library or the broker
         -- said no, and said why. The write time says WHICH.
         ctFail "large payload at " & tSize & " bytes", tRc \
               & " (the write call took " & tWriteMs & "ms; socketTimeoutInterval " \
               & "is " & the socketTimeoutInterval & "ms)"
         ctNext
         exit ctStageBig
      end if
      put true into sCtEntered
      exit ctStageBig
   end if

   put sCtSeen[tTopic] into tGot
   if tGot is not empty then
      if (the number of bytes of tGot) is tSize and tGot is sCtBig then
         put tSize into sCtBigOk
         add 1 to sCtBigStep
         put 0 into sCtTicks
         put false into sCtEntered
         -- THE ROUND TRIP is the number that means the same thing on both
         -- write paths: published, carried by the broker, and back. Taken from
         -- the arrival stamp, not from this poll, so it is a millisecond
         -- measurement rather than a multiple of kCtTickMs. The write call is
         -- printed beside it because under asynchronous writes it is an
         -- enqueue, and the gap between the two is the point.
         local tRtt
         put sCtSeenAt[tTopic] - sCtT0 into tRtt
         -- The rate INCLUDES one round trip, so a small rung measures latency
         -- and only a large one approaches throughput: 4 KB at "38 KB/s" and
         -- 1 MB at "1962 KB/s" on the same link, in the thirteenth run, are
         -- both honest and they are not the same quantity.
         ctSay "      " & tSize & " bytes round-tripped intact in " & tRtt \
               & "ms (" & ctRate(tSize, tRtt) & " incl. one round trip" \
               & "; write call " & sCtWriteMs & "ms)"
      else
         ctFail "large payload at " & tSize & " bytes", "got back " \
               & the number of bytes of tGot & " bytes"
         ctNext
      end if
      exit ctStageBig
   end if

   -- The deadline scales with the rung: 20s is generous for 200 KB and thin
   -- for 1 MB over an internet path.
   if sCtTicks >= (kCtDeadlineBig * max(1, tSize div 204800)) then
      if sCtBigOk > 0 then
         ctFail "large payload", tSize & " bytes never came back, though " \
               & sCtBigOk & " bytes did - the ceiling is between them"
      else
         ctFail "large payload", tSize & " bytes never came back"
      end if
      ctNext
   end if
end ctStageBig

-- KB/s for the record, with a floor of 1ms so an instant write does not divide
-- by zero and read as infinite.
--
-- ONLY CALL THIS ON A TIME THAT WAS ACTUALLY MEASURED. A duration taken at a
-- poll is quantised to kCtTickMs, and a rate computed from one says more about
-- the poll interval than about the link - which is exactly what the twelfth
-- run's "15 KB/s" for a 4 KB payload was.
function ctRate pBytes, pMs
   if pMs < 1 then
      put 1 into pMs
   end if
   return (round((pBytes / 1024) / (pMs / 1000))) & " KB/s"
end ctRate

-- The ladder. Doubling from 4 KB: 64 KB is the first size past a typical
-- socket buffer, and 128 KB is the first whose Remaining Length needs three
-- varint bytes.
function ctBigSize pStep
   -- Doubling from 4 KB. 64 KB is the first size past a typical socket
   -- buffer; 128 KB is the first whose Remaining Length needs three varint
   -- bytes; 131072 and 204800 are where runs 3 and 4 failed with a single
   -- write on the mosquitto LAN path. The last two rungs exist for the chunked
   -- write: 512 KB and 1 MB are past any cold kernel send buffer. On the path
   -- where the single write failed, a 1 MB round trip is the experiment that
   -- tests the mechanism in docs/ENGINE-NOTES.md 1.1; on a path that carried
   -- 200 KB before chunking (hivemq, run 2) it shows the ceiling is higher, and
   -- no more than that.
   switch pStep
      case 1
         return 4096
      case 2
         return 16384
      case 3
         return 65536
      case 4
         return 131072
      case 5
         return 204800
      case 6
         return 524288
      case 7
         return 1048576
   end switch
   return 0
end ctBigSize

-- Retained: publish with the flag, then resubscribe and expect the broker to
-- replay it to the new subscription. Cleared before the run ends.
-- THREE PHASES, and the middle one is not optional. Publishing a retained
-- message also delivers it LIVE to the subscription already standing. If this
-- stage forgot the topic and resubscribed in one step, that live delivery could
-- land after the forget and be counted as the replay - a pass that proves
-- nothing about retention. So: publish, WAIT for the live copy, only then
-- forget it and resubscribe, and what arrives after that can only be the
-- broker's stored copy.
command ctStageRetained
   local tTopic, tRc, tGot
   put sCtBase & "/ret" into tTopic

   -- phase 0: publish it retained
   if not sCtEntered then
      put mqttPublish(sCtHost, sCtPort, tTopic, "retained-value", 1, true) into tRc
      if tRc is not "OK" then
         ctFail "retained publish", tRc
         ctNext
         exit ctStageRetained
      end if
      put 1 into sCtPhase
      put true into sCtEntered
      exit ctStageRetained
   end if

   -- phase 1: wait for the LIVE delivery, so it cannot be mistaken for the replay
   if sCtPhase is 1 then
      if sCtSeen[tTopic] is not empty then
         delete variable sCtSeen[tTopic]
         delete variable sCtCount[tTopic]
         get mqttUnsubscribe(sCtHost, sCtPort, sCtBase & "/#")
         get mqttSubscribe(sCtHost, sCtPort, sCtBase & "/#", 1)
         put 2 into sCtPhase
         put 0 into sCtTicks
         exit ctStageRetained
      end if
      ctDeadline "the retained publish was never delivered live", kCtDeadline
      exit ctStageRetained
   end if

   -- phase 2: whatever arrives now is the stored copy
   put sCtSeen[tTopic] into tGot
   if tGot is not empty then
      if tGot is "retained-value" then
         ctPass "a retained message is replayed to a new subscription"
      else
         ctFail "retained message", "came back as " & tGot
      end if
      -- Clear it: an empty retained payload is how MQTT deletes one, and this
      -- run must not leave state on somebody's broker.
      get mqttPublish(sCtHost, sCtPort, tTopic, empty, 1, true)
      ctNext
      exit ctStageRetained
   end if
   ctDeadline "retained message was not replayed", kCtDeadline
end ctStageRetained

-- A NEGATIVE TEST, and the only one here: after unsubscribing, silence is the
-- pass condition. It also exercises the subscription bookkeeping whose key used
-- to be stored in UTF-8 bytes while the table was keyed by text.
command ctStageUnsubscribe
   local tTopic, tRc
   put sCtBase & "/after" into tTopic

   if not sCtEntered then
      put mqttUnsubscribe(sCtHost, sCtPort, sCtBase & "/#") into tRc
      if tRc is not "OK" then
         ctFail "unsubscribe", tRc
         ctNext
         exit ctStageUnsubscribe
      end if
      if mqttGetSubscriptions(sCtHost, sCtPort) contains (sCtBase & "/#") then
         ctFail "unsubscribe", "the subscription table still lists the filter"
      else
         ctPass "the subscription is removed from the table"
      end if
      get mqttPublish(sCtHost, sCtPort, tTopic, "should-not-arrive", 1, false)
      put true into sCtEntered
      exit ctStageUnsubscribe
   end if

   if sCtSeen[tTopic] is not empty then
      ctFail "unsubscribe", "a message arrived after unsubscribing"
      ctNext
      exit ctStageUnsubscribe
   end if

   if sCtTicks >= kCtQuiet then
      ctPass "nothing is delivered after unsubscribing (waited " \
            & (kCtQuiet * kCtTickMs) & "ms)"
      ctNext
   end if
end ctStageUnsubscribe

-- The library keys a connection by "host:port", so this needs a second address.
-- Two live connections is what makes the hold below a test of TIMER ROUTING and
-- not just of one keep-alive: before the token fix, whichever timer fired first
-- serviced BOTH connections, and the other was pinged on a schedule that was
-- not its own.
command ctStageSecondConnection
   local tRc
   if kCtHost2 is empty then
      ctSkip "a second simultaneous connection", \
            "no kCtHost2 set at the top of this script"
      ctNext
      exit ctStageSecondConnection
   end if

   if not sCtEntered then
      put kCtHost2 & ":" & kCtPort2 into sCtKey2
      -- A DIFFERENT keep-alive from the dashboard's 60s, on purpose: if the two
      -- chains were still sharing a timer, they could not both be right.
      put mqttConnect(kCtHost2, kCtPort2, "libmqttxt-ct2-" & the milliseconds, \
            empty, empty, 30, false, true, empty, empty, 0, false, true, \
            false, empty) into tRc
      if tRc is not "OK" then
         ctSkip "a second simultaneous connection", tRc
         put empty into sCtKey2
         ctNext
         exit ctStageSecondConnection
      end if
      put true into sCtEntered
      exit ctStageSecondConnection
   end if

   if mqttIsConnected(kCtHost2, kCtPort2) then
      ctPass "a second connection is live alongside the first (keep-alive 30s " \
            & "against the dashboard's 60s)"
      ctNext
      exit ctStageSecondConnection
   end if
   ctDeadline "the second connection never reached CONNACK", kCtDeadline
end ctStageSecondConnection

-- THE KEEP-ALIVE PROOF, and it can only be made by waiting. The library pings
-- at 75% of the keep-alive interval and the broker drops a silent client at
-- 150%; holding past both, with no traffic at all, is the only thing that shows
-- the PINGREQ went out, the PINGRESP came back, and the timer rescheduled.
command ctStageHold
   local tLeft
   if not sCtEntered then
      put the seconds + kCtHoldSecs into sCtHoldEnd
      ctSay "   holding idle for " & kCtHoldSecs & "s to exercise keep-alive; " \
            & "nothing is published during this stage"
      put true into sCtEntered
      exit ctStageHold
   end if

   put sCtHoldEnd - the seconds into tLeft
   if tLeft > 0 then
      uiStatus "Conformance run: idle hold, " & tLeft & "s left...", empty
      exit ctStageHold
   end if

   -- STILL CONNECTED IS NOT ENOUGH, and the first version of this stage claimed
   -- it was. The library pings at 45s idle and only gives up 90s after the
   -- PINGREQ - so across a 100s hold a connection whose PINGRESP never came
   -- back is still marked connected, and the assertion passed on a link that
   -- was dead in one direction. lastPingTime is the observable that separates
   -- them: __parsePingResp zeroes it, so a value still standing means a ping
   -- went out and nothing came back.
   local tInfo
   put mqttGetConnectionInfo(sCtHost, sCtPort) into tInfo

   if not mqttIsConnected(sCtHost, sCtPort) then
      ctFail "keep-alive", "the connection dropped during the idle hold, which " \
            & "is what a keep-alive that never fired looks like"
   else if tInfo["lastPingTime"] is not 0 then
      ctFail "keep-alive", "a PINGREQ went out and no PINGRESP came back " \
            & "(lastPingTime still " & tInfo["lastPingTime"] & ") - the link " \
            & "is dead inbound and only looks connected"
   else
      ctPass "the connection survived " & kCtHoldSecs & "s idle AND a PINGRESP " \
            & "was received and processed - keep-alive and its timer both work"
   end if

   if sCtKey2 is not empty then
      if mqttIsConnected(kCtHost2, kCtPort2) then
         ctPass "the SECOND connection also survived - the two keep-alive " \
               & "chains ran on their own schedules"
      else
         ctFail "multi-connection keep-alive", "the second connection dropped " \
               & "while the first survived, which is one timer serving both"
      end if
      mqttDisconnect kCtHost2, kCtPort2
   end if

   ctNext
end ctStageHold

-- ---- bookkeeping ----------------------------------------------------------

command ctNext
   add 1 to sCtStage
   put 0 into sCtTicks
   put false into sCtEntered
   put 0 into sCtPhase
end ctNext

command ctDeadline pWhy, pLimit
   if sCtTicks < pLimit then
      exit ctDeadline
   end if
   ctFail "TIMEOUT", pWhy & " within " & (pLimit * kCtTickMs) & "ms"
   ctNext
end ctDeadline

command ctPass pWhat
   add 1 to sCtPass
   ctSay "PASS  " & pWhat
end ctPass

command ctFail pWhat, pWhy
   add 1 to sCtFail
   ctSay "FAIL  " & pWhat & " - " & pWhy

   -- A BROKEN RECEIVE PATH POISONS EVERY STAGE AFTER IT, and the run that
   -- found this printed two PASSes it had not earned: after a large PUBLISH
   -- left the broker mid-packet, nothing inbound was processed again - so
   -- "nothing is delivered after unsubscribing" passed because nothing was
   -- being delivered at all. A report whose later lines cannot be trusted has
   -- to say so, or its green outnumbers its red and reads as a pass.
   if sCtFail is 1 then
      ctSay "      NOTE: the first failure is above. Inbound packets may have " \
            & "stopped there, so every result below it - including any PASS - " \
            & "needs the earlier failure fixed before it means anything."
   end if
end ctFail

command ctSkip pWhat, pWhy
   add 1 to sCtSkip
   ctSay "SKIP  " & pWhat & " (" & pWhy & ")"
end ctSkip

command ctSay pText
   set the defaultStack to the short name of this stack
   put pText & return after sCtLog
   if there is a field "mdLog" then
      put pText & return after field "mdLog"
      set the vscroll of field "mdLog" to the formattedHeight of field "mdLog"
   end if
end ctSay

command ctFinish
   put false into sCtRunning

   -- The pacing ladder turns asynchronous writes OFF to do its work, and a run
   -- cancelled or aborted mid-ladder would otherwise leave them off for the
   -- rest of the engine session - every later stage, and the dashboard itself,
   -- silently on the other write path. Restoring here covers every exit;
   -- the ladder's own restore covers the ordinary ones.
   ctProbeRestore

   -- Put the dashboard back the way it was found. The unsubscribe is
   -- conditional because a green run has already unsubscribed in its own
   -- stage; sending a second UNSUBSCRIBE for a filter the table no longer
   -- lists is legal MQTT, and it also printed an "Unsubscribe confirmed"
   -- AFTER the RUN FINISHED line, which read as a stage that had run late.
   mqttSetCallbackTarget the long id of this stack
   mqttSetMessageCallback "mdOnMessage"
   if mqttGetSubscriptions(sCtHost, sCtPort) contains (sCtBase & "/#") then
      get mqttUnsubscribe(sCtHost, sCtPort, sCtBase & "/#")
   end if

   -- A RUN THAT STOPPED EARLY IS NOT A RUN THAT PASSED, and this printed
   -- "RUN FINISHED ... GREEN" on a run cancelled mid-hold until the eleventh
   -- engine session showed it doing exactly that. Zero failures is not a pass
   -- when the stages that would have failed never ran - which is the same
   -- lesson as the cascade NOTE in ctFail, one level up. The header of this
   -- file promises a report that says when it is not finished; this is where
   -- that promise was being broken.
   if sCtEndedEarly is not empty then
      ctSay "      " & sCtPass & " passed, " & sCtFail & " failed, " \
            & sCtSkip & " skipped - RUN NOT FINISHED (" & sCtEndedEarly & ")"
      ctSay "      stage " & sCtStage & " of 13 was in progress and the stages " \
            & "after it never ran, so this is NOT a pass whatever the count says"
      uiStatus "Conformance run STOPPED at stage " & sCtStage & " of 13 - " \
            & "incomplete.", "warn"
      exit ctFinish
   end if

   ctSay "      " & sCtPass & " passed, " & sCtFail & " failed, " \
         & sCtSkip & " skipped - RUN FINISHED"

   if sCtFail is 0 then
      ctSay "      conformance run GREEN against " & sCtHost & ":" & sCtPort
      uiStatus "Conformance run: " & sCtPass & " passed, 0 failed.", "ok"
   else
      ctSay "      conformance run has FAILURES above"
      uiStatus "Conformance run: " & sCtFail & " FAILED (see the log).", "bad"
   end if
end ctFinish
