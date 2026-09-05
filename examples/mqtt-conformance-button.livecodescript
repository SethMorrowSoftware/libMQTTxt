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
-- libmqttxt/ct/<milliseconds>/, unique per run, and the one retained message it
-- sets is cleared before the run ends.

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
local sCtBase, sCtHost, sCtPort
local sCtSeen            -- topic -> payload, filled by ctOnMessage
local sCtCount           -- topic -> how many times it arrived (QoS 2 dedup)
local sCtUtf8, sCtBin, sCtBig
local sCtHoldEnd, sCtKey2
local sCtPhase           -- sub-state within a multi-step stage

on mouseUp
   if sCtRunning is "true" then
      ctSay "-- run cancelled by a second click --"
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
   put empty into sCtCount
   put 1 into sCtStage
   put 0 into sCtTicks
   put false into sCtEntered
   put 0 into sCtPhase
   put empty into sCtKey2

   -- Unique per run, so a retained message left by an earlier run cannot be
   -- mistaken for this one's.
   put "libmqttxt/ct/" & the milliseconds into sCtBase

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

   --   big: ~200 KB. Large enough that its Remaining Length needs three varint
   --        bytes and that it cannot arrive in one socket read - which is what
   --        the streaming-read change is for.
   local tChunk
   put empty into tChunk
   repeat 64 times
      put "0123456789abcdef" after tChunk
   end repeat
   put empty into sCtBig
   repeat 200 times
      put tChunk after sCtBig
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

   send "ctTick" to me in kCtTickMs milliseconds
end ctBegin

-- Every message that arrives during the run lands here. Recorded by topic, with
-- a COUNT, because "exactly once" is a claim about the count and not only about
-- the payload.
on ctOnMessage pTopic, pMessage
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
   if sCtStage < 11 and not mqttIsConnected(sCtHost, sCtPort) then
      ctSay "ABORT  the connection dropped mid-run; nothing below was tested"
      add 1 to sCtFail
      ctFinish
      exit ctTick
   end if

   add 1 to sCtTicks

   switch sCtStage
      case 1
         ctStageSubscribe
         break
      case 2
         ctStageRoundTrip "q0", 0, "QoS 0 round trip"
         break
      case 3
         ctStageRoundTrip "q1", 1, "QoS 1 round trip (PUBACK)"
         break
      case 4
         ctStageRoundTrip "q2", 2, "QoS 2 round trip (PUBREC/PUBREL/PUBCOMP)"
         break
      case 5
         ctStageUtf8
         break
      case 6
         ctStageBinary
         break
      case 7
         ctStageBig
         break
      case 8
         ctStageRetained
         break
      case 9
         ctStageUnsubscribe
         break
      case 10
         ctStageSecondConnection
         break
      case 11
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

   put sCtSeen[tTopic] into tGot
   if tGot is not empty then
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

      -- At QoS > 0 the library holds the message until the broker acknowledges
      -- it. A pendingAcks entry still standing here means the ack leg did not
      -- complete, whatever the payload says.
      if pQoS > 0 then
         local tInfo
         put mqttGetConnectionInfo(sCtHost, sCtPort) into tInfo
         if tInfo["pendingAcks"] is empty then
            ctPass "  the QoS " & pQoS & " acknowledgment leg drained"
         else
            ctFail "  the QoS " & pQoS & " acknowledgment leg", \
                  "pendingAcks still holds an entry"
         end if
      end if
      ctNext
      exit ctStageRoundTrip
   end if
   ctDeadline pLabel & ": nothing came back", kCtDeadline
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

-- THE STREAMING-READ PROOF. 200 KB cannot arrive in one socket read, and its
-- Remaining Length needs three varint bytes. Under the old per-byte read this
-- was 200,000 trips through the engine's message queue.
command ctStageBig
   local tTopic, tRc, tGot
   put sCtBase & "/big" into tTopic

   if not sCtEntered then
      put mqttPublish(sCtHost, sCtPort, tTopic, sCtBig, 1, false) into tRc
      if tRc is not "OK" then
         -- A broker may cap message size; that is its policy, not a defect here.
         ctSkip "large payload", tRc
         ctNext
         exit ctStageBig
      end if
      put true into sCtEntered
      exit ctStageBig
   end if

   put sCtSeen[tTopic] into tGot
   if tGot is not empty then
      if (the number of bytes of tGot) is (the number of bytes of sCtBig) \
            and tGot is sCtBig then
         ctPass "a " & (the number of bytes of sCtBig) & "-byte payload " \
               & "reassembles exactly across many socket reads"
      else
         ctFail "large payload", "sent " & the number of bytes of sCtBig \
               & " bytes, got back " & the number of bytes of tGot
      end if
      ctNext
      exit ctStageBig
   end if
   ctDeadline "large payload: nothing came back", kCtDeadlineBig
end ctStageBig

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

   if mqttIsConnected(sCtHost, sCtPort) then
      ctPass "the connection survived " & kCtHoldSecs & "s idle - PINGREQ/" \
            & "PINGRESP kept it open and the keep-alive timer rescheduled"
   else
      ctFail "keep-alive", "the connection dropped during the idle hold, which " \
            & "is what a keep-alive that never fired looks like"
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

   -- Put the dashboard back the way it was found.
   mqttSetCallbackTarget the long id of this stack
   mqttSetMessageCallback "mdOnMessage"
   get mqttUnsubscribe(sCtHost, sCtPort, sCtBase & "/#")

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
