/*
 * Copyright 2023 Adaptive Financial Consulting
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aeron.samples.admin.cluster;

import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.logbuffer.Header;
import io.aeron.samples.cluster.protocol.*;
import org.agrona.DirectBuffer;
import org.jline.reader.LineReader;
import org.jline.utils.AttributedStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Admin client egress listener
 */
public class AdminClientEgressListener implements EgressListener
{

//    //Note Taha : This is the egress Listener of the cluster that Admin utility
//    //makes for interacting with it's own "UI" component i.e. to interface
//    with the command line parameters


    private static final Logger LOGGER = LoggerFactory.getLogger(AdminClientEgressListener.class);
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final AddParticipantCommandResultDecoder addParticipantDecoder = new AddParticipantCommandResultDecoder();
    private final ParticipantListDecoder participantListDecoder = new ParticipantListDecoder();
    private final PendingMessageManager pendingMessageManager;
    private LineReader lineReader;

    /**
     * Constructor
     * @param pendingMessageManager the manager for pending messages
     */
    public AdminClientEgressListener(final PendingMessageManager pendingMessageManager)
    {
        this.pendingMessageManager = pendingMessageManager;
    }

    @Override
    public void onMessage(
        final long clusterSessionId,
        final long timestamp,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        if (length < MessageHeaderDecoder.ENCODED_LENGTH)
        {
            LOGGER.warn("Message too short");
            return;
        }
        messageHeaderDecoder.wrap(buffer, offset);

        switch (messageHeaderDecoder.templateId())
        {
            case AddParticipantCommandResultDecoder.TEMPLATE_ID ->
            {
                addParticipantDecoder.wrapAndApplyHeader(buffer, offset, messageHeaderDecoder);
                final String correlationId = addParticipantDecoder.correlationId();
                final long addedId = addParticipantDecoder.participantId();
                log("Participant added with id " + addedId, AttributedStyle.GREEN);
                pendingMessageManager.markMessageAsReceived(correlationId);
            }
            case ParticipantListDecoder.TEMPLATE_ID -> displayParticipants(buffer, offset);

            default -> log("unknown message type: " + messageHeaderDecoder.templateId(), AttributedStyle.RED);
        }
    }


    private void displayParticipants(final DirectBuffer buffer, final int offset)
    {
        participantListDecoder.wrapAndApplyHeader(buffer, offset, messageHeaderDecoder);
        pendingMessageManager.markMessageAsReceived(participantListDecoder.correlationId());
        final ParticipantListDecoder.ParticipantsDecoder participants = participantListDecoder.participants();
        final int count = participants.count();
        if (0 == count)
        {
            log("No participants exist in the cluster.",
                AttributedStyle.YELLOW);
        }
        else
        {
            log("Participant count: " + count, AttributedStyle.YELLOW);
            while (participants.hasNext())
            {
                participants.next();
                final long participantId = participants.participantId();
                final String name = participants.name();
                log("Participant: id " + participantId + " name: '" + name + "'", AttributedStyle.YELLOW);
            }
        }
    }

    @Override
    public void onSessionEvent(
        final long correlationId,
        final long clusterSessionId,
        final long leadershipTermId,
        final int leaderMemberId,
        final EventCode code,
        final String detail)
    {
        if (code != EventCode.OK)
        {
            log("Session event: " + code.name() + " " + detail + ". leadershipTermId=" + leadershipTermId,
                AttributedStyle.YELLOW);
        }
    }

    @Override
    public void onNewLeader(
        final long clusterSessionId,
        final long leadershipTermId,
        final int leaderMemberId,
        final String ingressEndpoints)
    {
        log("New Leader: " + leaderMemberId + ". leadershipTermId=" + leadershipTermId, AttributedStyle.YELLOW);
    }

    /**
     * Sets the terminal
     *
     * @param lineReader the lineReader
     */
    public void setLineReader(final LineReader lineReader)
    {
        this.lineReader = lineReader;
    }

    /**
     * Logs a message to the terminal if available or to the logger if not
     *
     * @param message message to log
     * @param color   message color to use
     */
    private void log(final String message, final int color)
    {
        LineReaderHelper.log(lineReader, message, color);
    }
}
