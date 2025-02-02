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

package io.aeron.samples.infra;


import io.aeron.samples.cluster.protocol.AddParticipantCommandDecoder;
import io.aeron.samples.cluster.protocol.ListParticipantsCommandDecoder;
import io.aeron.samples.cluster.protocol.MessageHeaderDecoder;
import io.aeron.samples.domain.participants.Participant;
import io.aeron.samples.domain.participants.Participants;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Demultiplexes messages from the ingress stream to the appropriate domain handler.
 */
public class SbeDemuxer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SbeDemuxer.class);
    private final Participants participants;
    private final ClusterClientResponder responder;

    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();

    private final AddParticipantCommandDecoder addParticipantDecoder = new AddParticipantCommandDecoder();
    private final ListParticipantsCommandDecoder listParticipantsDecoder = new ListParticipantsCommandDecoder();


    /**
     * Dispatches ingress messages to domain logic.
     *
     * @param participants          the participants domain model to which commands are dispatched
     * @param responder             the responder to which responses are sent
     */
    public SbeDemuxer(
        final Participants participants,
        final ClusterClientResponder responder)
    {
        this.participants = participants;
        this.responder = responder;
    }

    /**
     * Dispatch a message to the appropriate domain handler.
     *
     * @param buffer the buffer containing the inbound message, including a header
     * @param offset the offset to apply
     * @param length the length of the message
     */
    public void dispatch(final DirectBuffer buffer, final int offset, final int length)
    {
        if (length < MessageHeaderDecoder.ENCODED_LENGTH)
        {
            LOGGER.error("Message too short, ignored.");
            return;
        }
        headerDecoder.wrap(buffer, offset);

        switch (headerDecoder.templateId())
        {
            case AddParticipantCommandDecoder.TEMPLATE_ID ->
            {
                addParticipantDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                participants.addParticipant(addParticipantDecoder.participantId(),
                    addParticipantDecoder.correlationId(), addParticipantDecoder.name());
            }
            case ListParticipantsCommandDecoder.TEMPLATE_ID ->
            {
                listParticipantsDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                final List<Participant> participantList = participants.getParticipantList();
                responder.returnParticipantList(participantList, listParticipantsDecoder.correlationId());
            }
            default -> LOGGER.error("Unknown message template {}, ignored.", headerDecoder.templateId());
        }
    }
}
