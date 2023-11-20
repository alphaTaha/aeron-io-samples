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

import io.aeron.samples.cluster.protocol.AddParticipantCommandResultEncoder;
import io.aeron.samples.cluster.protocol.MessageHeaderEncoder;
import io.aeron.samples.cluster.protocol.ParticipantListEncoder;
import io.aeron.samples.domain.participants.Participant;
import org.agrona.ExpandableDirectByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Implementation of the {@link ClusterClientResponder} interface which returns SBE encoded results to the client
 */
public class ClusterClientResponderImpl implements ClusterClientResponder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterClientResponderImpl.class);
    private final SessionMessageContextImpl context;
    private final AddParticipantCommandResultEncoder addParticipantResultEncoder =
        new AddParticipantCommandResultEncoder();
    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final ExpandableDirectByteBuffer buffer = new ExpandableDirectByteBuffer(1024);
    private final ParticipantListEncoder participantListEncoder = new ParticipantListEncoder();
    /**
     * Constructor
     *
     * @param context the context to use in order to interact with clients
     */
    public ClusterClientResponderImpl(final SessionMessageContextImpl context)
    {
        this.context = context;
    }

    @Override
    public void acknowledgeParticipantAdded(final long participantId, final String correlationId)
    {
        //Sending the response back to the client
        addParticipantResultEncoder.wrapAndApplyHeader(buffer, 0, messageHeaderEncoder);
        addParticipantResultEncoder.correlationId(correlationId);
        addParticipantResultEncoder.participantId(participantId);
        context.reply(buffer, 0, MessageHeaderEncoder.ENCODED_LENGTH + addParticipantResultEncoder.encodedLength());
    }

    @Override
    public void returnParticipantList(final List<Participant> participants, final String correlationId)
    {
        participantListEncoder.wrapAndApplyHeader(buffer, 0, messageHeaderEncoder);
        participantListEncoder.correlationId(correlationId);

        final ParticipantListEncoder.ParticipantsEncoder participantsEncoder =
            participantListEncoder.participantsCount(participants.size());

        for (int i = 0; i < participants.size(); i++)
        {
            final Participant participant = participants.get(i);
            participantsEncoder.next()
                .participantId(participant.participantId())
                .name(participant.name());
        }

        context.reply(buffer, 0, MessageHeaderEncoder.ENCODED_LENGTH +
            participantListEncoder.encodedLength());
    }

}
