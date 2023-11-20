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

import io.aeron.samples.domain.participants.Participant;

import java.util.List;

/**
 * Interface for responding to auction requests, encapsulating the SBE encoding and Aeron interactions
 */
public interface ClusterClientResponder
{

    /**
     * Acknowledges that a participant has been added to the client using the correlation they provided
     * @param participantId the id of the participant added
     * @param correlationId the correlation id provided by the client
     */
    void acknowledgeParticipantAdded(long participantId, String correlationId);

    /**
     * Lists all participants in the cluster
     *
     * @param participantList the list of participants to return
     * @param correlationId
     */
    void returnParticipantList(List<Participant> participantList, String correlationId);
}
