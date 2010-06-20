/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2010 Oracle.  All rights reserved.
 *
 */

package com.sleepycat.je.rep.elections;

import java.util.Set;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.rep.elections.Protocol.Promise;
import com.sleepycat.je.rep.elections.Protocol.Value;
import com.sleepycat.je.rep.impl.TextProtocol.MessageExchange;
import com.sleepycat.je.rep.impl.node.NameIdPair;

/**
 * Extends the base proposer to choose a phase 2 value based on a suggestion's
 * relative ranking.
 */
public class RankingProposer extends Proposer {

    public RankingProposer(Elections elections,
                           NameIdPair nameIdPair) {
        super(elections, nameIdPair);
    }

    /**
     * Chooses a Value based on the relative ranking of all Promise responses.
     * The one with the highest ranking is chosen. In the case of a tie, the
     * socket address is used to order the choice so that a consistent result
     * is obtained across the set irrespective of the iteration order over the
     * set.
     */
    @Override
    protected Value choosePhase2Value(Set<MessageExchange> exchanges) {
        long maxRanking = Long.MIN_VALUE;
        int maxPriority = Integer.MIN_VALUE;
        String maxTarget = null;

        Value acceptorValue = null;
        for (MessageExchange me : exchanges) {
            if (me.getResponseMessage().getOp() !=
                elections.getProtocol().PROMISE) {
                continue;
            }
            final Promise p = (Promise) me.getResponseMessage();
            if (p.getSuggestionRanking() < maxRanking) {
               continue;
            }
            /* Use priority as a tie breaker. */
            if (p.getSuggestionRanking() == maxRanking) {
              if (p.getPriority() < maxPriority) {
                  continue;
              }
              /*
               * Use socket address to choose in case of a tie, so we
               * always have a consistent ordering.
               */
              if ((p.getPriority() ==  maxPriority) &&
                  ((maxTarget != null) &&
                   (me.target.toString().compareTo(maxTarget) <= 0))) {
                  continue;
              }
            }

            acceptorValue = p.getSuggestion();
            maxRanking = p.getSuggestionRanking();
            maxPriority = p.getPriority();
            maxTarget = me.target.toString();
        }

        if (acceptorValue == null) {
            throw EnvironmentFailureException.unexpectedState
                ("No suggestion found in message exchange");
        }
        return acceptorValue;
    }

    /**
     * Returns a proposal number. Note that the proposal numbers must increase
     * over time, even across restarts of the proposer process.
     * @return a 24 character string representing the proposal number
     */
    @Override
    public synchronized Proposal nextProposal() {
        return proposalGenerator.nextProposal();
    }

    private final TimebasedProposalGenerator proposalGenerator =
        new TimebasedProposalGenerator();
}
