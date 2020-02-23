# Notes for implementing two-phase commit

The transaction prepare mechanism provides a building block for supporting distributed
[two-phase commits](https://en.wikipedia.org/wiki/Two-phase_commit_protocol). Here is a recipe
for implementing it fully, assuming that each cluster is replicated and independent of each
other.

One coordinator "owns" the entire workflow, and it belongs to one cluster. The coordinator can
be chosen by any means, possibly randomly. The other clusters involved in the transaction are
designated as participants, as per the usual two-phase commit protocol definition.

1. A transaction is created on each cluster in the usual fashion, and updates are applied to them.
2. The coordinator encodes a message which consists of its transaction id and all of the
   cluster names involved, and then sends it to each participant.
3. Each participant prepares its own transaction with the message provided by the
   coordinator. It's not strictly required that each participant record all the other
   participants, but it must record the coordinator cluster name and its transaction id.
   Knowing the other participants can be used for certain recovery scenarios, so it's a good
   idea to record it anyhow. The participant replies back with its own transaction id.
4. Upon receiving replies from all participants, the coordinator then prepares its own
   transaction, recording the participant cluster names and their transaction ids. It should
   use the prepareCommit method, but the regular prepare method works too. Once the
   coordinator's transaction is prepared, the coordinator is now "committed" to committing the
   entire transaction, and the second phase begins. If the coordinator wanted to roll back, it
   shouldn't have prepared its own transaction.
5. The coordinator sends a commit message to each participant, and each of them commits its own
   transaction and replies success. The participants cannot roll back.
6. Upon receiving replies from all participants, the coordinator commits its own transaction,
   and now the entire two-phase commit is complete.

## Failure handling

The first requirement is that all clusters must be replicated. If a prepared transaction fails
to finish normally, then a recovery handler is required to complete the transaction, based on
what it learned from the prepared message.

Any failure prior to step 3 should be detected by whatever socket disconnect protocol is being
used. The transactions aren't prepared yet, and so they can be rolled back normally.

To handle failures after step 3, all prepared participants must periodically poll the
coordinator to determine the final state of the transaction. This polling begins as soon as the
transaction is prepared, and it continues after the prepared transaction is recovered. If the
coordinator is aware of the transaction (the one provided by the coordinator and recorded in
the participant's message), then the participant must continue waiting for a command from the
coordinator.

If the coordinator doesn't have any knowledge of the transaction, then it's safe to assume that
the coordinator rolled back, and the participant should roll back as well. Note that in a
replicated cluster, the participant must be the cluster leader for the roll back to succeed.

To avoid race conditions, the participants must only attempt to contact the leader of the
coordinator's cluster. Before responding that a coordinator transaction is missing, the
isLeader method must return true. It's possible to add an enhancement in which the transaction
records the replication log position, and then a participant can accept a response from a
replica, if it's caught up. Note that the isLeader method doesn't return true unless fully
caught up, and so this technique is free of race conditions.

If the coordinator cluster was decommissioned for some reason, the polling participant won't get
any response, and so the transaction is stuck. There's no best way to handle this situation,
but the simplest thing to do is roll back and potentially break atomicity. An improved strategy
involves contacting the other participants. If they're all available and also stuck, then the
participant can commit an auxiliary transaction which records a decision to commit. It then
commits the original transaction. This auxiliary state must linger until all participants have
acknowledged and committed too. If some of the participants have been decommissioned, then their
input isn't required.

A failure after step 4 requires that the coordinator continue its job to fully commit the
transaction, by completing steps 5 and 6. The recovery handler knows it should act as a
coordinator when the prepareCommit method is called, or else it would recover as a
participant. If the coordinator called the regular prepare method, it would need to encode
something in the message to determine if it should act as a coordinator when recovered.

The coordinator sweeps through all the participants and asks them to commit the transaction. If
they have no knowledge of it, then this implies that they already committed, based on a previous
attempt by the coordinator. They simply respond with success in this case. The coordinator
keeps trying to contact participants which haven't responded, and it never gives up or rolls
back. If a participant cluster is decommissioned, then the coordinator can simply skip it,
treating it as a success.

The coordinator must only contact participant instances which are leaders, or those which are
known to be caught up. If the participant is a replica, then it cannot commit anyhow.

Prior to step 4, the coordinator can choose to roll back the transaction, if for example one of
the participants didn't respond in time. All it needs to do is roll back its own transaction
(which hasn't been prepared yet), and then it sends a message to all participants to roll back
as well. If the coordinator crashes before it can roll back its own transaction or send
messages to the participants, the two-phase commit transaction will still roll back, although
this is dependent on the polling interval and how long it takes for the coordinator's cluster
to elect a new leader. When a new leader is elected, the coordinator's transaction rolls back
automatically because it was never prepared. The polling participants eventually learn that the
coordinator's transaction is gone, and then they roll back too.

## More notes

In steps 3 and 4, the participant transaction id is stored in the coordinator's prepare
message. This isn't strictly necessary, but it might be useful for verification or repair. The
coordinator can send messages to participants with the coordinator and participant transaction
ids, and the participant can verify that the mapping is correct. A similar verification can be
performed when the participants poll the coordinator.
