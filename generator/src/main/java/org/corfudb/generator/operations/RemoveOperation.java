package org.corfudb.generator.operations;

import java.util.UUID;

import ch.qos.logback.classic.Logger;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.State;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.slf4j.LoggerFactory;

/**
 * Created by maithem on 7/14/17.
 */
@Slf4j
public class RemoveOperation extends Operation {

    Logger correctness = (Logger) LoggerFactory.getLogger("correctness");

    public RemoveOperation(State state) {
        super(state);
    }

    @Override
    public void execute() {
        UUID streamID = (UUID) state.getStreams().sample(1).get(0);
        String key = (String) state.getKeys().sample(1).get(0);
        String operation = "Rm";
        if(state.getRuntime().getObjectsView().TXActive()) {
            operation = "TxRm";
        }
        state.getMap(streamID).remove(key);
        if (TransactionalContext.isInTransaction()) {
            correctness.info("{}, {}:{}, {}", operation, streamID, key,
                    TransactionalContext.getCurrentContext().getSnapshotTimestamp());
        } else {
            correctness.info("{}, {}:{}", operation, streamID, key);
        }
    }
}
