package org.corfudb.generator.operations;

import ch.qos.logback.classic.Logger;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.State;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

/**
 * Created by box on 7/15/17.
 */
@Slf4j
public class SnapshotTxOperation extends Operation {

    Logger correctness = (Logger) LoggerFactory.getLogger("correctness");

    public SnapshotTxOperation(State state) {
        super(state);
    }

    @Override
    public void execute() {
        long trimMark = state.getTrimMark();
        Random rand = new Random();
        long delta = (long) rand.nextInt(10) + 1;

        correctness.info("TxSnap, start");
        state.startSnapshotTx(trimMark + delta);

        int numOperations = state.getOperationCount().sample(1).get(0);
        List<Operation> operations = state.getOperations().sample(numOperations);

        for (int x = 0; x < operations.size(); x++) {
            if (operations.get(x) instanceof OptimisticTxOperation
                    || operations.get(x) instanceof SnapshotTxOperation
                    || operations.get(x) instanceof RemoveOperation
                    || operations.get(x) instanceof WriteOperation
                    || operations.get(x) instanceof NestedTxOperation) {
                continue;
            }

            operations.get(x).execute();
        }
        try {
            state.stopSnapshotTx();
        } catch (TransactionAbortedException tae) {
            correctness.info("TxSnap, aborted");
            throw tae;
        }

        correctness.info("TxSnap, end");
    }
}
