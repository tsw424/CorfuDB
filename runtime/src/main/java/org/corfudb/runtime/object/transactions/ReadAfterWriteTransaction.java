package org.corfudb.runtime.object.transactions;

import org.corfudb.runtime.object.ICorfuSMR;

public class ReadAfterWriteTransaction
        extends AbstractOptimisticTransaction {

    public ReadAfterWriteTransaction(TransactionBuilder builder) {
        super(builder);
    }

    @Override
    protected <T> void addToReadSet(ICorfuSMR<T> wrapper,
                                       Object[] conflictObject) {
        Transactions.getContext().getConflictSet().add(wrapper, conflictObject);
    }

}
