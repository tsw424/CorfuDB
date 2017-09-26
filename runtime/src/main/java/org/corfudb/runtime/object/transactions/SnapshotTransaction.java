package org.corfudb.runtime.object.transactions;

import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.ICorfuSMRAccess;
import org.corfudb.runtime.object.VersionedObjectManager;

/**
 * A snapshot transactional context.
 *
 * <p>Given the snapshot (log address) given by the TransactionBuilder,
 * access all objects within the same snapshot during the course of
 * this transactional context.
 *
 * <p>Created by mwei on 11/22/16.
 */
public class SnapshotTransaction extends AbstractTransaction {

    final long previousSnapshot;

    public SnapshotTransaction(TransactionBuilder builder) {
        super(builder);
        previousSnapshot = Transactions.getReadSnapshot();
        Transactions.getContext().setReadSnapshot(builder.snapshot);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <R, T> R access(ICorfuSMR<T> wrapper,
                           ICorfuSMRAccess<R, T> accessFunction,
                           Object[] conflictObject) {

        // In snapshot transactions, there are no conflicts.
        // Hence, we do not need to add this access to a conflict set
        // do not add: addToReadSet(proxy, conflictObject);
        return ((VersionedObjectManager<T>)wrapper.getObjectManager$CORFU())
                    .access(o -> o.getVersionUnsafe()
                        == builder.getSnapshot()
                        && !o.isOptimisticallyModifiedUnsafe(),
                o -> {
                    syncWithRetryUnsafe(o, builder.getSnapshot(), wrapper, null);
                },
                o -> accessFunction.access(o));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T, R> R getUpcallResult(ICorfuSMR<T> wrapper,
                                      long timestamp,
                                      Object[] conflictObject) {
        throw new UnsupportedOperationException("Can't get upcall during a read-only transaction!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> long logUpdate(ICorfuSMR<T> wrapper,
                              String smrUpdateFunction, boolean keepUpcallResult,
                              Object[] conflictObject, Object... args) {
        throw new UnsupportedOperationException(
                "Can't modify object during a read-only transaction!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long commit() throws TransactionAbortedException {
        // Restore the previous snapshot on commit.
        Transactions.getContext().setReadSnapshot(previousSnapshot);
        return super.commit();
    }

}
