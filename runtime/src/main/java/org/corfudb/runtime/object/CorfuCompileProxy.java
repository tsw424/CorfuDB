package org.corfudb.runtime.object;

import static java.lang.Long.min;

import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.UUID;
import java.util.function.Supplier;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.transactions.AbstractTransaction;
import org.corfudb.runtime.object.transactions.Transactions;
import org.corfudb.runtime.view.ObjectBuilder;
import org.corfudb.util.Utils;
import org.corfudb.util.serializer.ISerializer;

/**
 * In the Corfu runtime, on top of a stream,
 * an SMR object layer implements objects whose history of updates
 * are backed by a stream.
 *
 * <p>This class implements the methods that an in-memory corfu-object proxy carries
 * in order to by in sync with a stream.
 *
 * <p>We refer to the program's object as the -corfu object-,
 * and to the internal object implementation as the -proxy-.
 *
 * <p>If a Corfu object's method is an Accessor, it invokes the proxy's
 * access() method.
 *
 * <p>If a Corfu object's method is a Mutator or Accessor-Mutator, it invokes the
 * proxy's logUpdate() method.
 *
 * <p>Finally, if a Corfu object's method is an Accessor-Mutator,
 * it obtains a result by invoking getUpcallResult().
 *
 * <p>Created by mwei on 11/11/16.
 */
@Slf4j
public class CorfuCompileProxy<T> implements ICorfuSMRProxyInternal<T> {

    /**
     * The underlying object. This object stores the actual
     * state as well as the version of the object. It also
     * provides locks to access the object safely from a
     * multi-threaded context.
     */
    @Getter
    final VersionedObjectManager<T> underlyingObject;

    /**
     * The ID of the stream of the log.
     */
    @Deprecated // TODO: Add replacement method that conforms to style
    @SuppressWarnings("checkstyle:abbreviation") // Due to deprecation
    UUID streamID;

    /**
     * The type of the underlying object. We use this to instantiate
     * new instances of the underlying object.
     */
    Class<T> type;

    /**
     * The serializer SMR entries will use to serialize their
     * arguments.
     */
    @Getter
    ISerializer serializer;

    /** The wrapper this proxy was created with.
     *
     */
    final ICorfuSMR<T> wrapper;

    /**
     * Creates a CorfuCompileProxy object on a particular stream.
     *
     * @param wrapper             The wrapper for this object.
     */
    @Deprecated // TODO: Add replacement method that conforms to style
    @SuppressWarnings("checkstyle:abbreviation") // Due to deprecation
    public CorfuCompileProxy(ICorfuSMR<T> wrapper) {
        this.streamID = wrapper.getCorfuBuilder().getStreamId();
        this.type = wrapper.getCorfuBuilder().getType();
        this.serializer = ((ObjectBuilder<T>)wrapper.getCorfuBuilder()).getSerializer();

        this.wrapper = wrapper;
        this.underlyingObject = (VersionedObjectManager) wrapper.getObjectManager$CORFU();

    }

    static private <T> CorfuRuntime getRuntimeFromWrapper(ICorfuSMR<T> wrapper) {
        return ((ObjectBuilder<T>) wrapper.getCorfuBuilder()).getRuntime();
    }

    static private <T> Object[] getArgumentsFromWrapper(ICorfuSMR<T> wrapper) {
        return ((ObjectBuilder<T>) wrapper.getCorfuBuilder()).getArguments();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public <R> R access(ICorfuSMRAccess<R, T> accessMethod,
                        Object[] conflictObject) {
        throw new RuntimeException("Shouldn't be here");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long logUpdate(String smrUpdateFunction, final boolean keepUpcallResult,
                          Object[] conflictObject, Object... args) {
        throw new RuntimeException("Shouldn't be here");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <R> R getUpcallResult(long timestamp, Object[] conflictObject) {
        throw new RuntimeException("Shouldn't be here");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sync() {
        // Linearize this read against a timestamp
        final long timestamp =
                getRuntimeFromWrapper(wrapper).getSequencerView()
                        .nextToken(Collections.singleton(streamID), 0).getToken()
                        .getTokenValue();

        log.debug("Sync[{}] {}", this, timestamp);

        // Acquire locks and perform read.
        underlyingObject.update(o -> {
            o.syncObjectUnsafe(timestamp);
            return null;
        });
    }

    /**
     * Get the ID of the stream this proxy is subscribed to.
     *
     * @return The UUID of the stream this proxy is subscribed to.
     */
    @Override
    public UUID getStreamID() {
        return streamID;
    }

    /**
     * Run in a transactional context.
     *
     * @param txFunction The function to run in a transactional context.
     * @return The value supplied by the function.
     */
    @Override
    public <R> R TXExecute(Supplier<R> txFunction) {
        // Don't nest transactions if we are already running transactionally
        if (Transactions.active()) {
            try {
                return txFunction.get();
            } catch (Exception e) {
                log.warn("TXExecute[{}] Abort with Exception: {}", this, e);
                this.abortTransaction(e);
            }
        }
        long sleepTime = 1L;
        final long maxSleepTime = 1000L;
        int retries = 1;
        while (true) {
            try {
                getRuntimeFromWrapper(wrapper).getObjectsView().TXBegin();
                R ret = txFunction.get();
                getRuntimeFromWrapper(wrapper).getObjectsView().TXEnd();
                return ret;
            } catch (TransactionAbortedException e) {
                // If TransactionAbortedException is due to a 'Network Exception' do not keep
                // retrying a nested transaction indefinitely (this could go on forever).
                // If this is part of an outer transaction abort and remove from context.
                // Re-throw exception to client.
                log.warn("TXExecute[{}] Abort with exception {}", this, e);
                if (e.getAbortCause() == AbortCause.NETWORK) {
                    if (Transactions.current() != null) {
                        try {
                            Transactions.abort();
                        } catch (TransactionAbortedException tae) {
                            // discard manual abort
                        }
                        throw e;
                    }
                }

                log.debug("Transactional function aborted due to {}, retrying after {} msec",
                        e, sleepTime);
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException ie) {
                    log.warn("TxExecuteInner retry sleep interrupted {}", ie);
                }
                sleepTime = min(sleepTime * 2L, maxSleepTime);
                retries++;
            } catch (Exception e) {
                log.warn("TXExecute[{}] Abort with Exception: {}", this, e);
                this.abortTransaction(e);
            }
        }
    }

    /**
     * Get an object builder to build new objects.
     *
     * @return An object which permits the construction of new objects.
     */
    @Override
    public IObjectBuilder<?> getObjectBuilder() {
        return getRuntimeFromWrapper(wrapper).getObjectsView().build();
    }

    /**
     * Return the type of the object being replicated.
     *
     * @return The type of the replicated object.
     */
    @Override
    public Class<T> getObjectType() {
        return type;
    }

    /**
     * Get the latest version read by the proxy.
     *
     * @return The latest version read by the proxy.
     */
    @Override
    public long getVersion() {
        return access(o -> underlyingObject.getVersionUnsafe(),
                null);
    }

    /**
     * Get a new instance of the real underlying object.
     *
     * @return An instance of the real underlying object
     */
    @SuppressWarnings("unchecked")
    private T getNewInstance() {
        try {
            T ret = null;
            if (getArgumentsFromWrapper(wrapper) == null ||
                    getArgumentsFromWrapper(wrapper).length == 0) {
                ret = type.newInstance();
            } else {
                // This loop is not ideal, but the easiest way to get around Java boxing,
                // which results in primitive constructors not matching.
                for (Constructor<?> constructor : type.getDeclaredConstructors()) {
                    try {
                        ret = (T) constructor.newInstance(getArgumentsFromWrapper(wrapper));
                        break;
                    } catch (Exception e) {
                        // just keep trying until one works.
                    }
                }
            }
            if (ret instanceof ICorfuSMRProxyWrapper) {
                ((ICorfuSMRProxyWrapper<T>) ret).setProxy$CORFU(this);
            }
            return ret;
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return type.getSimpleName() + "[" + Utils.toReadableId(streamID) + "]";
    }

    private void abortTransaction(Exception e) {
        long snapshotTimestamp;
        AbortCause abortCause;
        TransactionAbortedException tae;

        AbstractTransaction context = Transactions.current();

        if (e instanceof TransactionAbortedException) {
            tae = (TransactionAbortedException) e;
        } else {
            if (e instanceof NetworkException) {
                // If a 'NetworkException' was received within a transactional context, an attempt to
                // 'getSnapshotTimestamp' will also fail (as it requests it to the Sequencer).
                // A new NetworkException would prevent the earliest to be propagated and encapsulated
                // as a TransactionAbortedException.
                snapshotTimestamp = -1L;
                abortCause = AbortCause.NETWORK;
            } else if (e instanceof UnsupportedOperationException) {
                snapshotTimestamp = Transactions.getReadSnapshot();
                abortCause = AbortCause.UNSUPPORTED;
            } else {
                log.error("abort[{}] Abort Transaction with Exception {}", this, e);
                snapshotTimestamp = Transactions.getReadSnapshot();
                abortCause = AbortCause.UNDEFINED;
            }

            TxResolutionInfo txInfo = new TxResolutionInfo(
                    context.getTransactionID(), snapshotTimestamp);
            tae = new TransactionAbortedException(txInfo, null, getStreamID(),
                    abortCause, e, context);
            context.abort(tae);
        }


        // Discard the transaction chain, if present.
        try {
            Transactions.abort();
        } catch (TransactionAbortedException e2) {
            // discard manual abort
        }

        throw tae;
    }
}
