package org.corfudb.runtime.object;

import java.util.Collections;

import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.exceptions.TrimmedUpcallException;
import org.corfudb.runtime.view.ObjectBuilder;
import org.corfudb.util.Utils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LinearizableEngine implements IStateMachineEngine {

    final CorfuRuntime runtime;

    public LinearizableEngine(CorfuRuntime runtime) {
        this.runtime = runtime;
    }

    /** {@inheritDoc} */
    @Override
    public <R, T> R access(ICorfuWrapper<T> wrapper,
                           IStateMachineAccess<R, T> accessFunction,
                           Object[] conflictObject) {

        // Linearize this read against a timestamp
        final long timestamp =
                runtime.getSequencerView()
                        .nextToken(Collections.singleton(wrapper.getId$CORFU()),
                                0).getToken()
                        .getTokenValue();
        log.debug("access[{}] conflictObj={} version={}",
                Utils.toReadableId(wrapper.getId$CORFU()), conflictObject, timestamp);

        // Perform underlying access
        try {
            return ((VersionedObjectManager<T>)wrapper.getObjectManager$CORFU())
                    .access(o -> o.getVersionUnsafe() >= timestamp
                            && !o.isOptimisticallyModifiedUnsafe(),
                    o -> o.syncObjectUnsafe(timestamp),
                    o -> accessFunction.access(o));
        } catch (TrimmedException te) {
            log.warn("access[{}] Encountered Trim, reset and retry",
                    Utils.toReadableId(wrapper.getId$CORFU()));
            // We encountered a TRIM during sync, reset the object
            ((VersionedObjectManager<T>)wrapper.getObjectManager$CORFU()).update(o -> {
                o.resetUnsafe();
                return null;
            });
            // And attempt an access again.
            return access(wrapper, accessFunction, conflictObject);
        }
    }

    /** {@inheritDoc} */
    @Override
    public <T> long logUpdate(ICorfuWrapper<T> wrapper,
                              String smrUpdateFunction,
                              boolean keepUpcallResult,
                              Object[] conflictObject,
                              Object... args) {
        // If we aren't in a transaction, we can just write the modification.
        // We need to add the acquired token into the pending upcall list.
        SMREntry smrEntry = new SMREntry(smrUpdateFunction, args,
                ((ObjectBuilder<T>)wrapper.getCorfuBuilder()).getSerializer());


        long address = ((VersionedObjectManager<T>)wrapper.getObjectManager$CORFU())
                                                .logUpdate(smrEntry, keepUpcallResult);

        log.trace("update[{}] {}@{} ({}) conflictObj={}",
                Utils.toReadableId(wrapper.getId$CORFU()),
                smrUpdateFunction, address, args, conflictObject);
        return address;
    }

    /** {@inheritDoc} */
    @Override
    public <T, R> R getUpcallResult(ICorfuWrapper<T> wrapper,
                                    long address,
                                    Object[] conflictObject) {
        // Check first if we have the upcall, if we do
        // we can service the request right away.
        if (((VersionedObjectManager<T>)wrapper.getObjectManager$CORFU())
                .upcallResults.containsKey(address)) {
            log.trace("Upcall[{}] {} Direct", this, address);
            R ret = (R) ((VersionedObjectManager<T>)wrapper.getObjectManager$CORFU())
                    .upcallResults.get(address);
            ((VersionedObjectManager<T>)wrapper.getObjectManager$CORFU())
                    .upcallResults.remove(address);
            return ret == VersionedObjectManager.NullValue.NULL_VALUE ? null : ret;
        }

        try {
            return ((VersionedObjectManager<T>)wrapper.getObjectManager$CORFU()).update(o -> {
                o.syncObjectUnsafe(address);
                if (o.upcallResults.containsKey(address)) {
                    log.trace("Upcall[{}] {} Sync'd", this, address);
                    R ret = (R) o.upcallResults.get(address);
                    o.upcallResults.remove(address);
                    return ret == VersionedObjectManager.NullValue.NULL_VALUE ? null : ret;
                }

                // The version is already ahead, but we don't have the result.
                // The only way to get the correct result
                // of the upcall would be to rollback. For now, we throw an exception
                // since this is generally not expected. --- and probably a bug if it happens.
                throw new RuntimeException("Attempted to get the result "
                        + "of an upcall@" + address + " but we are @"
                        + ((VersionedObjectManager<T>)wrapper.getObjectManager$CORFU())
                        .getVersionUnsafe()
                        + " and we don't have a copy");
            });
        } catch (TrimmedException ex) {
            throw new TrimmedUpcallException(address);
        }
    }
}
