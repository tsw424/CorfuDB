package org.corfudb.runtime.object.transactions;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import lombok.Getter;

import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.ICorfuSMRProxyInternal;

/**
 * This class captures information about objects mutated (written) during speculative
 * transaction execution.
 */
@Getter
public class WriteSet extends ConflictSet {

    /** The actual updates to mutated objects. */
    final MultiObjectSMREntry writeSet = new MultiObjectSMREntry();


    public <T> long add(ICorfuSMR<T> wrapper, SMREntry updateEntry, Object[] conflictObjects) {
        super.add(wrapper, conflictObjects);
        writeSet.addTo(wrapper.getCorfuStreamID(), updateEntry);
        return writeSet.getSMRUpdates(wrapper.getCorfuStreamID()).size() - 1;
    }

}
