package org.corfudb.runtime.object;

import java.util.UUID;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.ObjectBuilder;

/**
 * A Corfu container object is a container for other Corfu objects.
 * It has explicit access to its own stream ID, and a runtime, allowing it
 * to manipulate and return other Corfu objects.
 *
 * <p>Created by mwei on 11/12/16.
 */

public abstract class AbstractCorfuWrapper<T> implements ICorfuSMRProxyWrapper<T> {

    ICorfuSMRProxy<T> proxy;

    /**
     * Get a builder, which allows the construction of
     * new Corfu objects.
     */
    protected IObjectBuilder<?> getNewBuilder() {
        return ((ObjectBuilder<T>)this.getObjectManager$CORFU().getBuilder()).getRuntime()
                .getObjectsView().build();
    }

    /**
     * Get the stream ID that this container belongs to.
     *
     * @return Returns the StreamID of this Corfu Wrapper.
     */
    protected UUID getStreamId() {
        return this.getObjectManager$CORFU().getBuilder().getStreamId();
    }

    @Override
    public void setProxy$CORFU(ICorfuSMRProxy<T> proxy) {
        this.proxy = proxy;
    }
}
