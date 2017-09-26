package org.corfudb.runtime.object;

import java.util.function.Supplier;

/** Interface for a manager, which manages an object.
 *
 * @param <T>   The type the manager manages.
 */
public interface IObjectManager<T> {

    /** Get a builder for this object.
     *
     * @return  A builder for this object.
     */
    IObjectBuilder<T> getBuilder();

    /** Get a engine for this state machine.
     *
     * @return  A state machine replication engine.
     */
    IStateMachineEngine getEngine();

    /** Get the current version of this object.
     *
     * @return The current version of the object.
     */
    long getVersion();


    /** Execute the given function as a transaction, potentially affecting multiple state
     * machines.
     *
     * @param txFunction
     * @param <R>
     * @return
     */
    <R> R txExecute(Supplier<R> txFunction);
}
