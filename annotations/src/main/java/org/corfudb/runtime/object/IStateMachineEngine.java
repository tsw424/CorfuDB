package org.corfudb.runtime.object;

import java.util.function.Supplier;

public interface IStateMachineEngine {

    /** Access a state machine's state.
     *
     * @param wrapper
     * @param accessFunction
     * @param conflictObject
     * @param <R>
     * @param <T>
     * @return
     */
    <R, T> R access(ICorfuSMR<T> wrapper,
           ICorfuSMRAccess<R, T> accessFunction,
           Object[] conflictObject);

    /** Log a update to a state machine.
     *
     * @param wrapper
     * @param conflictObject
     * @param <T>
     * @return
     */
    <T> long logUpdate(ICorfuSMR<T> wrapper,
                       String smrUpdateFunction, boolean keepUpcallResult,
                       Object[] conflictObject, Object... args);

    /** Get the result of an upcall (a state machine update which returns a value) at
     * a given address.
     * @param wrapper
     * @param address
     * @param conflictObject
     * @param <T>
     * @param <R>
     * @return
     */
    <T,R> R getUpcallResult (ICorfuSMR<T> wrapper,
                             long address,
                             Object[] conflictObject);
}
