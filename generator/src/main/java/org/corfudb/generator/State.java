package org.corfudb.generator;

import com.google.common.reflect.TypeToken;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import lombok.RequiredArgsConstructor;
import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.distributions.OperationCount;
import org.corfudb.generator.distributions.Operations;
import org.corfudb.generator.distributions.Streams;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.object.transactions.TransactionType;

import lombok.Getter;
import lombok.Setter;

/**
 * This object keeps state information of the different data distributions and runtime client.
 *
 * Created by maithem on 7/14/17.
 */
public class State {

    @RequiredArgsConstructor
    public enum StringIndexer implements
            CorfuTable.IndexSpecification<String, String, String, String> {
        BY_VALUE((k,v) -> Collections.singleton(v)),
        BY_FIRST_CHAR((k, v) -> Collections.singleton(Character.toString(v.charAt(0))))
        ;

        @Getter
        final CorfuTable.IndexFunction<String, String, String> indexFunction;

        @Getter
        final CorfuTable.ProjectionFunction<String, String, String, String> projectionFunction
                = (i, s) -> s.map(entry -> entry.getValue());
    }

    @Getter
    final Streams streams;

    @Getter
    final Keys keys;

    @Getter
    final OperationCount operationCount;

    @Getter
    final Operations operations;

    @Getter
    final CorfuRuntime runtime;

    @Getter
    final Map<UUID, CorfuTable> maps;

    @Getter
    @Setter
    volatile long trimMark = -1;

    public State(int numStreams, int numKeys, CorfuRuntime rt) {
        streams = new Streams(numStreams);
        keys = new Keys(numKeys);
        operationCount = new OperationCount();

        this.runtime = rt;
        maps = new HashMap<>();
        operations = new Operations(this);

        streams.populate();
        keys.populate();
        operationCount.populate();
        operations.populate();

        openObjects();
    }

    private void openObjects() {
        for (UUID uuid : streams.getDataSet()) {
            CorfuTable<String, String, StringIndexer, String> map = runtime.getObjectsView()
                    .build()
                    .setStreamID(uuid)
                    .setTypeToken(new TypeToken<CorfuTable<String, String,
                            StringIndexer, String>>() {})
                    .setArguments(StringIndexer.class)
                    .open();

            maps.put(uuid, map);
        }
    }

    public Map<String, String> getMap(UUID uuid) {
        Map map = maps.get(uuid);
        if (map == null) {
            throw new RuntimeException("Map doesn't exist");
        }
        return maps.get(uuid);
    }

    public Collection<CorfuTable> getMaps() {
        return maps.values();
    }

    public void startOptimisticTx() {
        runtime.getObjectsView().TXBegin();
    }

    public long stopOptimisticTx() {
        return runtime.getObjectsView().TXEnd();
    }

    public void startSnapshotTx(long snapshot) {
        runtime.getObjectsView()
                .TXBuild()
                .setType(TransactionType.SNAPSHOT)
                .setSnapshot(snapshot)
                .begin();
    }

    public long stopSnapshotTx() {
        return runtime.getObjectsView().TXEnd();
    }
}
