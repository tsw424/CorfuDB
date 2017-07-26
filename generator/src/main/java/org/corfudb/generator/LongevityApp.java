package org.corfudb.generator;

import ch.qos.logback.classic.Logger;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.operations.CheckpointOperation;
import org.corfudb.generator.operations.Operation;
import org.corfudb.runtime.CorfuRuntime;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by rmichoud on 7/25/17.
 */

/**
 * This Longevity app will stress test corfu using the Load
 * generator during a given duration.
 *
 * Current implementation spawns a task producer and a task
 * consumer. Both of them are a single threaded. In combination
 * with the blocking queue, they limit the amount of task created
 * during execution. Apps thread pool will take care of executing
 * the operations.
 *
 */
@Slf4j
public class LongevityApp {

    Logger correctness = (Logger) LoggerFactory.getLogger("correctness");

    private long durationMs;
    private String configurationString;
    BlockingQueue<Future> appsFutures;
    CorfuRuntime rt;
    State state;

    ExecutorService taskProducer;
    ExecutorService taskConsumer;
    ScheduledExecutorService scheduler;
    ExecutorService apps;

    final int applicationTimeout = 20;


    public LongevityApp(long durationMs, String configurationString) {
        this.durationMs = durationMs;
        this.configurationString = configurationString;

        appsFutures = new ArrayBlockingQueue<>(1000);
        rt = new CorfuRuntime(configurationString).connect();
        state = new State(50, 100, rt);

        taskProducer = Executors.newSingleThreadExecutor();
        taskConsumer = Executors.newSingleThreadExecutor();

        apps = Executors.newFixedThreadPool(10);
        scheduler = Executors.newScheduledThreadPool(1);


    }

//    public void dumpInitialState() {
//        for ()
//    }

    public void runLongevityTest() {
        long startTime = System.currentTimeMillis();

        Runnable task = () -> {
            try {
                Operation current = state.getOperations().getRandomOperation();

                current.execute();
            } catch (Exception e) {
                log.error("Operation failed", e);
            }
        };

        Runnable cpTrimTask = () -> {
            Operation op = new CheckpointOperation(state);
            op.execute();
        };
        scheduler.scheduleAtFixedRate(cpTrimTask, 30, 20, TimeUnit.SECONDS);

        taskProducer.execute(() -> {
            while(System.currentTimeMillis() - startTime < durationMs) {
                try {
                    appsFutures.put(apps.submit(task));
                } catch (InterruptedException e) {
                    log.error("Operation failed", e);
                }
            }
        });

        taskConsumer.execute(() -> {
            while (System.currentTimeMillis() - startTime < durationMs || !appsFutures.isEmpty()) {
                if (!appsFutures.isEmpty()) {
                    Future f = appsFutures.poll();
                    try {
                        f.get();
                    } catch (Exception e) {
                        log.error("Operation failed", e);
                    }
                }
            }

            scheduler.shutdownNow();
            apps.shutdown();
            try {
                apps.awaitTermination(applicationTimeout, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error("Application was stuck", e);
            }

        });

        taskProducer.shutdown();
        taskConsumer.shutdown();
    }
}
