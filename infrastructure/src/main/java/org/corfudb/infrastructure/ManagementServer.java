package org.corfudb.infrastructure;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.netty.channel.ChannelHandlerContext;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.format.Types.NodeMetrics;
import org.corfudb.infrastructure.log.StreamLogFiles;
import org.corfudb.protocols.wireprotocol.AddNodeRequest;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.FailureDetectorMsg;
import org.corfudb.protocols.wireprotocol.FileSegmentReplicationRequest;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.clients.SequencerClient;
import org.corfudb.runtime.exceptions.LayoutModificationException;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;

/**
 * Instantiates and performs failure detection and handling asynchronously.
 *
 * <p>Failure Detector:
 * Executes detection policy (eg. PeriodicPollingPolicy).
 * It then checks for status of the nodes. If the result map is not empty,
 * there are failed nodes to be addressed. This then triggers the failure
 * handler which then responds to these failures based on a policy.
 *
 * <p>Created by zlokhandwala on 9/28/16.
 */
@Slf4j
public class ManagementServer extends AbstractServer {

    /**
     * The options map.
     */
    private final Map<String, Object> opts;
    private final ServerContext serverContext;

    private static final String PREFIX_MANAGEMENT = "MANAGEMENT";
    private static final String KEY_LAYOUT = "LAYOUT";

    private static final String metricsPrefix = "corfu.server.management-server.";

    private CorfuRuntime corfuRuntime;
    /**
     * Policy to be used to detect failures.
     */
    private IFailureDetectorPolicy failureDetectorPolicy;
    /**
     * Policy to be used to handle failures.
     */
    private IFailureHandlerPolicy failureHandlerPolicy;
    /**
     * Latest layout received from bootstrap or the runtime.
     */
    private volatile Layout latestLayout;
    /**
     * Bootstrap endpoint to seed the Management Server.
     */
    private String bootstrapEndpoint;
    /**
     * To determine whether the cluster is setup and the server is ready to
     * start handling the detected failures.
     */
    private boolean startFailureHandler = false;
    /**
     * Failure Handler Dispatcher to launch configuration changes or recovery.
     */
    FailureHandlerDispatcher failureHandlerDispatcher;
    /**
     * Interval in executing the failure detection policy.
     * In milliseconds.
     */
    @Getter
    private final long policyExecuteInterval = 1000;
    /**
     * To schedule failure detection.
     */
    @Getter
    private final ScheduledExecutorService failureDetectorService;
    /**
     * Future for periodic failure detection task.
     */
    private Future failureDetectorFuture = null;
    private boolean recovered = false;

    @Getter
    private volatile CompletableFuture<Boolean> sequencerBootstrappedFuture;

    /**
     * Returns new ManagementServer.
     *
     * @param serverContext context object providing parameters and objects
     */
    public ManagementServer(ServerContext serverContext) {

        this.opts = serverContext.getServerConfig();
        this.serverContext = serverContext;

        bootstrapEndpoint = (opts.get("--management-server") != null)
                ? opts.get("--management-server").toString() : null;
        sequencerBootstrappedFuture = new CompletableFuture<>();

        safeUpdateLayout(getCurrentLayout());
        // If no state was preserved, there is no layout to recover.
        if (latestLayout == null) {
            recovered = true;
        }

        if ((Boolean) opts.get("--single")) {
            String localAddress = opts.get("--address") + ":" + opts.get("<port>");

            Layout singleLayout = new Layout(
                    Collections.singletonList(localAddress),
                    Collections.singletonList(localAddress),
                    Collections.singletonList(new LayoutSegment(
                            Layout.ReplicationMode.CHAIN_REPLICATION,
                            0L,
                            -1L,
                            Collections.singletonList(
                                    new Layout.LayoutStripe(
                                            Collections.singletonList(localAddress)
                                    )
                            )
                    )),
                    0L
            );

            safeUpdateLayout(singleLayout);
        }

        this.failureDetectorPolicy = serverContext.getFailureDetectorPolicy();
        this.failureHandlerPolicy = serverContext.getFailureHandlerPolicy();
        this.failureHandlerDispatcher = new FailureHandlerDispatcher();
        this.failureDetectorService = Executors.newScheduledThreadPool(
                2,
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("FaultDetector-%d-" + getLocalEndpoint())
                        .build());

        // Initiating periodic task to poll for failures.
        try {
            failureDetectorService.scheduleAtFixedRate(
                    this::failureDetectorScheduler,
                    0,
                    policyExecuteInterval,
                    TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException err) {
            log.error("Error scheduling failure detection task, {}", err);
        }
    }

    private void bootstrapPrimarySequencerServer() {
        try {
            String primarySequencer = latestLayout.getSequencers().get(0);
            boolean bootstrapResult = getCorfuRuntime().getRouter(primarySequencer)
                    .getClient(SequencerClient.class)
                    .bootstrap(0L, Collections.emptyMap(), latestLayout.getEpoch())
                    .get();
            sequencerBootstrappedFuture.complete(bootstrapResult);
            if (!bootstrapResult) {
                log.warn("Sequencer already bootstrapped.");
            } else {
                log.info("Bootstrapped sequencer server at epoch:{}", latestLayout.getEpoch());
            }
            return;
        } catch (InterruptedException | ExecutionException e) {
            log.error("Bootstrapping sequencer failed. Retrying: {}", e);
        }
        getCorfuRuntime().invalidateLayout();
    }

    private boolean recover() {
        try {
            boolean recoveryResult = failureHandlerDispatcher
                    .recoverCluster((Layout) latestLayout.clone(), getCorfuRuntime());
            safeUpdateLayout(corfuRuntime.getLayoutView().getLayout());
            return recoveryResult;
        } catch (CloneNotSupportedException e) {
            log.error("Failure Handler could not clone layout: {}", e);
            log.error("Recover: Node will remain blocked until recovered successfully.");
            return false;
        }
    }

    /**
     * Handler for the base server.
     */
    @Getter
    private CorfuMsgHandler handler = new CorfuMsgHandler()
            .generateHandlers(MethodHandles.lookup(), this);

    /**
     * Thread safe updating of layout only if new layout has higher epoch value.
     *
     * @param layout New Layout
     */
    private synchronized void safeUpdateLayout(Layout layout) {
        // Cannot update with a null layout.
        if (layout == null) {
            return;
        }

        // Update only if new layout has a higher epoch than the existing layout.
        if (latestLayout == null || layout.getEpoch() > latestLayout.getEpoch()) {
            latestLayout = layout;
            // Persisting this new updated layout
            setCurrentLayout(latestLayout);
        }
    }

    /**
     * Sets the latest layout in the persistent datastore.
     *
     * @param layout Layout to be persisted
     */
    private void setCurrentLayout(Layout layout) {
        serverContext.getDataStore().put(Layout.class, PREFIX_MANAGEMENT, KEY_LAYOUT, layout);
    }

    /**
     * Fetches the latest layout from the persistent datastore.
     *
     * @return The last persisted layout
     */
    private Layout getCurrentLayout() {
        return serverContext.getDataStore().get(Layout.class, PREFIX_MANAGEMENT, KEY_LAYOUT);
    }

    boolean checkBootstrap(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        if (latestLayout == null && bootstrapEndpoint == null) {
            log.warn("Received message but not bootstrapped! Message={}", msg);
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.MANAGEMENT_NOBOOTSTRAP_ERROR));
            return false;
        }
        return true;
    }

    /**
     * Bootstraps the management server.
     * The msg contains the layout to be bootstrapped.
     *
     * @param msg corfu message containing MANAGEMENT_BOOTSTRAP_REQUEST
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    @ServerHandler(type = CorfuMsgType.MANAGEMENT_BOOTSTRAP_REQUEST, opTimer = metricsPrefix
            + "bootstrap-request")
    public synchronized void handleManagementBootstrap(CorfuPayloadMsg<Layout> msg,
                                                       ChannelHandlerContext ctx, IServerRouter r,
                                                       boolean isMetricsEnabled) {
        if (latestLayout != null) {
            // We are already bootstrapped, bootstrap again is not allowed.
            log.warn("Got a request to bootstrap a server which is already bootstrapped, "
                    + "rejecting!");
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.MANAGEMENT_ALREADY_BOOTSTRAP_ERROR));
        } else {
            log.info("Received Bootstrap Layout : {}", msg.getPayload());
            safeUpdateLayout(msg.getPayload());
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.ACK));
        }
    }

    /**
     * Trigger to start the failure handler.
     *
     * @param msg corfu message containing MANAGEMENT_START_FAILURE_HANDLER
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    @ServerHandler(type = CorfuMsgType.MANAGEMENT_START_FAILURE_HANDLER, opTimer = metricsPrefix
            + "start-failure-handler")
    public synchronized void initiateFailureHandler(CorfuMsg msg, ChannelHandlerContext ctx,
                                                    IServerRouter r,
                                                    boolean isMetricsEnabled) {
        if (isShutdown()) {
            log.warn("Management Server received {} but is shutdown.", msg.getMsgType().toString());
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.NACK));
            return;
        }
        // This server has not been bootstrapped yet, ignore all requests.
        if (!checkBootstrap(msg, ctx, r)) {
            return;
        }

        if (!startFailureHandler) {
            startFailureHandler = true;
            log.info("Initiated Failure Handler.");
        } else {
            log.info("Failure Handler already initiated.");
        }
        r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.ACK));
    }

    /**
     * Triggers the failure handler.
     * The msg contains the failed/defected nodes.
     *
     * @param msg corfu message containing MANAGEMENT_FAILURE_DETECTED
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    @ServerHandler(type = CorfuMsgType.MANAGEMENT_FAILURE_DETECTED, opTimer = metricsPrefix
            + "failure-detected")
    public synchronized void handleFailureDetectedMsg(CorfuPayloadMsg<FailureDetectorMsg> msg,
                                                      ChannelHandlerContext ctx, IServerRouter r,
                                                      boolean isMetricsEnabled) {
        if (isShutdown()) {
            log.warn("Management Server received {} but is shutdown.", msg.getMsgType().toString());
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.NACK));
            return;
        }
        // This server has not been bootstrapped yet, ignore all requests.
        if (!checkBootstrap(msg, ctx, r)) {
            return;
        }

        log.info("Received Failures : {}", msg.getPayload().getNodes());
        try {
            failureHandlerDispatcher.dispatchHandler(failureHandlerPolicy, (Layout) latestLayout
                    .clone(), getCorfuRuntime(), msg.getPayload().getNodes());
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.ACK));
        } catch (CloneNotSupportedException e) {
            log.error("Failure Handler could not clone layout: {}", e);
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.NACK));
        }
    }

    @ServerHandler(type = CorfuMsgType.ADD_NODE_REQUEST, opTimer = metricsPrefix + "add-node")
    public synchronized void handleAddNodeRequest(CorfuPayloadMsg<AddNodeRequest> msg,
                                                  ChannelHandlerContext ctx, IServerRouter r,
                                                  boolean isMetricsEnabled) {
        if (isShutdown()) {
            log.warn("Management Server received {} but is shutdown.", msg.getMsgType().toString());
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.NACK));
            return;
        }
        // This server has not been bootstrapped yet, ignore all requests.
        if (!checkBootstrap(msg, ctx, r)) {
            return;
        }

        log.info("Received add node request: {}", msg.getPayload());

        // Bootstrap the to-be added node with the old layout.
        IClientRouter newEndpointRouter = getCorfuRuntime().getRouter(msg.getPayload()
                .getEndpoint());
        try {
            // Ignoring call result as the call returns ACK or throws an exception.
            newEndpointRouter.getClient(LayoutClient.class).bootstrapLayout(latestLayout).get();
            newEndpointRouter.getClient(ManagementClient.class)
                    .bootstrapManagement(latestLayout).get();
            newEndpointRouter.getClient(ManagementClient.class).initiateFailureHandler().get();

            log.info("handleAddNodeRequest: New node {} bootstrapped.",
                    msg.getPayload().getEndpoint());
        } catch (Exception e) {
            log.error("handleAddNodeRequest: Aborting as new node could not be bootstrapped : ", e);
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.NACK));
            return;
        }

        try {
            failureHandlerDispatcher.addNode((Layout) latestLayout.clone(), getCorfuRuntime(),
                    msg.getPayload());
            safeUpdateLayout(getCorfuRuntime().getLayoutView().getLayout());
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.ACK));
        } catch (OutrankedException | QuorumUnreachableException | CloneNotSupportedException e) {
            log.error("Request to add new node: {} failed with exception:", msg.getPayload(), e);
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.NACK));
        }
    }

    @ServerHandler(type = CorfuMsgType.MERGE_SEGMENTS_REQUEST, opTimer = metricsPrefix
            + "merge-segments")
    public synchronized void handleMergeSegmentRequest(CorfuMsg msg,
                                                       ChannelHandlerContext ctx,
                                                       IServerRouter r, boolean isMetricsEnabled) {
        if (isShutdown()) {
            log.warn("Management Server received {} but is shutdown.", msg.getMsgType().toString());
            r.sendResponse(ctx, msg, CorfuMsgType.NACK.msg());
            return;
        }
        // This server has not been bootstrapped yet, ignore all requests.
        if (!checkBootstrap(msg, ctx, r)) {
            return;
        }

        log.info("Received merge segments request.");

        if (segmentCopyWorkflow != null) {
            // Workflow already started. Check status and send appropriate message to caller.
            if (segmentCopyWorkflow.isDone() && !segmentCopyWorkflow.isCompletedExceptionally()) {
                // Completed successfully.
            } else if (!segmentCopyWorkflow.isDone()) {
                // Workflow in progress.
                return;
            } else {
                // Workflow ended exceptionally. Return exception to user.
                return;
            }
        } else {
            log.info("Submitting task to replicate and merge segments");

            List<LayoutSegment> segmentList = latestLayout.getSegments();
            if (segmentList.size() < 2) {
                log.info("Not enough segments to merge.");
                return;
            }

            int collapsingSegmentIndex = segmentList.size() - 1;
            LayoutSegment collapsingSegment = segmentList.get(collapsingSegmentIndex);
            int oldSegmentIndex = segmentList.size() - 2;
            LayoutSegment oldSegment = segmentList.get(oldSegmentIndex);

            Set<String> newNodes = new HashSet<>();
            collapsingSegment.getStripes().forEach(
                    layoutStripe -> newNodes.addAll(layoutStripe.getLogServers()));
            oldSegment.getStripes().forEach(
                    layoutStripe -> newNodes.removeAll(layoutStripe.getLogServers()));

            final String[] recoveringNode = newNodes.toArray(new String[newNodes.size()]);
            // FIXME: All new nodes
            if (newNodes.size() != 1) {
                log.warn("Cannot merge segments with multiple new nodes: {}", newNodes);
                return;
            }

            newNodes.forEach(s -> recoveringNode[0] = s);

            // Segment catchup by selective hole filling.
            segmentCopyWorkflow = CompletableFuture.supplyAsync(() -> {
                segmentCatchup(collapsingSegment);
                return true;
            });

            // Segment Replication.
            segmentCopyWorkflow = segmentCopyWorkflow.thenApplyAsync(segmentCatchupResult -> {
                if (!segmentCatchupResult) {
                    // Cannot start replication if segment catchup fails.
                    log.error("segment catchup has failed. Cannot replicate segment.");
                    return segmentCatchupResult;
                }
                log.info("Starting segment replication.");

                return replicateSegment(collapsingSegment,
                        getCorfuRuntime().getRouter(recoveringNode[0])
                                .getClient(LogUnitClient.class));
            });

            // Trigger segment merge.
            segmentCopyWorkflow = segmentCopyWorkflow.thenApplyAsync(segmentReplicationResult -> {
                if (!segmentReplicationResult) {
                    // Cannot merge if segment replication fails.
                    return segmentReplicationResult;
                }
                try {
                    failureHandlerDispatcher
                            .mergeSegments((Layout) latestLayout.clone(), getCorfuRuntime());
                    safeUpdateLayout(getCorfuRuntime().getLayoutView().getLayout());
                    r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.ACK));
                } catch (OutrankedException | QuorumUnreachableException
                        | CloneNotSupportedException | LayoutModificationException e) {
                    log.error("Request to merge segments failed with exception:", e);
                }
                return true;
            });
        }
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
    }

    private CompletableFuture<Boolean> segmentCopyWorkflow;

    public boolean segmentCatchup(LayoutSegment segment) {
        log.info("Starting hole filling.");

        // TODO: Abstract this logic to replication mode specific segment merging.
        // Enabling merge segments only for chain replication.
        if (!segment.getReplicationMode().equals(Layout.ReplicationMode.CHAIN_REPLICATION)) {
            throw new UnsupportedOperationException(
                    "Segment catchup only implemented for chain replication.");
        }

        // Catchup segment for every stripe.
        for (Layout.LayoutStripe layoutStripe : segment.getStripes()) {
            List<String> logServers = layoutStripe.getLogServers();

            if (logServers.size() < 2) {
                log.info("Hole filling not required as only one log server present in stripe.");
                continue;
            }

            // Chain replication specific hole filling mechanism.
            Set<Long> headKnownAddressSet;
            Set<Long> tailKnownAddressSet;

            try {
                // Fetch known address set from head.
                headKnownAddressSet = getCorfuRuntime()
                        .getRouter(logServers.get(0))
                        .getClient(LogUnitClient.class)
                        .requestKnownAddressSet(segment.getStart(), segment.getEnd()).get();
                // Fetch known address set from tail.
                tailKnownAddressSet = getCorfuRuntime()
                        .getRouter(logServers.get(logServers.size() - 1))
                        .getClient(LogUnitClient.class)
                        .requestKnownAddressSet(segment.getStart(), segment.getEnd()).get();

            } catch (InterruptedException | ExecutionException e) {
                log.error("Unable to fetch known address set from log units, ", e);
                return false;
            }

            // Diff known address sets.
            headKnownAddressSet.removeAll(tailKnownAddressSet);
            if (headKnownAddressSet.isEmpty()) {
                log.info("Head and tail log servers in the chain have given segment replicated.");
                continue;
            }

            // Trigger reads on tail for result addresses to hole fill differences.
            getCorfuRuntime().getAddressSpaceView().read(headKnownAddressSet);
        }

        return true;
    }

    public boolean replicateSegment(LayoutSegment layoutSegment, LogUnitClient logUnitClient) {
        try {
            long logFileStartSegment =
                    layoutSegment.getStart() / StreamLogFiles.RECORDS_PER_LOG_FILE;
            long logFileEndSegment =
                    layoutSegment.getEnd() / StreamLogFiles.RECORDS_PER_LOG_FILE;
            for (long i = logFileStartSegment; i < logFileEndSegment; i++) {
                // Stream files directly as they are in the committed set.
                String filePath = serverContext.getServerConfig().get("--log-path")
                        + File.separator + "log" + File.separator + i + ".log";
                File segmentFile = new File(filePath);
                FileInputStream fis = new FileInputStream(segmentFile);
                byte[] fileBuffer = new byte[(int) segmentFile.length()];
                fis.read(fileBuffer);
                // We pay the cost of byte[] copy.
                logUnitClient.replicateSegment(
                        new FileSegmentReplicationRequest(i, fileBuffer, false)).get();
            }
            // For the last file, we need to transfer it with the inProgress flag set to true.
            String filePath = serverContext.getServerConfig().get("--log-path")
                    + File.separator + "log" + File.separator + logFileEndSegment + ".log";
            File segmentFile = new File(filePath);
            FileInputStream fis = new FileInputStream(segmentFile);
            byte[] fileBuffer = new byte[(int) segmentFile.length()];
            fis.read(fileBuffer);
            // We pay the cost of byte[] copy.
            logUnitClient.replicateSegment(
                    new FileSegmentReplicationRequest(logFileEndSegment, fileBuffer, true)).get();

        } catch (InterruptedException | ExecutionException | IOException e) {
            e.printStackTrace();
            log.error("ChainReplication: Segment replication failed, ", e);
            return false;
        }
        return true;
    }

    /**
     * Handles the heartbeat request.
     * It accumulates the metrics required to build
     * and send the response(NodeMetrics).
     *
     * @param msg corfu message containing HEARTBEAT_REQUEST
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    @ServerHandler(type = CorfuMsgType.HEARTBEAT_REQUEST, opTimer = metricsPrefix
            + "heartbeat-request")
    public void handleHearbeatRequest(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r,
                                      boolean isMetricsEnabled) {
        // Currently builds a default instance of the model.
        // TODO: Collect metrics from Layout, Sequencer and LogUnit Servers.
        NodeMetrics nodeMetrics = NodeMetrics.getDefaultInstance();
        r.sendResponse(ctx, msg, new CorfuPayloadMsg<>(CorfuMsgType.HEARTBEAT_RESPONSE,
                nodeMetrics.toByteArray()));
    }

    /**
     * Returns a connected instance of the CorfuRuntime.
     *
     * @return A connected instance of runtime.
     */
    public synchronized CorfuRuntime getCorfuRuntime() {

        if (corfuRuntime == null) {
            corfuRuntime = new CorfuRuntime();
            if ((Boolean) opts.get("--enable-tls")) {
                corfuRuntime.enableTls((String) opts.get("--keystore"),
                        (String) opts.get("--keystore-password-file"),
                        (String) opts.get("--truststore"),
                        (String) opts.get("--truststore-password-file"));
                if ((Boolean) opts.get("--enable-sasl-plain-text-auth")) {
                    corfuRuntime.enableSaslPlainText(
                            (String) opts.get("--sasl-plain-text-username-file"),
                            (String) opts.get("--sasl-plain-text-password-file"));
                }
            }
            // Runtime can be set up either using the layout or the bootstrapEndpoint address.
            if (latestLayout != null) {
                latestLayout.getLayoutServers().forEach(ls -> corfuRuntime.addLayoutServer(ls));
            } else {
                corfuRuntime.addLayoutServer(bootstrapEndpoint);
            }
            corfuRuntime.connect();
            log.info("Corfu Runtime connected successfully");
        }
        return corfuRuntime;
    }

    /**
     * Gets the address of this endpoint.
     *
     * @return localEndpoint address
     */
    private String getLocalEndpoint() {
        return this.opts.get("--address") + ":" + this.opts.get("<port>");
    }

    /**
     * Schedules the failure detector task only if the previous task is completed.
     */
    private synchronized void failureDetectorScheduler() {
        if (latestLayout == null && bootstrapEndpoint == null) {
            log.warn("Management Server waiting to be bootstrapped");
            return;
        }
        // Recover if flag is false
        if (!recovered) {
            recovered = recover();
            if (!recovered) {
                log.error("failureDetectorScheduler: Recovery failed. Retrying.");
                return;
            }
            // If recovery succeeds, reconfiguration was successful.
            sequencerBootstrappedFuture.complete(true);
        } else if (!sequencerBootstrappedFuture.isDone()) {
            // This should be invoked only once in case of a clean startup (not recovery).
            bootstrapPrimarySequencerServer();
        }

        if (failureDetectorFuture == null || failureDetectorFuture.isDone()) {
            failureDetectorFuture = failureDetectorService.submit(this::failureDetectorTask);
        } else {
            log.debug("Cannot initiate new polling task. Polling in progress.");
        }
    }

    /**
     * This contains the complete failure detection and handling mechanism.
     *
     * <p>It first checks whether the current node is bootstrapped.
     * If not, it continues checking in intervals of 1 second.
     * If yes, it sets up the corfuRuntime and continues execution
     * of the policy.
     *
     * <p>It executes the policy which detects and reports failures.
     * Once detected, it triggers the trigger handler which takes care of
     * dispatching the appropriate handler.
     *
     * <p>Currently executing the periodicPollPolicy.
     * It executes the the polling at an interval of every 1 second.
     * After every poll it checks for any failures detected.
     */
    private void failureDetectorTask() {

        CorfuRuntime corfuRuntime = getCorfuRuntime();
        corfuRuntime.invalidateLayout();

        // Fetch the latest layout view through the runtime.
        safeUpdateLayout(corfuRuntime.getLayoutView().getLayout());

        // Execute the failure detection policy once.
        failureDetectorPolicy.executePolicy(latestLayout, corfuRuntime);

        // Get the server status from the policy and check for failures.
        PollReport pollReport = failureDetectorPolicy.getServerStatus();

        // Analyze the poll report and trigger failure handler if needed.
        analyzePollReportAndTriggerHandler(pollReport);

    }

    /**
     * Analyzes the poll report and triggers the failure handler if status change
     * of node detected.
     *
     * @param pollReport Poll report obtained from failure detection policy.
     */
    private void analyzePollReportAndTriggerHandler(PollReport pollReport) {

        // Check if handler has been initiated.
        if (!startFailureHandler) {
            log.debug("Failure Handler not yet initiated: {}", pollReport.toString());
            return;
        }

        final ManagementClient localManagementClient = corfuRuntime.getRouter(getLocalEndpoint())
                .getClient(ManagementClient.class);

        try {
            if (!pollReport.getIsFailurePresent()) {
                // CASE 1:
                // No Failures detected by polling policy.
                // We can check if we have unresponsive servers marked in the layout and
                // un-mark them as they respond to polling now.
                if (!latestLayout.getUnresponsiveServers().isEmpty()) {
                    log.info("Received response from unresponsive server");
                    localManagementClient.handleFailure(pollReport.getFailingNodes()).get();
                    return;
                }
                log.debug("No failures present.");

            } else if (!pollReport.getFailingNodes().isEmpty() && !latestLayout
                    .getUnresponsiveServers().isEmpty()) {
                // CASE 2:
                // Failures detected - unresponsive servers.
                // We check if these servers are the same set of servers which are marked as
                // unresponsive in the layout. If yes take no action. Else trigger handler.
                log.info("Failures detected. Failed nodes : {}", pollReport.toString());
                // Check if this failure has already been recognized.
                //TODO: Does not handle the un-marking case where markedSet is a superset of
                // pollFailures.
                for (String failedServer : pollReport.getFailingNodes()) {
                    if (!latestLayout.getUnresponsiveServers().contains(failedServer)) {
                        localManagementClient.handleFailure(pollReport.getFailingNodes()).get();
                        return;
                    }
                }
                log.debug("Failure already taken care of.");

            } else {
                // CASE 3:
                // Failures detected but not marked in the layout or
                // some servers have been partially sealed to new epoch or stuck on
                // the previous epoch.
                localManagementClient.handleFailure(pollReport.getFailingNodes()).get();
                // TODO: Only re-sealing needed if server stuck on a previous epoch.
            }

        } catch (Exception e) {
            log.error("Exception invoking failure handler : {}", e);
        }
    }

    /**
     * Management Server shutdown:
     * Shuts down the fault detector service.
     */
    public void shutdown() {
        super.shutdown();
        // Shutting the fault detector.
        failureDetectorService.shutdownNow();

        // Shut down the Corfu Runtime.
        if (corfuRuntime != null) {
            corfuRuntime.shutdown();
        }

        try {
            failureDetectorService.awaitTermination(serverContext.SHUTDOWN_TIMER.getSeconds(),
                    TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            log.debug("failureDetectorService awaitTermination interrupted : {}", ie);
        }
        log.info("Management Server shutting down.");
    }
}
