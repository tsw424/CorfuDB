package org.corfudb.runtime.view;

import lombok.Data;
import lombok.Getter;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.infrastructure.BaseServer;
import org.corfudb.infrastructure.IServerRouter;
import org.corfudb.infrastructure.LayoutServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.ManagementServer;
import org.corfudb.infrastructure.SequencerServer;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.infrastructure.TestServerRouter;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.LayoutBootstrapRequest;
import org.corfudb.protocols.wireprotocol.SequencerTailsRecoveryMsg;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.clients.SequencerClient;
import org.corfudb.runtime.clients.TestClientRouter;
import org.corfudb.runtime.clients.TestRule;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * This class serves as a base class for most higher-level Corfu unit tests
 * providing several helper functions to reduce boilerplate code.
 *
 * For most tests, a CorfuRuntime can be obtained by calling getDefaultRuntime().
 * This instantiates a single-node in-memory Corfu server at port 9000, already
 * bootstrapped. If getDefaultRuntime() is not called, then no servers are
 * started.
 *
 * For all other tests, servers can be started using the addServer(port, options)
 * function. The bootstrapAllServers(layout) function can be used to
 * bootstrap the servers with a specific layout. These servers can be referred
 * to by a CorfuRuntime using the "test:<port number>" convention. For example,
 * calling new CorfuRuntime("test:9000"); will connect a CorfuRuntime to the
 * test server at port 9000.
 *
 * To access servers, call the getLogUnit(port), getLayoutServer(port) and
 * getSequencer(port). This allows access to the server class public fields
 * and methods.
 *
 * In addition to simulating Corfu servers, this class also permits installing
 * special rules, which can be used to simulate failures or reorder messages.
 * To install, use the addClientRule(testRule) and addServerRule(testRule)
 * methods.
 *
 * Created by mwei on 12/22/15.
 */
public abstract class AbstractViewTest extends AbstractCorfuTest {

    /** The runtime generated by default, by getDefaultRuntime(). */
    @Getter
    CorfuRuntime runtime;

    /** A map of the current test servers, by endpoint name */
    final Map<String, TestServer> testServerMap = new ConcurrentHashMap<>();

    /** A map of maps to endpoint->routers, mapped for each runtime instance captured */
    final Map<CorfuRuntime, Map<String, TestClientRouter>>
            runtimeRouterMap = new ConcurrentHashMap<>();

    /** Initialize the AbstractViewTest. */
    public AbstractViewTest() {
        // Force all new CorfuRuntimes to override the getRouterFn
        CorfuRuntime.overrideGetRouterFunction = this::getRouterFunction;
        runtime = new CorfuRuntime(getDefaultEndpoint());
        // Default number of times to read before hole filling to 0
        // (most aggressive, to surface concurrency issues).
        runtime.getParameters().setHoleFillRetry(0);
    }

    /** Function for obtaining a router, given a runtime and an endpoint.
     *
     * @param runtime       The CorfuRuntime to obtain a router for.
     * @param endpoint      An endpoint string for the router.
     * @return
     */
    private IClientRouter getRouterFunction(CorfuRuntime runtime, String endpoint) {
        runtimeRouterMap.putIfAbsent(runtime, new ConcurrentHashMap<>());
        if (!endpoint.startsWith("test:")) {
            throw new RuntimeException("Unsupported endpoint in test: " + endpoint);
        }
        return runtimeRouterMap.get(runtime).computeIfAbsent(endpoint,
                x -> {
                    TestClientRouter tcn =
                            new TestClientRouter(testServerMap.get(endpoint).getServerRouter());
                    tcn.addClient(new BaseClient())
                            .addClient(new SequencerClient())
                            .addClient(new LayoutClient())
                            .addClient(new LogUnitClient())
                            .addClient(new ManagementClient());
                    return tcn;
                }
        );
    }

    /**
     * Before each test, reset the tests.
     */
    @Before
    public void resetTests() {
        testServerMap.clear();
        runtime.parseConfigurationString(getDefaultConfigurationString());
       //         .setCacheDisabled(true); // Disable cache during unit tests to fully stress the system.
        runtime.getAddressSpaceView().resetCaches();
    }

    @After
    public void cleanupBuffers() {
        testServerMap.values().stream().forEach(x -> {
            x.getLogUnitServer().shutdown();
            x.getManagementServer().shutdown();
        });
        // Abort any active transactions...
        while (runtime.getObjectsView().TXActive()) {
            runtime.getObjectsView().TXAbort();
        }
    }

    /** Add a server at a specific port, using the given configuration options.
     *
     * @param port      The port to use.
     * @param config    The configuration to use for the server.
     */
    public void addServer(int port, Map<String, Object> config) {
        addServer(port, new ServerContext(config, new TestServerRouter(port)));
    }

    /**
     * Add a server to a specific port, using the given ServerContext.
     * @param port
     * @param serverContext
     */
    public void addServer(int port, ServerContext serverContext) {
        new TestServer(serverContext).addToTest(port, this);
    }

    /** Add a default, in-memory unbootstrapped server at a specific port.
     *
     * @param port      The port to use.
     */
    public void addServer(int port) {
        new TestServer(new ServerContextBuilder().setSingle(false).setServerRouter(new TestServerRouter(port)).setPort(port).build()).addToTest(port, this);
    }


    /** Add a default, in-memory bootstrapped single node server at a specific port.
     *
     * @param port      The port to use.
     */
    public void addSingleServer(int port) {
        new TestServer(port).addToTest(port, this);
    }


    /** Get a instance of a test server, which provides access to the underlying components and server router.
     *
     * @param port      The port of the test server to retrieve.
     * @return          A test server instance.
     */
    public TestServer getServer(int port) {
        return testServerMap.get("test:" + port);
    }

    /** Get a instance of a logging unit, given a port.
     *
     * @param port      The port of the logging unit to retrieve.
     * @return          A logging unit instance.
     */
    public LogUnitServer getLogUnit(int port) {
        return getServer(port).getLogUnitServer();
    }

    /** Get a instance of a sequencer, given a port.
     *
     * @param port      The port of the sequencer to retrieve.
     * @return          A sequencer instance.
     */
    public SequencerServer getSequencer(int port) {
        return getServer(port).getSequencerServer();
    }

    /** Get a instance of a layout server, given a port.
     *
     * @param port      The port of the layout server to retrieve.
     * @return          A layout server instance.
     */
    public LayoutServer getLayoutServer(int port) {
        return getServer(port).getLayoutServer();
    }

    /**
     * Get an instance of the management server, given a port
     *
     * @param port The port of the management server to retrieve
     * @return A management server instance.
     */
    public ManagementServer getManagementServer(int port) {
        return getServer(port).getManagementServer();
    }

    /** Get a instance of base server, given a port.
     *
     * @param port      The port of the base server to retrieve.
     * @return          A base server instance.
     */
    public BaseServer getBaseServer(int port) {
        return getServer(port).getBaseServer();
    }

    public IServerRouter getServerRouter(int port) {
        return getServer(port).getServerRouter();
    }

    /** Bootstraps all servers with a particular layout.
     *
     * @param l         The layout to bootstrap all servers with.
     */
    public void bootstrapAllServers(Layout l)
    {
        testServerMap.entrySet().parallelStream()
                .forEach(e -> {
                    e.getValue().layoutServer
                            .handleMessage(CorfuMsgType.LAYOUT_BOOTSTRAP.payloadMsg(new LayoutBootstrapRequest(l)),
                                    null, e.getValue().serverRouter);
                    e.getValue().managementServer
                            .handleMessage(CorfuMsgType.MANAGEMENT_BOOTSTRAP_REQUEST.payloadMsg(l),
                                    null, e.getValue().serverRouter);
                });
        TestServer primarySequencerNode = testServerMap.get(l.getSequencers().get(0));
        primarySequencerNode.sequencerServer
                .handleMessage(CorfuMsgType.BOOTSTRAP_SEQUENCER.payloadMsg(new SequencerTailsRecoveryMsg(0L,
                        Collections.EMPTY_MAP, l.getEpoch())), null, primarySequencerNode.serverRouter);
    }

    /** Get a default CorfuRuntime. The default CorfuRuntime is connected to a single-node
     * in-memory server at port 9000.
     * @return  A default CorfuRuntime
     */
    public CorfuRuntime getDefaultRuntime() {
        if (!testServerMap.containsKey(getEndpoint(SERVERS.PORT_0))) {
            addSingleServer(SERVERS.PORT_0);
        }
        return getRuntime().connect();
    }

    /**
     * Create a runtime based on the provided layout.
     * @param l
     * @return
     */
    public CorfuRuntime getRuntime(Layout l) {
        String cfg = l.getLayoutServers().stream().collect(Collectors.joining(","));
        return new CorfuRuntime(cfg);
    }

    /** Clear installed rules for the default runtime.
     */
    public void clearClientRules() {
        clearClientRules(getRuntime());
    }

    /** Clear installed rules for a given runtime.
     *
     * @param r     The runtime to clear rules for.
     */
    public void clearClientRules(CorfuRuntime r) {
        runtimeRouterMap.get(r).values().forEach(x -> x.rules.clear());
    }

    /** Add a rule for the default runtime.
     *
     * @param rule  The rule to install
     */
    public void addClientRule(TestRule rule) {
        addClientRule(getRuntime(), rule);
    }

    /** Add a rule for a particular runtime.
     *
     * @param r     The runtime to install the rule to
     * @param rule  The rule to install.
     */
    public void addClientRule(CorfuRuntime r, TestRule rule) {
        runtimeRouterMap.get(r).values().forEach(x -> x.rules.add(rule));
    }

    /** Add a rule to a particular router in a particular runtime.
     *
     * @param r                     The runtime to install the rule to
     * @param clientRouterEndpoint  The Client router endpoint to install the rule to
     * @param rule                  The rule to install.
     */
    public void addClientRule(CorfuRuntime r, String clientRouterEndpoint, TestRule rule) {
        runtimeRouterMap.get(r).get(clientRouterEndpoint).rules.add(rule);
    }

    /** Clear rules for a particular server.
     *
     * @param port  The port of the server to clear rules for.
     */
    public void clearServerRules(int port) {
        getServer(port).getServerRouter().rules.clear();
    }

    /** Install a rule to a particular server.
     *
     * @param port  The port of the server to install the rule to.
     * @param rule  The rule to install.
     */
    public void addServerRule(int port, TestRule rule) {
        getServer(port).getServerRouter().rules.add(rule);
    }

    /** The configuration string used for the default runtime.
     *
     * @return  The configuration string used for the default runtime.
     */
    public String getDefaultConfigurationString() {
        return getDefaultEndpoint();
    }

    /** The default endpoint (single server) used for the default runtime.
     *
     * @return  Returns the default endpoint.
     */
    public String getDefaultEndpoint() {
        return getEndpoint(SERVERS.PORT_0);
    }

    /** Get the endpoint string, given a port number.
     *
     * @param port  The port number to get an endpoint string for.
     * @return      The endpoint string.
     */
    public String getEndpoint(int port) {
        return "test:" + port;
    }


    // Private

    /**
     * This class holds instances of servers used for test.
     */
    @Data
    private static class TestServer {
        ServerContext serverContext;
        BaseServer baseServer;
        SequencerServer sequencerServer;
        LayoutServer layoutServer;
        LogUnitServer logUnitServer;
        ManagementServer managementServer;
        IServerRouter serverRouter;
        int port;

        TestServer(Map<String, Object> optsMap)
        {
            this(new ServerContext(optsMap, new TestServerRouter()));
        }

        TestServer(ServerContext serverContext) {
            this.serverContext = serverContext;
            this.serverRouter = serverContext.getServerRouter();
            this.baseServer = new BaseServer();
            this.sequencerServer = new SequencerServer(serverContext);
            this.layoutServer = new LayoutServer(serverContext);
            this.logUnitServer = new LogUnitServer(serverContext);
            this.managementServer = new ManagementServer(serverContext);

            this.serverRouter.addServer(baseServer);
            this.serverRouter.addServer(sequencerServer);
            this.serverRouter.addServer(layoutServer);
            this.serverRouter.addServer(logUnitServer);
            this.serverRouter.addServer(managementServer);
        }

        TestServer(int port)
        {
            this(ServerContextBuilder.defaultContext(port).getServerConfig());
        }

        void addToTest(int port, AbstractViewTest test) {

            if (test.testServerMap.putIfAbsent("test:" + port, this) != null) {
                throw new RuntimeException("Server already registered at port " + port);
            }

        }

        public TestServerRouter getServerRouter() {
            return (TestServerRouter) this.serverRouter;
        }
    }
}
