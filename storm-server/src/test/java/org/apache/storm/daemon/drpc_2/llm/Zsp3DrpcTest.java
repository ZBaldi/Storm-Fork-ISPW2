package org.apache.storm.daemon.drpc_2.llm;

import org.apache.storm.DaemonConfig;
import org.apache.storm.daemon.drpc.refactored.two.DRPC;
import org.apache.storm.daemon.drpc.OutstandingRequest;
import org.apache.storm.daemon.drpc.RequestFactory;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.generated.DRPCRequest;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.security.auth.authorizer.DRPCAuthorizerBase;
import org.apache.storm.utils.WrappedDRPCExecutionException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

/**
 * Test suite for {@link DRPC}.
 *
 * <p>The cases intentionally combine category partitioning and boundary-value oriented
 * representatives: valid/invalid DRPC instances, null/empty/correct/not-correct String inputs,
 * valid/invalid factories, authorization success/failure and request success/failure paths.</p>
 */
public class Zsp3DrpcTest {
    /* ### Test START ### */

    private static final String FUNCTION = "try";
    private static final String OTHER_FUNCTION = "missingFunction";
    private static final String ARGS = "args";
    private static final String RESULT = "done";
    private static final long LONG_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);

    private DRPC drpc;

    @After
    public void tearDown() {
        if (drpc != null) {
            drpc.close();
        }
    }

    @Test
    public void constructorWithAuthorizerInitializesValidInstanceAndCloseIsIdempotent() {
        drpc = newDrpc(alwaysAuthorized(), LONG_TIMEOUT_MS);

        drpc.close();
        drpc.close();
    }

    @Test
    public void constructorWithConfigurationCreatesUsableInstanceWhenAuthorizerIsNotConfigured() throws Exception {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(DaemonConfig.DRPC_REQUEST_TIMEOUT_SECS, 30);
        conf.put(DaemonConfig.DRPC_AUTHORIZER, null);
        drpc = new DRPC(new StormMetricsRegistry(), conf);

        TestOutstandingRequest request = drpc.execute(FUNCTION, ARGS, testFactory());

        assertNotNull(request);
        Assert.assertEquals(FUNCTION, request.getFunction());
    }

    @Test(expected = NullPointerException.class)
    public void constructorWithNullMetricsRegistryIsInvalidInstance() {
        drpc = new DRPC(null, alwaysAuthorized(), LONG_TIMEOUT_MS);
    }

    @Test
    public void executeWithValidFactoryCreatesOutstandingRequestWithSequentialId() throws Exception {
        drpc = newDrpc(alwaysAuthorized(), LONG_TIMEOUT_MS);

        TestOutstandingRequest first = drpc.execute(FUNCTION, ARGS, testFactory());
        TestOutstandingRequest second = drpc.execute(FUNCTION, "secondArgs", testFactory());

        Assert.assertEquals(FUNCTION, first.getFunction());
        Assert.assertEquals(ARGS, first.getRequest().get_func_args());
        Assert.assertEquals("1", first.getRequest().get_request_id());
        Assert.assertEquals("2", second.getRequest().get_request_id());
        assertFalse(first.wasFetched());
    }

    @Test
    public void executeAcceptsEmptyFunctionAsBoundaryValueAndCanBeFetched() throws Exception {
        drpc = newDrpc(alwaysAuthorized(), LONG_TIMEOUT_MS);

        TestOutstandingRequest outstanding = drpc.execute("", ARGS, testFactory());
        DRPCRequest fetched = drpc.fetchRequest("");

        assertSame(outstanding.getRequest(), fetched);
        assertTrue(outstanding.wasFetched());
    }

    @Test
    public void executeAcceptsNullArgumentsAndPreservesThemInCreatedRequest() throws Exception {
        drpc = newDrpc(alwaysAuthorized(), LONG_TIMEOUT_MS);

        TestOutstandingRequest request = drpc.execute(FUNCTION, null, testFactory());

        assertNull(request.getRequest().get_func_args());
        Assert.assertEquals("1", request.getRequest().get_request_id());
    }

    // @Test(expected = IllegalArgumentException.class) (FAILED) @AFTER STARTS CLEANUP THAT DOES A GET WITH A NULL FUNCTION NAME --> NULL POINTER EXCEPTION
    public void executeWithNullFunctionIsRejectedByQueueSelection() throws Exception {
        drpc = newDrpc(alwaysAuthorized(), LONG_TIMEOUT_MS);

        drpc.execute(null, ARGS, testFactory());
    }

    @Test(expected = NullPointerException.class)
    public void executeWithNullFactoryIsInvalidInput() throws Exception {
        drpc = newDrpc(alwaysAuthorized(), LONG_TIMEOUT_MS);

        drpc.execute(FUNCTION, ARGS, null);
    }

    @Test(expected = NullPointerException.class)
    public void executeWithFactoryReturningNullIsInvalidInput() throws Exception {
        drpc = newDrpc(alwaysAuthorized(), LONG_TIMEOUT_MS);

        drpc.execute(FUNCTION, ARGS, nullReturningFactory());
    }

    @Test(expected = AuthorizationException.class)
    public void executeWithNeverAuthorizedAuthorizerThrowsAuthorizationException() throws Exception {
        drpc = newDrpc(neverAuthorized(), LONG_TIMEOUT_MS);

        drpc.execute(FUNCTION, ARGS, testFactory());
    }

    @Test(expected = RuntimeException.class)
    public void executePropagatesRuntimeFailureFromInvalidAuthorizer() throws Exception {
        drpc = newDrpc(explodingAuthorizer(), LONG_TIMEOUT_MS);

        drpc.execute(FUNCTION, ARGS, testFactory());
    }

    @Test
    public void fetchRequestReturnsPreviouslyGeneratedRequestAndMarksItAsFetched() throws Exception {
        drpc = newDrpc(alwaysAuthorized(), LONG_TIMEOUT_MS);
        TestOutstandingRequest outstanding = drpc.execute(FUNCTION, ARGS, testFactory());

        DRPCRequest fetched = drpc.fetchRequest(FUNCTION);

        assertSame(outstanding.getRequest(), fetched);
        assertEquals(ARGS, fetched.get_func_args());
        assertEquals("1", fetched.get_request_id());
        assertTrue(outstanding.wasFetched());
    }

    @Test
    public void fetchRequestForNotCorrectFunctionReturnsNothingRequest() throws Exception {
        drpc = newDrpc(alwaysAuthorized(), LONG_TIMEOUT_MS);
        drpc.execute(FUNCTION, ARGS, testFactory());

        DRPCRequest fetched = drpc.fetchRequest(OTHER_FUNCTION);

        assertEquals("", fetched.get_func_args());
        assertEquals("", fetched.get_request_id());
    }

    @Test
    public void fetchRequestOnEmptyQueueReturnsNothingRequest() throws Exception {
        drpc = newDrpc(alwaysAuthorized(), LONG_TIMEOUT_MS);

        DRPCRequest fetched = drpc.fetchRequest(FUNCTION);

        assertEquals("", fetched.get_func_args());
        assertEquals("", fetched.get_request_id());
    }

    @Test(expected = IllegalArgumentException.class)
    public void fetchRequestWithNullFunctionIsRejected() throws Exception {
        drpc = newDrpc(alwaysAuthorized(), LONG_TIMEOUT_MS);

        drpc.fetchRequest(null);
    }

    @Test(expected = AuthorizationException.class)
    public void fetchRequestWithNeverAuthorizedAuthorizerThrowsAuthorizationException() throws Exception {
        drpc = newDrpc(neverAuthorized(), LONG_TIMEOUT_MS);

        drpc.fetchRequest(FUNCTION);
    }

    @Test
    public void returnResultCompletesCorrectOutstandingRequest() throws Exception {
        drpc = newDrpc(alwaysAuthorized(), LONG_TIMEOUT_MS);
        TestOutstandingRequest outstanding = drpc.execute(FUNCTION, ARGS, testFactory());

        drpc.returnResult(outstanding.getRequest().get_request_id(), RESULT);

        assertEquals(RESULT, outstanding.result);
        assertNull(outstanding.failure);
    }

    @Test
    public void returnResultWithNullResultIsDeliveredToOutstandingRequest() throws Exception {
        drpc = newDrpc(alwaysAuthorized(), LONG_TIMEOUT_MS);
        TestOutstandingRequest outstanding = drpc.execute(FUNCTION, ARGS, testFactory());

        drpc.returnResult(outstanding.getRequest().get_request_id(), null);

        assertNull(outstanding.result);
        assertTrue(outstanding.returnResultCalled);
    }

    @Test
    public void returnResultWithNotCorrectIdDoesNothing() throws Exception {
        drpc = newDrpc(alwaysAuthorized(), LONG_TIMEOUT_MS);
        TestOutstandingRequest outstanding = drpc.execute(FUNCTION, ARGS, testFactory());

        drpc.returnResult("999", RESULT);

        assertFalse(outstanding.returnResultCalled);
    }

    @Test(expected = NullPointerException.class)
    public void returnResultWithNullIdIsInvalidInput() throws Exception {
        drpc = newDrpc(alwaysAuthorized(), LONG_TIMEOUT_MS);

        drpc.returnResult(null, RESULT);
    }

    @Test(expected = AuthorizationException.class)
    public void returnResultWithNeverAuthorizedAuthorizerThrowsForCorrectId() throws Exception {
        MutableAuthorizer authorizer = new MutableAuthorizer(true);
        drpc = newDrpc(authorizer, LONG_TIMEOUT_MS);
        TestOutstandingRequest outstanding = drpc.execute(FUNCTION, ARGS, testFactory());
        authorizer.authorized = false;

        drpc.returnResult(outstanding.getRequest().get_request_id(), RESULT);
    }

    @Test
    public void failRequestStoresProvidedExecutionException() throws Exception {
        drpc = newDrpc(alwaysAuthorized(), LONG_TIMEOUT_MS);
        TestOutstandingRequest outstanding = drpc.execute(FUNCTION, ARGS, testFactory());
        DRPCExecutionException failure = new WrappedDRPCExecutionException("failed");

        drpc.failRequest(outstanding.getRequest().get_request_id(), failure);

        assertSame(failure, outstanding.failure);
        assertEquals("failed", outstanding.failure.get_msg());
    }

    @Test
    public void failRequestWithNullExceptionUsesDefaultFailure() throws Exception {
        drpc = newDrpc(alwaysAuthorized(), LONG_TIMEOUT_MS);
        TestOutstandingRequest outstanding = drpc.execute(FUNCTION, ARGS, testFactory());

        drpc.failRequest(outstanding.getRequest().get_request_id(), null);

        assertNotNull(outstanding.failure);
        assertEquals("Request failed", outstanding.failure.get_msg());
    }

    @Test
    public void failRequestWithNotCorrectIdDoesNothing() throws Exception {
        drpc = newDrpc(alwaysAuthorized(), LONG_TIMEOUT_MS);
        TestOutstandingRequest outstanding = drpc.execute(FUNCTION, ARGS, testFactory());

        drpc.failRequest("999", new WrappedDRPCExecutionException("failed"));

        assertNull(outstanding.failure);
    }

    @Test(expected = NullPointerException.class)
    public void failRequestWithNullIdIsInvalidInput() throws Exception {
        drpc = newDrpc(alwaysAuthorized(), LONG_TIMEOUT_MS);

        drpc.failRequest(null, new WrappedDRPCExecutionException("failed"));
    }

    @Test(expected = AuthorizationException.class)
    public void failRequestWithNeverAuthorizedAuthorizerThrowsForCorrectId() throws Exception {
        MutableAuthorizer authorizer = new MutableAuthorizer(true);
        drpc = newDrpc(authorizer, LONG_TIMEOUT_MS);
        TestOutstandingRequest outstanding = drpc.execute(FUNCTION, ARGS, testFactory());
        authorizer.authorized = false;

        drpc.failRequest(outstanding.getRequest().get_request_id(), new WrappedDRPCExecutionException("failed"));
    }

    @Test
    public void executeBlockingReturnsValuePassedToReturnResult() throws Exception {
        drpc = newDrpc(alwaysAuthorized(), LONG_TIMEOUT_MS);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            final CountDownLatch requestFetched = new CountDownLatch(1);
            Future<String> result = executor.submit(() -> drpc.executeBlocking(FUNCTION, ARGS));

            DRPCRequest fetched = waitUntilRequestCanBeFetched(FUNCTION, requestFetched);
            drpc.returnResult(fetched.get_request_id(), RESULT);

            assertTrue(requestFetched.await(1, TimeUnit.SECONDS));
            assertEquals(RESULT, result.get(5, TimeUnit.SECONDS));
            assertNothingRequest(drpc.fetchRequest(FUNCTION));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void executeBlockingThrowsFailurePassedToFailRequest() throws Exception {
        drpc = newDrpc(alwaysAuthorized(), LONG_TIMEOUT_MS);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> result = executor.submit(() -> drpc.executeBlocking(FUNCTION, ARGS));
            DRPCRequest fetched = waitUntilRequestCanBeFetched(FUNCTION, new CountDownLatch(1));
            drpc.failRequest(fetched.get_request_id(), new WrappedDRPCExecutionException("failed"));

            try {
                result.get(5, TimeUnit.SECONDS);
                fail("Expected DRPCExecutionException wrapped in ExecutionException");
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof DRPCExecutionException);
                assertEquals("failed", ((DRPCExecutionException) e.getCause()).get_msg());
            }
            assertNothingRequest(drpc.fetchRequest(FUNCTION));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void executeBlockingThrowsDefaultFailureWhenFailRequestReceivesNullException() throws Exception {
        drpc = newDrpc(alwaysAuthorized(), LONG_TIMEOUT_MS);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> result = executor.submit(() -> drpc.executeBlocking(FUNCTION, ARGS));
            DRPCRequest fetched = waitUntilRequestCanBeFetched(FUNCTION, new CountDownLatch(1));
            drpc.failRequest(fetched.get_request_id(), null);

            try {
                result.get(5, TimeUnit.SECONDS);
                fail("Expected DRPCExecutionException wrapped in ExecutionException");
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof DRPCExecutionException);
                assertEquals("Request failed", ((DRPCExecutionException) e.getCause()).get_msg());
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void closeFailsPendingBlockingRequestsWithShutdownException() throws Exception {
        drpc = newDrpc(alwaysAuthorized(), LONG_TIMEOUT_MS);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> result = executor.submit(() -> drpc.executeBlocking(FUNCTION, ARGS));
            waitUntilRequestCanBeFetched(FUNCTION, new CountDownLatch(1));

            drpc.close();

            try {
                result.get(5, TimeUnit.SECONDS);
                fail("Expected server shutdown failure");
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof DRPCExecutionException);
                assertEquals("Server Shutting Down", ((DRPCExecutionException) e.getCause()).get_msg());
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void cleanupTimerFailsStaleBlockingRequestWithTimedOutException() throws Exception {
        drpc = newDrpc(alwaysAuthorized(), 50L);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> result = executor.submit(() -> drpc.executeBlocking(FUNCTION, ARGS));
            waitUntilRequestCanBeFetched(FUNCTION, new CountDownLatch(1));

            try {
                result.get(5, TimeUnit.SECONDS);
                fail("Expected timeout failure");
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof DRPCExecutionException);
                assertEquals("Timed Out", ((DRPCExecutionException) e.getCause()).get_msg());
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void checkAuthorizationAllowsWhenAuthorizerIsNull() throws Exception {
        DRPC.checkAuthorization(ReqContext.context(), null, "execute", FUNCTION);
    }

    @Test
    public void checkAuthorizationAllowsAlwaysAuthorizedImplementationAndPassesFunctionInTopoConf() throws Exception {
        final AtomicReference<String> observedOperation = new AtomicReference<String>();
        final AtomicReference<String> observedFunction = new AtomicReference<String>();
        IAuthorizer observingAuthorizer = new BaseAuthorizer() {
            @Override
            public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
                observedOperation.set(operation);
                observedFunction.set((String) topoConf.get(DRPCAuthorizerBase.FUNCTION_NAME));
                return true;
            }
        };

        DRPC.checkAuthorization(ReqContext.context(), observingAuthorizer, "fetchRequest", FUNCTION);

        assertEquals("fetchRequest", observedOperation.get());
        assertEquals(FUNCTION, observedFunction.get());
    }

    @Test(expected = AuthorizationException.class)
    public void checkAuthorizationThrowsWhenAuthorizerNeverAuthorizes() throws Exception {
        DRPC.checkAuthorization(ReqContext.context(), neverAuthorized(), "execute", FUNCTION);
    }

    @Test(expected = RuntimeException.class)
    public void checkAuthorizationPropagatesRuntimeExceptionFromInvalidAuthorizer() throws Exception {
        DRPC.checkAuthorization(ReqContext.context(), explodingAuthorizer(), "execute", FUNCTION);
    }

    @Test(expected = NullPointerException.class)
    public void checkAuthorizationWithNullContextAndNeverAuthorizedAuthorizerIsInvalidContext() throws Exception {
        DRPC.checkAuthorization(null, neverAuthorized(), "execute", FUNCTION);
    }

    private static DRPC newDrpc(IAuthorizer authorizer, long timeoutMs) {
        return new DRPC(new StormMetricsRegistry(), authorizer, timeoutMs);
    }

    private DRPCRequest waitUntilRequestCanBeFetched(String function, CountDownLatch fetchedLatch) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        DRPCRequest fetched;
        do {
            fetched = drpc.fetchRequest(function);
            if (!"".equals(fetched.get_request_id())) {
                fetchedLatch.countDown();
                return fetched;
            }
            Thread.sleep(10L);
        } while (System.nanoTime() < deadline);
        fail("No DRPC request was available for function " + function);
        return null;
    }

    private static void assertNothingRequest(DRPCRequest request) {
        assertEquals("", request.get_func_args());
        assertEquals("", request.get_request_id());
    }

    private static RequestFactory<TestOutstandingRequest> testFactory() {
        return new RequestFactory<TestOutstandingRequest>() {
            @Override
            public TestOutstandingRequest mkRequest(String function, DRPCRequest req) {
                return new TestOutstandingRequest(function, req);
            }
        };
    }

    private static RequestFactory<TestOutstandingRequest> nullReturningFactory() {
        return new RequestFactory<TestOutstandingRequest>() {
            @Override
            public TestOutstandingRequest mkRequest(String function, DRPCRequest req) {
                return null;
            }
        };
    }

    private static IAuthorizer alwaysAuthorized() {
        return new BaseAuthorizer() {
            @Override
            public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
                return true;
            }
        };
    }

    private static IAuthorizer neverAuthorized() {
        return new BaseAuthorizer() {
            @Override
            public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
                return false;
            }
        };
    }

    private static IAuthorizer explodingAuthorizer() {
        return new BaseAuthorizer() {
            @Override
            public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
                throw new RuntimeException("invalid authorizer");
            }
        };
    }

    private static class MutableAuthorizer extends BaseAuthorizer {
        private volatile boolean authorized;

        MutableAuthorizer(boolean authorized) {
            this.authorized = authorized;
        }

        @Override
        public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
            return authorized;
        }
    }

    private abstract static class BaseAuthorizer implements IAuthorizer {
        @Override
        public void prepare(Map<String, Object> conf) {
            // No external dependencies are needed for these tests.
        }
    }

    private static class TestOutstandingRequest extends OutstandingRequest {
        private String result;
        private DRPCExecutionException failure;
        private boolean returnResultCalled;

        TestOutstandingRequest(String function, DRPCRequest req) {
            super(function, req);
        }

        @Override
        public void returnResult(String result) {
            this.returnResultCalled = true;
            this.result = result;
        }

        @Override
        public void fail(DRPCExecutionException e) {
            this.failure = e;
        }
    }

    /* ### Test END ### */
}
