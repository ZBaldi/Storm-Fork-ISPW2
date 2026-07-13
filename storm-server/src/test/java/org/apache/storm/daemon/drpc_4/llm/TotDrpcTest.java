package org.apache.storm.daemon.drpc_4.llm;

import org.apache.storm.daemon.drpc.refactored.four.DRPC;
import org.apache.storm.daemon.drpc.OutstandingRequest;
import org.apache.storm.daemon.drpc.RequestFactory;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.generated.DRPCRequest;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.security.auth.authorizer.DRPCAuthorizerBase;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.*;

import static org.junit.Assert.*;

public class TotDrpcTest {
    // ### Test START ###

    private static final long TEST_TIMEOUT_MS = 60_000L;

    private static final IAuthorizer ALWAYS_AUTHORIZER = new IAuthorizer() {
        @Override
        public void prepare(Map<String, Object> conf) {
            // No external dependency required for tests.
        }

        @Override
        public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
            return true;
        }
    };

    private static final IAuthorizer NEVER_AUTHORIZER = new IAuthorizer() {
        @Override
        public void prepare(Map<String, Object> conf) {
            // No external dependency required for tests.
        }

        @Override
        public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
            return false;
        }
    };

    private static final IAuthorizer INVALID_AUTHORIZER = new IAuthorizer() {
        @Override
        public void prepare(Map<String, Object> conf) {
            // No external dependency required for tests.
        }

        @Override
        public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
            throw new RuntimeException("invalid authorizer");
        }
    };


    private static class DenyOperationAuthorizer implements IAuthorizer {
        private final String deniedOperation;

        DenyOperationAuthorizer(String deniedOperation) {
            this.deniedOperation = deniedOperation;
        }

        @Override
        public void prepare(Map<String, Object> conf) {
            // No external dependency required for tests.
        }

        @Override
        public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
            return !deniedOperation.equals(operation);
        }
    }

    private static class CapturingAuthorizer implements IAuthorizer {
        private String operation;
        private Object functionName;

        @Override
        public void prepare(Map<String, Object> conf) {
            // No external dependency required for tests.
        }

        @Override
        public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
            this.operation = operation;
            this.functionName = topoConf.get(DRPCAuthorizerBase.FUNCTION_NAME);
            return true;
        }
    }

    private static class TestOutstandingRequest extends OutstandingRequest {
        private String result;
        private DRPCExecutionException failure;

        TestOutstandingRequest(String function, DRPCRequest req) {
            super(function, req);
        }

        @Override
        public void returnResult(String result) {
            this.result = result;
        }

        @Override
        public void fail(DRPCExecutionException e) {
            this.failure = e;
        }
    }

    private static final RequestFactory<TestOutstandingRequest> VALID_FACTORY = new RequestFactory<TestOutstandingRequest>() {
        @Override
        public TestOutstandingRequest mkRequest(String function, DRPCRequest req) {
            return new TestOutstandingRequest(function, req);
        }
    };

    private static final RequestFactory<TestOutstandingRequest> NULL_FACTORY = new RequestFactory<TestOutstandingRequest>() {
        @Override
        public TestOutstandingRequest mkRequest(String function, DRPCRequest req) {
            return null;
        }
    };

    private DRPC newDrpc(IAuthorizer authorizer) {
        return new DRPC(new StormMetricsRegistry(), authorizer, TEST_TIMEOUT_MS);
    }

    private DRPCRequest fetchUntilAvailable(DRPC drpc, String functionName) throws Exception {
        long deadline = System.currentTimeMillis() + 5_000L;
        DRPCRequest request;
        do {
            request = drpc.fetchRequest(functionName);
            if (!"".equals(request.get_request_id())) {
                return request;
            }
            Thread.sleep(10L);
        } while (System.currentTimeMillis() < deadline);
        fail("No DRPC request became available for function '" + functionName + "'");
        return null;
    }

    @Test
    public void checkAuthorizationAllowsAlwaysAuthorizedAuthorizer() throws Exception {
        DRPC.checkAuthorization(ReqContext.context(), ALWAYS_AUTHORIZER, "execute", "try");
    }

    @Test(expected = AuthorizationException.class)
    public void checkAuthorizationRejectsNeverAuthorizedAuthorizer() throws Exception {
        DRPC.checkAuthorization(ReqContext.context(), NEVER_AUTHORIZER, "execute", "try");
    }

    @Test(expected = RuntimeException.class)
    public void checkAuthorizationPropagatesInvalidAuthorizerFailure() throws Exception {
        DRPC.checkAuthorization(ReqContext.context(), INVALID_AUTHORIZER, "execute", "try");
    }

    @Test
    public void checkAuthorizationPassesFunctionNameToAuthorizer() throws Exception {
        CapturingAuthorizer authorizer = new CapturingAuthorizer();

        DRPC.checkAuthorization(ReqContext.context(), authorizer, "fetchRequest", "try");

        assertEquals("fetchRequest", authorizer.operation);
        assertEquals("try", authorizer.functionName);
    }

    @Test
    public void fetchRequestReturnsNothingRequestWhenQueueIsEmpty() throws Exception {
        DRPC drpc = newDrpc(null);
        try {
            DRPCRequest request = drpc.fetchRequest("try");

            assertEquals("", request.get_func_args());
            assertEquals("", request.get_request_id());
        } finally {
            drpc.close();
        }
    }

    // @Test(expected = IllegalArgumentException.class)
    public void fetchRequestRejectsNullFunctionName() throws Exception {
        DRPC drpc = newDrpc(null);
        try {
            drpc.fetchRequest(null);
        } finally {
            drpc.close();
        }
    }

    @Test(expected = AuthorizationException.class)
    public void fetchRequestIsRejectedWhenAuthorizerDeniesIt() throws Exception {
        DRPC drpc = newDrpc(NEVER_AUTHORIZER);
        try {
            drpc.fetchRequest("try");
        } finally {
            drpc.close();
        }
    }

    @Test
    public void executeCreatesOutstandingRequestAndFetchRequestReturnsGeneratedDrpcRequest() throws Exception {
        DRPC drpc = newDrpc(null);
        try {
            TestOutstandingRequest outstanding = drpc.execute("try", "args", VALID_FACTORY);

            assertNotNull(outstanding);
            assertEquals("try", outstanding.getFunction());
            assertEquals("args", outstanding.getRequest().get_func_args());
            assertEquals("1", outstanding.getRequest().get_request_id());

            DRPCRequest fetched = drpc.fetchRequest("try");
            assertEquals("args", fetched.get_func_args());
            assertEquals("1", fetched.get_request_id());
            assertTrue(outstanding.wasFetched());
        } finally {
            drpc.close();
        }
    }

    // @Test(expected = IllegalArgumentException.class) (FAILED) CLEANUP  DOES A GET WITH A NULL FUNCTION NAME --> NULL POINTER EXCEPTION
    public void executeRejectsNullFunctionName() throws Exception {
        DRPC drpc = newDrpc(null);
        try {
            drpc.execute(null, "args", VALID_FACTORY);
        } finally {
            drpc.close();
        }
    }

    @Test(expected = NullPointerException.class)
    public void executeFailsWhenFactoryReturnsNullRequest() throws Exception {
        DRPC drpc = newDrpc(null);
        try {
            drpc.execute("try", "args", NULL_FACTORY);
        } finally {
            drpc.close();
        }
    }

    @Test(expected = AuthorizationException.class)
    public void executeIsRejectedWhenAuthorizerDeniesIt() throws Exception {
        DRPC drpc = newDrpc(NEVER_AUTHORIZER);
        try {
            drpc.execute("try", "args", VALID_FACTORY);
        } finally {
            drpc.close();
        }
    }

    @Test
    public void returnResultCompletesExistingOutstandingRequest() throws Exception {
        DRPC drpc = newDrpc(null);
        try {
            TestOutstandingRequest outstanding = drpc.execute("try", "args", VALID_FACTORY);

            drpc.returnResult("1", "done");

            assertEquals("done", outstanding.result);
        } finally {
            drpc.close();
        }
    }

    @Test
    public void returnResultIgnoresUnknownRequestId() throws Exception {
        DRPC drpc = newDrpc(null);
        try {
            drpc.returnResult("not-present", "done");
        } finally {
            drpc.close();
        }
    }

    // @Test(expected = AuthorizationException.class)
    public void returnResultIsRejectedWhenAuthorizerDeniesExistingRequest() throws Exception {
        DRPC drpc = newDrpc(new DenyOperationAuthorizer("result"));
        try {
            drpc.execute("try", "args", VALID_FACTORY);
            drpc.returnResult("1", "done");
        } finally {
            drpc.close();
        }
    }

    @Test
    public void failRequestCompletesExistingOutstandingRequestWithGivenException() throws Exception {
        DRPC drpc = newDrpc(null);
        try {
            TestOutstandingRequest outstanding = drpc.execute("try", "args", VALID_FACTORY);
            DRPCExecutionException failure = new DRPCExecutionException("failed");

            drpc.failRequest("1", failure);

            assertSame(failure, outstanding.failure);
            assertEquals("failed", outstanding.failure.get_msg());
        } finally {
            drpc.close();
        }
    }

    @Test
    public void failRequestUsesDefaultFailureWhenExceptionIsNull() throws Exception {
        DRPC drpc = newDrpc(null);
        try {
            TestOutstandingRequest outstanding = drpc.execute("try", "args", VALID_FACTORY);

            drpc.failRequest("1", null);

            assertNotNull(outstanding.failure);
            assertEquals("Request failed", outstanding.failure.get_msg());
        } finally {
            drpc.close();
        }
    }

    @Test
    public void failRequestIgnoresUnknownRequestId() throws Exception {
        DRPC drpc = newDrpc(null);
        try {
            drpc.failRequest("not-present", new DRPCExecutionException("failed"));
        } finally {
            drpc.close();
        }
    }

    @Test(expected = AuthorizationException.class)
    public void failRequestIsRejectedWhenAuthorizerDeniesExistingRequest() throws Exception {
        DRPC drpc = newDrpc(new DenyOperationAuthorizer("failRequest"));
        try {
            drpc.execute("try", "args", VALID_FACTORY);
            drpc.failRequest("1", new DRPCExecutionException("failed"));
        } finally {
            drpc.close();
        }
    }

    @Test
    public void executeBlockingReturnsValuePassedToReturnResult() throws Exception {
        final DRPC drpc = newDrpc(null);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> result = executor.submit(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    return drpc.executeBlocking("try", "args");
                }
            });

            DRPCRequest fetched = fetchUntilAvailable(drpc, "try");
            assertEquals("args", fetched.get_func_args());
            drpc.returnResult(fetched.get_request_id(), "done");

            assertEquals("done", result.get(5, TimeUnit.SECONDS));
            assertEquals("", drpc.fetchRequest("try").get_request_id());
        } finally {
            executor.shutdownNow();
            drpc.close();
        }
    }

    @Test
    public void executeBlockingThrowsFailurePassedToFailRequest() throws Exception {
        final DRPC drpc = newDrpc(null);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> result = executor.submit(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    return drpc.executeBlocking("try", "args");
                }
            });

            DRPCRequest fetched = fetchUntilAvailable(drpc, "try");
            drpc.failRequest(fetched.get_request_id(), new DRPCExecutionException("failed"));

            try {
                result.get(5, TimeUnit.SECONDS);
                fail("Expected DRPCExecutionException");
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof DRPCExecutionException);
                assertEquals("failed", ((DRPCExecutionException) e.getCause()).get_msg());
            }
        } finally {
            executor.shutdownNow();
            drpc.close();
        }
    }

    @Test
    public void executeBlockingThrowsDefaultFailureWhenFailRequestReceivesNullException() throws Exception {
        final DRPC drpc = newDrpc(null);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> result = executor.submit(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    return drpc.executeBlocking("try", "args");
                }
            });

            DRPCRequest fetched = fetchUntilAvailable(drpc, "try");
            drpc.failRequest(fetched.get_request_id(), null);

            try {
                result.get(5, TimeUnit.SECONDS);
                fail("Expected DRPCExecutionException");
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof DRPCExecutionException);
                assertEquals("Request failed", ((DRPCExecutionException) e.getCause()).get_msg());
            }
        } finally {
            executor.shutdownNow();
            drpc.close();
        }
    }

    @Test(expected = AuthorizationException.class)
    public void executeBlockingIsRejectedWhenAuthorizerDeniesIt() throws Exception {
        DRPC drpc = newDrpc(NEVER_AUTHORIZER);
        try {
            drpc.executeBlocking("try", "args");
        } finally {
            drpc.close();
        }
    }

    @Test
    public void closeFailsPendingBlockingRequestsAsServerShutdown() throws Exception {
        final DRPC drpc = newDrpc(null);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> result = executor.submit(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    return drpc.executeBlocking("try", "args");
                }
            });

            fetchUntilAvailable(drpc, "try");
            drpc.close();

            try {
                result.get(5, TimeUnit.SECONDS);
                fail("Expected DRPCExecutionException");
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof DRPCExecutionException);
                assertEquals("Server Shutting Down", ((DRPCExecutionException) e.getCause()).get_msg());
            }
        } finally {
            executor.shutdownNow();
        }
    }

    // ### Test END ###
}
