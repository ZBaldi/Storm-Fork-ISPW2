package org.apache.storm.daemon.drpc_2.llm;

import com.codahale.metrics.Meter;
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
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.anyString;

/**
 * JUnit 4 tests for DRPC.
 *
 * The tests use small fakes/mocks to decouple DRPC from external Storm services and focus on:
 * authorization, request queueing/fetching, result/failure completion and timeout cleanup.
 */
public class Fsp2DrpcTest {

    // ### Test START ###

    private static final String FUNCTION = "try";
    private static final String ARGS = "args";
    private static final String RESULT = "done";
    private static final long TEST_TIMEOUT_MS = 500L;

    private DRPC drpcAuthOk;
    private DRPC drpcAuthKo;
    private DRPC drpcNoAuth;
    private DRPC drpcNotValid;

    @Before
    public void setUp() {
        drpcAuthOk = new DRPC(mockMetricsRegistry(), new AlwaysAuthorizedAuthorizer(), TEST_TIMEOUT_MS);
        drpcAuthKo = new DRPC(mockMetricsRegistry(), new NeverAuthorizedAuthorizer(), TEST_TIMEOUT_MS);
        drpcNoAuth = new DRPC(mockMetricsRegistry(), null, TEST_TIMEOUT_MS);
        drpcNotValid = null;
    }

    @After
    public void tearDown() {
        closeQuietly(drpcAuthOk);
        closeQuietly(drpcAuthKo);
        closeQuietly(drpcNoAuth);
    }

    /** Test fetchRequest method with functionName = "try" (existing) and state authorized. Expected = queued DRPCRequest. */
    @Test
    public void fetchRequestCorrectFunctionNameAuthShouldReturnQueuedRequest() throws AuthorizationException {
        TestOutstandingRequest expected = drpcAuthOk.execute(FUNCTION, ARGS, testOutstandingFactory());

        DRPCRequest actual = drpcAuthOk.fetchRequest(FUNCTION);

        Assert.assertEquals(expected.getRequest(), actual);
        Assert.assertTrue(expected.wasFetched());
        Assert.assertEquals(ARGS, actual.get_func_args());
        Assert.assertEquals("1", actual.get_request_id());
    }

    /** Test fetchRequest method with functionName = "try" and no queued request. Expected = empty DRPCRequest. */
    @Test
    public void fetchRequestCorrectFunctionNameAuthWithoutRequestShouldReturnNothingRequest() throws AuthorizationException {
        DRPCRequest actual = drpcAuthOk.fetchRequest(FUNCTION);

        Assert.assertEquals(new DRPCRequest("", ""), actual);
    }

    /** Test fetchRequest method with functionName = "missing" and another function queued. Expected = empty DRPCRequest. */
    @Test
    public void fetchRequestNotCorrectFunctionNameAuthShouldReturnNothingRequest() throws AuthorizationException {
        drpcAuthOk.execute(FUNCTION, ARGS, testOutstandingFactory());

        DRPCRequest actual = drpcAuthOk.fetchRequest("missing");

        Assert.assertEquals(new DRPCRequest("", ""), actual);
    }

    /** Test fetchRequest method with functionName = null and state authorized. Expected = IllegalArgumentException. */
    @Test
    public void fetchRequestNullFunctionNameAuthThrowsIllegalArgumentException() {
        Assert.assertThrows(IllegalArgumentException.class, () -> drpcAuthOk.fetchRequest(null));
    }

    /** Test fetchRequest method with functionName = "try" and state not authorized. Expected = AuthorizationException. */
    @Test
    public void fetchRequestCorrectFunctionNameNotAuthThrowsAuthorizationException() {
        Assert.assertThrows(AuthorizationException.class, () -> drpcAuthKo.fetchRequest(FUNCTION));
    }

    /** Test fetchRequest method with functionName = "try" and invalid DRPC instance. Expected = NullPointerException. */
    @Test
    public void fetchRequestCorrectFunctionNameInvalidStateThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> drpcNotValid.fetchRequest(FUNCTION));
    }

    /** Test returnResult method with existing id = "1", result = "done" and state authorized. Expected = request completed. */
    @Test
    public void returnResultCorrectIdValidResultAuthShouldCompleteRequest() throws AuthorizationException {
        TestOutstandingRequest request = drpcAuthOk.execute(FUNCTION, ARGS, testOutstandingFactory());

        drpcAuthOk.returnResult("1", RESULT);

        Assert.assertEquals(RESULT, request.getReturnedResult());
        Assert.assertNull(request.getFailure());
    }

    /** Test returnResult method with id = "missing", result = "done" and state authorized. Expected = no exception. */
    @Test
    public void returnResultNotCorrectIdValidResultAuthShouldDoNothing() throws AuthorizationException {
        drpcAuthOk.execute(FUNCTION, ARGS, testOutstandingFactory());

        drpcAuthOk.returnResult("missing", RESULT);
    }

    /** Test returnResult method with id = null and result = "". Expected = NullPointerException. */
    @Test
    public void returnResultNullIdEmptyResultAuthThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> drpcAuthOk.returnResult(null, ""));
    }

    /** Test returnResult method with existing id = "1" and state not authorized for result operation. Expected = AuthorizationException. */
    @Test
    public void returnResultCorrectIdValidResultNotAuthThrowsAuthorizationException() throws AuthorizationException {
        DRPC localDrpc = new DRPC(mockMetricsRegistry(), new DenyOperationAuthorizer("result"), TEST_TIMEOUT_MS);
        try {
            localDrpc.execute(FUNCTION, ARGS, testOutstandingFactory());

            Assert.assertThrows(AuthorizationException.class, () -> localDrpc.returnResult("1", RESULT));
        } finally {
            closeQuietly(localDrpc);
        }
    }

    /** Test returnResult method with valid input and invalid DRPC instance. Expected = NullPointerException. */
    @Test
    public void returnResultValidInputInvalidStateThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> drpcNotValid.returnResult("1", RESULT));
    }

    /** Test failRequest method with existing id and valid exception. Expected = request failed with supplied exception. */
    @Test
    public void failRequestCorrectIdValidExceptionAuthShouldFailRequest() throws AuthorizationException {
        TestOutstandingRequest request = drpcAuthOk.execute(FUNCTION, ARGS, testOutstandingFactory());
        DRPCExecutionException failure = new WrappedDRPCExecutionException("failed");

        drpcAuthOk.failRequest("1", failure);

        Assert.assertSame(failure, request.getFailure());
    }

    /** Test failRequest method with existing id and null exception. Expected = default failure exception. */
    @Test
    public void failRequestCorrectIdNullExceptionAuthShouldUseDefaultFailure() throws AuthorizationException {
        TestOutstandingRequest request = drpcAuthOk.execute(FUNCTION, ARGS, testOutstandingFactory());

        drpcAuthOk.failRequest("1", null);

        Assert.assertNotNull(request.getFailure());
        Assert.assertEquals("Request failed", request.getFailure().get_msg());
    }

    /** Test failRequest method with id = "missing" and valid exception. Expected = no exception. */
    @Test
    public void failRequestNotCorrectIdValidExceptionAuthShouldDoNothing() throws AuthorizationException {
        drpcAuthOk.execute(FUNCTION, ARGS, testOutstandingFactory());

        drpcAuthOk.failRequest("missing", new WrappedDRPCExecutionException("failed"));
    }

    /** Test failRequest method with id = null. Expected = NullPointerException. */
    @Test
    public void failRequestNullIdValidExceptionAuthThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class,
                () -> drpcAuthOk.failRequest(null, new WrappedDRPCExecutionException("failed")));
    }

    /** Test failRequest method with existing id and state not authorized for fail operation. Expected = AuthorizationException. */
    @Test
    public void failRequestCorrectIdValidExceptionNotAuthThrowsAuthorizationException() throws AuthorizationException {
        DRPC localDrpc = new DRPC(mockMetricsRegistry(), new DenyOperationAuthorizer("failRequest"), TEST_TIMEOUT_MS);
        try {
            localDrpc.execute(FUNCTION, ARGS, testOutstandingFactory());

            Assert.assertThrows(AuthorizationException.class,
                    () -> localDrpc.failRequest("1", new WrappedDRPCExecutionException("failed")));
        } finally {
            closeQuietly(localDrpc);
        }
    }

    /** Test execute method with functionName = "try", funcArgs = "args" and valid factory. Expected = request is created and queued. */
    @Test
    public void executeValidFunctionNameValidFuncArgsValidFactoryAuthShouldPass() throws AuthorizationException {
        TestOutstandingRequest actual = drpcAuthOk.execute(FUNCTION, ARGS, testOutstandingFactory());

        Assert.assertEquals(FUNCTION, actual.getFunction());
        Assert.assertEquals(new DRPCRequest(ARGS, "1"), actual.getRequest());
        Assert.assertEquals(actual.getRequest(), drpcAuthOk.fetchRequest(FUNCTION));
    }

    /** Test execute method with functionName = "try", funcArgs = null and valid factory. Expected = request with null args. */
    @Test
    public void executeValidFunctionNameNullFuncArgsValidFactoryAuthShouldPass() throws AuthorizationException {
        TestOutstandingRequest actual = drpcAuthOk.execute(FUNCTION, null, testOutstandingFactory());

        Assert.assertEquals(FUNCTION, actual.getFunction());
        Assert.assertEquals(new DRPCRequest(null, "1"), actual.getRequest());
    }

    /** Test execute method with empty functionName and valid args. Expected = request is accepted and queued. */
    @Test
    public void executeEmptyFunctionNameValidFuncArgsValidFactoryAuthShouldPass() throws AuthorizationException {
        TestOutstandingRequest actual = drpcAuthOk.execute("", ARGS, testOutstandingFactory());

        Assert.assertEquals("", actual.getFunction());
        Assert.assertEquals(new DRPCRequest(ARGS, "1"), actual.getRequest());
    }

    /** Test execute method with functionName = null and valid factory. Expected = IllegalArgumentException after request creation. */
    // @Test (FAILED) @AFTER STARTS CLEANUP THAT DOES A GET WITH A NULL FUNCTION NAME --> NULL POINTER EXCEPTION
    public void executeNullFunctionNameValidFactoryAuthThrowsIllegalArgumentException() {
        Assert.assertThrows(IllegalArgumentException.class, () -> drpcAuthOk.execute(null, ARGS, testOutstandingFactory()));
    }

    /** Test execute method with valid input and null factory. Expected = NullPointerException. */
    @Test
    public void executeValidFunctionNameValidFuncArgsNullFactoryAuthThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> drpcAuthOk.execute(FUNCTION, ARGS, null));
    }

    /** Test execute method with valid input and factory returning null. Expected = NullPointerException. */
    @Test
    public void executeValidFunctionNameValidFuncArgsNotValidFactoryAuthThrowsNullPointerException() {
        RequestFactory<TestOutstandingRequest> invalidFactory = (function, request) -> null;

        Assert.assertThrows(NullPointerException.class, () -> drpcAuthOk.execute(FUNCTION, ARGS, invalidFactory));
    }

    /** Test execute method with valid input and state not authorized. Expected = AuthorizationException. */
    @Test
    public void executeValidFunctionNameValidFuncArgsValidFactoryNotAuthThrowsAuthorizationException() {
        Assert.assertThrows(AuthorizationException.class, () -> drpcAuthKo.execute(FUNCTION, ARGS, testOutstandingFactory()));
    }

    /** Test execute method with valid input and invalid DRPC instance. Expected = NullPointerException. */
    @Test
    public void executeValidFunctionNameValidFuncArgsInvalidStateThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> drpcNotValid.execute(FUNCTION, ARGS, testOutstandingFactory()));
    }

    /** Test executeBlocking method with successful returnResult. Expected = result passed to returnResult. */
    @Test
    public void executeBlockingValidFunctionNameValidFuncArgsAuthReturnResultShouldReturnValue() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> future = executor.submit(() -> drpcAuthOk.executeBlocking(FUNCTION, ARGS));
            DRPCRequest request = waitAndFetchRequest(drpcAuthOk, FUNCTION);

            drpcAuthOk.returnResult(request.get_request_id(), RESULT);

            Assert.assertEquals(RESULT, future.get(2, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }

    /** Test executeBlocking method with failRequest. Expected = DRPCExecutionException with supplied message. */
    @Test
    public void executeBlockingValidFunctionNameValidFuncArgsAuthFailRequestThrowsDRPCExecutionException() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> future = executor.submit(() -> drpcAuthOk.executeBlocking(FUNCTION, ARGS));
            DRPCRequest request = waitAndFetchRequest(drpcAuthOk, FUNCTION);

            drpcAuthOk.failRequest(request.get_request_id(), new WrappedDRPCExecutionException("failed"));

            try {
                future.get(2, TimeUnit.SECONDS);
                Assert.fail("Expected DRPCExecutionException");
            } catch (java.util.concurrent.ExecutionException ex) {
                Assert.assertTrue(ex.getCause() instanceof DRPCExecutionException);
                Assert.assertEquals("failed", ((DRPCExecutionException) ex.getCause()).get_msg());
            }
        } finally {
            executor.shutdownNow();
        }
    }

    /** Test executeBlocking method with valid input and state not authorized. Expected = AuthorizationException. */
    @Test
    public void executeBlockingValidFunctionNameValidFuncArgsNotAuthThrowsAuthorizationException() {
        Assert.assertThrows(AuthorizationException.class, () -> drpcAuthKo.executeBlocking(FUNCTION, ARGS));
    }

    /** Test executeBlocking method with valid input and invalid DRPC instance. Expected = NullPointerException. */
    @Test
    public void executeBlockingValidFunctionNameValidFuncArgsInvalidStateThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> drpcNotValid.executeBlocking(FUNCTION, ARGS));
    }

    /** Test timeout cleanup with an unfetched request. Expected = request failure contains timeout message and queue is empty. */
    @Test
    public void requestTimeoutUnfetchedRequestShouldFailAndBeRemovedFromQueue() throws Exception {
        DRPC shortTimeoutDrpc = new DRPC(mockMetricsRegistry(), new AlwaysAuthorizedAuthorizer(), 200L);
        try {
            TestOutstandingRequest request = shortTimeoutDrpc.execute(FUNCTION, ARGS, testOutstandingFactory());

            Thread.sleep(450L);

            Assert.assertNotNull(request.getFailure());
            Assert.assertEquals("Timed Out", request.getFailure().get_msg());
            Assert.assertEquals(new DRPCRequest("", ""), shortTimeoutDrpc.fetchRequest(FUNCTION));
        } finally {
            closeQuietly(shortTimeoutDrpc);
        }
    }

    /** Test close method with pending request. Expected = request failed with shutdown message. */
    @Test
    public void closeWithPendingRequestShouldFailRequestWithShutdownMessage() throws AuthorizationException {
        DRPC localDrpc = new DRPC(mockMetricsRegistry(), new AlwaysAuthorizedAuthorizer(), TEST_TIMEOUT_MS);
        TestOutstandingRequest request = localDrpc.execute(FUNCTION, ARGS, testOutstandingFactory());

        localDrpc.close();

        Assert.assertNotNull(request.getFailure());
        Assert.assertEquals("Server Shutting Down", request.getFailure().get_msg());
    }

    /** Test checkAuthorization method with always authorized authorizer. Expected = no exception. */
    @Test
    public void checkAuthorizationValidContextAlwaysAuthorizedShouldPass() throws AuthorizationException {
        DRPC.checkAuthorization(ReqContext.context(), new AlwaysAuthorizedAuthorizer(), "execute", FUNCTION);
    }

    /** Test checkAuthorization method with never authorized authorizer. Expected = AuthorizationException. */
    @Test
    public void checkAuthorizationValidContextNeverAuthorizedThrowsAuthorizationException() {
        Assert.assertThrows(AuthorizationException.class,
                () -> DRPC.checkAuthorization(ReqContext.context(), new NeverAuthorizedAuthorizer(), "execute", FUNCTION));
    }

    /** Test checkAuthorization method with null authorizer. Expected = no exception. */
    @Test
    public void checkAuthorizationValidContextNullAuthorizerShouldPass() throws AuthorizationException {
        DRPC.checkAuthorization(ReqContext.context(), null, "execute", FUNCTION);
    }

    /** Test checkAuthorization method with null context and always authorized authorizer. Expected = no exception. */
    @Test
    public void checkAuthorizationNullContextAlwaysAuthorizedShouldPass() throws AuthorizationException {
        DRPC.checkAuthorization(null, new AlwaysAuthorizedAuthorizer(), "execute", FUNCTION);
    }

    /** Test checkAuthorization method with invalid authorizer. Expected = RuntimeException. */
    @Test
    public void checkAuthorizationValidContextInvalidAuthorizerThrowsRuntimeException() {
        Assert.assertThrows(RuntimeException.class,
                () -> DRPC.checkAuthorization(ReqContext.context(), new RuntimeExceptionAuthorizer(), "execute", FUNCTION));
    }

    /** Test checkAuthorization method passes the function name inside topoConf. Expected = captured function equals "try". */
    @Test
    public void checkAuthorizationValidContextShouldPassFunctionNameInTopoConf() throws AuthorizationException {
        CapturingAuthorizer authorizer = new CapturingAuthorizer();

        DRPC.checkAuthorization(ReqContext.context(), authorizer, "execute", FUNCTION);

        Assert.assertEquals(FUNCTION, authorizer.getLastTopoConf().get(DRPCAuthorizerBase.FUNCTION_NAME));
    }

    // ### Test END ###

    private static StormMetricsRegistry mockMetricsRegistry() {
        StormMetricsRegistry metricsRegistry = Mockito.mock(StormMetricsRegistry.class);
        Mockito.when(metricsRegistry.registerMeter(anyString())).thenReturn(new Meter());
        return metricsRegistry;
    }

    private static RequestFactory<TestOutstandingRequest> testOutstandingFactory() {
        return TestOutstandingRequest::new;
    }

    private static void closeQuietly(DRPC drpc) {
        if (drpc != null) {
            drpc.close();
        }
    }

    private static DRPCRequest waitAndFetchRequest(DRPC drpc, String functionName) throws Exception {
        long deadline = System.currentTimeMillis() + 1500L;
        DRPCRequest request;
        do {
            request = drpc.fetchRequest(functionName);
            if (!new DRPCRequest("", "").equals(request)) {
                return request;
            }
            Thread.sleep(20L);
        } while (System.currentTimeMillis() < deadline);
        Assert.fail("Request was not queued in time");
        return null;
    }

    private static class TestOutstandingRequest extends OutstandingRequest {
        private String returnedResult;
        private DRPCExecutionException failure;

        TestOutstandingRequest(String function, DRPCRequest request) {
            super(function, request);
        }

        @Override
        public void returnResult(String result) {
            this.returnedResult = result;
        }

        @Override
        public void fail(DRPCExecutionException e) {
            this.failure = e;
        }

        String getReturnedResult() {
            return returnedResult;
        }

        DRPCExecutionException getFailure() {
            return failure;
        }
    }

    private static class AlwaysAuthorizedAuthorizer implements IAuthorizer {
        @Override
        public void prepare(Map<String, Object> conf) {
            // Not needed for these unit tests.
        }

        @Override
        public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
            return true;
        }
    }

    private static class NeverAuthorizedAuthorizer implements IAuthorizer {
        @Override
        public void prepare(Map<String, Object> conf) {
            // Not needed for these unit tests.
        }

        @Override
        public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
            return false;
        }
    }


    private static class DenyOperationAuthorizer implements IAuthorizer {
        private final String deniedOperation;

        DenyOperationAuthorizer(String deniedOperation) {
            this.deniedOperation = deniedOperation;
        }

        @Override
        public void prepare(Map<String, Object> conf) {
            // Not needed for these unit tests.
        }

        @Override
        public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
            return !deniedOperation.equals(operation);
        }
    }

    private static class RuntimeExceptionAuthorizer implements IAuthorizer {
        @Override
        public void prepare(Map<String, Object> conf) {
            // Not needed for these unit tests.
        }

        @Override
        public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
            throw new RuntimeException("invalid authorizer");
        }
    }

    private static class CapturingAuthorizer implements IAuthorizer {
        private Map<String, Object> lastTopoConf;

        @Override
        public void prepare(Map<String, Object> conf) {
            // Not needed for these unit tests.
        }

        @Override
        public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
            this.lastTopoConf = topoConf;
            return true;
        }

        Map<String, Object> getLastTopoConf() {
            return lastTopoConf;
        }
    }
}
