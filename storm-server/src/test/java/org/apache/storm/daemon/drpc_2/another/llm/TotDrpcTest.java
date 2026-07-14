package org.apache.storm.daemon.drpc_2.another.llm;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;

import com.codahale.metrics.Meter;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.storm.daemon.drpc.OutstandingRequest;
import org.apache.storm.daemon.drpc.RequestFactory;
import org.apache.storm.daemon.drpc.refactored.two.DRPC;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.generated.DRPCRequest;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.security.auth.ReqContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class TotDrpcTest {
    // ### Test START ###

    private static final long LONG_TIMEOUT_MS = 60000L;
    private static final long SHORT_TIMEOUT_MS = 250L;

    private StormMetricsRegistry metricsRegistry;
    private DRPC drpcAuthOk;
    private DRPC drpcAuthKo;
    private DRPC drpcNoAuth;

    private final IAuthorizer alwaysAuthorized = new IAuthorizer() {
        @Override
        public void prepare(Map<String, Object> conf) {
        }

        @Override
        public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
            return true;
        }
    };

    private final IAuthorizer neverAuthorized = new IAuthorizer() {
        @Override
        public void prepare(Map<String, Object> conf) {
        }

        @Override
        public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
            return false;
        }
    };

    private final IAuthorizer invalidAuthorizer = new IAuthorizer() {
        @Override
        public void prepare(Map<String, Object> conf) {
        }

        @Override
        public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
            throw new RuntimeException("invalid authorizer");
        }
    };

    @Before
    public void setUp() {
        metricsRegistry = Mockito.mock(StormMetricsRegistry.class);
        Mockito.when(metricsRegistry.registerMeter(anyString())).thenReturn(Mockito.mock(Meter.class));
        drpcAuthOk = new DRPC(metricsRegistry, alwaysAuthorized, LONG_TIMEOUT_MS);
        drpcAuthKo = new DRPC(metricsRegistry, neverAuthorized, LONG_TIMEOUT_MS);
        drpcNoAuth = new DRPC(metricsRegistry, null, LONG_TIMEOUT_MS);
    }

    @After
    public void tearDown() {
        if (drpcAuthOk != null) {
            drpcAuthOk.close();
        }
        if (drpcAuthKo != null) {
            drpcAuthKo.close();
        }
        if (drpcNoAuth != null) {
            drpcNoAuth.close();
        }
    }

    /** Test checkAuthorization method with valid ReqContext, always authorized authorizer, operation = "execute" and function = "try". Expected = no exception. */
    @Test
    public void checkAuthorizationValidContextAlwaysAuthorizedShouldPass() throws AuthorizationException {
        DRPC.checkAuthorization(ReqContext.context(), alwaysAuthorized, "execute", "try");
    }

    /** Test checkAuthorization method with valid ReqContext, never authorized authorizer, operation = "execute" and function = "try". Expected = throws AuthorizationException. */
    @Test
    public void checkAuthorizationValidContextNeverAuthorizedThrowsAuthorizationException() {
        Assert.assertThrows(AuthorizationException.class, () ->
            DRPC.checkAuthorization(ReqContext.context(), neverAuthorized, "execute", "try"));
    }

    /** Test checkAuthorization method with null authorizer. Expected = no exception because authorization is disabled. */
    @Test
    public void checkAuthorizationNullAuthorizerShouldPass() throws AuthorizationException {
        DRPC.checkAuthorization(ReqContext.context(), null, "execute", "try");
    }

    /** Test checkAuthorization method with null ReqContext and always authorized authorizer. Expected = no exception. */
    @Test
    public void checkAuthorizationNullContextAlwaysAuthorizedShouldPass() throws AuthorizationException {
        DRPC.checkAuthorization(null, alwaysAuthorized, "execute", "try");
    }

    /** Test checkAuthorization method with invalid authorizer implementation. Expected = throws RuntimeException. */
    @Test
    public void checkAuthorizationInvalidAuthorizerThrowsRuntimeException() {
        Assert.assertThrows(RuntimeException.class, () ->
            DRPC.checkAuthorization(ReqContext.context(), invalidAuthorizer, "execute", "try"));
    }

    /** Test execute method with functionName = "try", funcArgs = "args", valid factory and authorized state. Expected = returned OutstandingRequest and generated DRPCRequest("args", "1"). */
    @SuppressWarnings("unchecked")
    @Test
    public void executeValidFunctionNameValidArgsValidFactoryAuthShouldPass() throws AuthorizationException {
        RequestFactory<TestOutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        ArgumentCaptor<DRPCRequest> requestCaptor = ArgumentCaptor.forClass(DRPCRequest.class);
        TestOutstandingRequest expected = new TestOutstandingRequest("try", new DRPCRequest("args", "1"));
        Mockito.when(factory.mkRequest(eq("try"), any(DRPCRequest.class))).thenReturn(expected);

        TestOutstandingRequest actual = drpcAuthOk.execute("try", "args", factory);

        Mockito.verify(factory).mkRequest(eq("try"), requestCaptor.capture());
        Assert.assertSame(expected, actual);
        Assert.assertEquals("args", requestCaptor.getValue().get_func_args());
        Assert.assertEquals("1", requestCaptor.getValue().get_request_id());
    }

    /** Test execute method with functionName = "try", funcArgs = null, valid factory and authorized state. Expected = returned OutstandingRequest containing null arguments. */
    @SuppressWarnings("unchecked")
    @Test
    public void executeValidFunctionNameNullArgsValidFactoryAuthShouldPass() throws AuthorizationException {
        RequestFactory<TestOutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        TestOutstandingRequest expected = new TestOutstandingRequest("try", new DRPCRequest(null, "1"));
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(expected);

        TestOutstandingRequest actual = drpcAuthOk.execute("try", null, factory);

        Assert.assertSame(expected, actual);
    }

    /** Test execute method with functionName = "", funcArgs = "args", valid factory and authorized state. Expected = returned OutstandingRequest. */
    @SuppressWarnings("unchecked")
    @Test
    public void executeEmptyFunctionNameValidArgsValidFactoryAuthShouldPass() throws AuthorizationException {
        RequestFactory<TestOutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        TestOutstandingRequest expected = new TestOutstandingRequest("", new DRPCRequest("args", "1"));
        Mockito.when(factory.mkRequest(eq(""), any(DRPCRequest.class))).thenReturn(expected);

        TestOutstandingRequest actual = drpcAuthOk.execute("", "args", factory);

        Assert.assertSame(expected, actual);
    }

    /** Test execute method with functionName = null. Expected = throws IllegalArgumentException when the request queue is selected. */
    @SuppressWarnings("unchecked")
    // @Test
    public void executeNullFunctionNameValidArgsValidFactoryAuthThrowsIllegalArgumentException() {
        RequestFactory<TestOutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        Mockito.when(factory.mkRequest(eq(null), any(DRPCRequest.class)))
            .thenReturn(new TestOutstandingRequest(null, new DRPCRequest("args", "1")));

        Assert.assertThrows(IllegalArgumentException.class, () -> drpcAuthOk.execute(null, "args", factory));
    }

    /** Test execute method with valid functionName, valid args and null factory. Expected = throws NullPointerException. */
    @Test
    public void executeValidFunctionNameValidArgsNullFactoryAuthThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> drpcAuthOk.execute("try", "args", null));
    }

    /** Test execute method with valid functionName, valid args and factory returning null. Expected = throws NullPointerException because null requests cannot be stored. */
    @SuppressWarnings("unchecked")
    @Test
    public void executeValidFunctionNameValidArgsInvalidFactoryAuthThrowsNullPointerException() {
        RequestFactory<TestOutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(null);

        Assert.assertThrows(NullPointerException.class, () -> drpcAuthOk.execute("try", "args", factory));
    }

    /** Test execute method with valid inputs and state not authorized. Expected = throws AuthorizationException and factory is not invoked. */
    @SuppressWarnings("unchecked")
    @Test
    public void executeValidFunctionNameValidArgsNotAuthorizedThrowsAuthorizationException() {
        RequestFactory<TestOutstandingRequest> factory = Mockito.mock(RequestFactory.class);

        Assert.assertThrows(AuthorizationException.class, () -> drpcAuthKo.execute("try", "args", factory));
        Mockito.verifyNoInteractions(factory);
    }

    /** Test fetchRequest method with functionName = "try" after execute and authorized state. Expected = previously generated DRPCRequest. */
    @SuppressWarnings("unchecked")
    @Test
    public void fetchRequestCorrectFunctionNameAuthShouldReturnGeneratedRequest() throws AuthorizationException {
        RequestFactory<TestOutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        DRPCRequest expectedRequest = new DRPCRequest("args", "1");
        TestOutstandingRequest outstandingRequest = new TestOutstandingRequest("try", expectedRequest);
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
        drpcAuthOk.execute("try", "args", factory);

        DRPCRequest actualRequest = drpcAuthOk.fetchRequest("try");

        Assert.assertEquals(expectedRequest, actualRequest);
        Assert.assertTrue(outstandingRequest.wasFetched());
    }

    /** Test fetchRequest method with functionName = "unknown" and authorized state. Expected = DRPCRequest("", ""). */
    @Test
    public void fetchRequestNotCorrectFunctionNameAuthShouldReturnNothingRequest() throws AuthorizationException {
        DRPCRequest actualRequest = drpcAuthOk.fetchRequest("unknown");

        Assert.assertEquals(new DRPCRequest("", ""), actualRequest);
    }

    /** Test fetchRequest method with empty functionName and authorized state. Expected = DRPCRequest("", "") when no request is queued. */
    @Test
    public void fetchRequestEmptyFunctionNameAuthShouldReturnNothingRequest() throws AuthorizationException {
        DRPCRequest actualRequest = drpcAuthOk.fetchRequest("");

        Assert.assertEquals(new DRPCRequest("", ""), actualRequest);
    }

    /** Test fetchRequest method with null functionName and authorized state. Expected = throws IllegalArgumentException. */
    @Test
    public void fetchRequestNullFunctionNameAuthThrowsIllegalArgumentException() {
        Assert.assertThrows(IllegalArgumentException.class, () -> drpcAuthOk.fetchRequest(null));
    }

    /** Test fetchRequest method with functionName = "try" and state not authorized. Expected = throws AuthorizationException. */
    @Test
    public void fetchRequestCorrectFunctionNameNotAuthorizedThrowsAuthorizationException() {
        Assert.assertThrows(AuthorizationException.class, () -> drpcAuthKo.fetchRequest("try"));
    }

    /** Test returnResult method with existing id, result = "done" and authorized state. Expected = result delivered to the outstanding request. */
    @SuppressWarnings("unchecked")
    @Test
    public void returnResultCorrectIdValidResultAuthShouldPass() throws AuthorizationException {
        RequestFactory<TestOutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        TestOutstandingRequest outstandingRequest = new TestOutstandingRequest("try", new DRPCRequest("args", "1"));
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
        drpcAuthOk.execute("try", "args", factory);

        drpcAuthOk.returnResult("1", "done");

        Assert.assertEquals("done", outstandingRequest.returnedResult);
    }

    /** Test returnResult method with existing id, result = "" and authorized state. Expected = empty result delivered to the outstanding request. */
    @SuppressWarnings("unchecked")
    @Test
    public void returnResultCorrectIdEmptyResultAuthShouldPass() throws AuthorizationException {
        RequestFactory<TestOutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        TestOutstandingRequest outstandingRequest = new TestOutstandingRequest("try", new DRPCRequest("args", "1"));
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
        drpcAuthOk.execute("try", "args", factory);

        drpcAuthOk.returnResult("1", "");

        Assert.assertEquals("", outstandingRequest.returnedResult);
    }

    /** Test returnResult method with existing id, result = null and authorized state. Expected = null result delivered to the outstanding request. */
    @SuppressWarnings("unchecked")
    @Test
    public void returnResultCorrectIdNullResultAuthShouldPass() throws AuthorizationException {
        RequestFactory<TestOutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        TestOutstandingRequest outstandingRequest = new TestOutstandingRequest("try", new DRPCRequest("args", "1"));
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
        drpcAuthOk.execute("try", "args", factory);

        drpcAuthOk.returnResult("1", null);

        Assert.assertNull(outstandingRequest.returnedResult);
    }

    /** Test returnResult method with not existing id and authorized state. Expected = no exception and no delivered result. */
    @Test
    public void returnResultNotCorrectIdAuthShouldDoNothing() throws AuthorizationException {
        drpcAuthOk.returnResult("999", "done");
    }

    /** Test returnResult method with null id and authorized state. Expected = throws NullPointerException. */
    @Test
    public void returnResultNullIdValidResultAuthThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> drpcAuthOk.returnResult(null, "done"));
    }

    /** Test returnResult method with existing id and authorization denied for the result operation. Expected = throws AuthorizationException. */
    @SuppressWarnings("unchecked")
    @Test
    public void returnResultCorrectIdValidResultNotAuthorizedThrowsAuthorizationException() throws AuthorizationException {
        DRPC drpc = new DRPC(metricsRegistry, authorizerPermittingOnly("execute"), LONG_TIMEOUT_MS);
        RequestFactory<TestOutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        TestOutstandingRequest outstandingRequest = new TestOutstandingRequest("try", new DRPCRequest("args", "1"));
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
        try {
            drpc.execute("try", "args", factory);

            Assert.assertThrows(AuthorizationException.class, () -> drpc.returnResult("1", "done"));
        } finally {
            drpc.close();
        }
    }

    /** Test failRequest method with existing id, valid DRPCExecutionException and authorized state. Expected = exception delivered to the outstanding request. */
    @SuppressWarnings("unchecked")
    @Test
    public void failRequestCorrectIdValidExceptionAuthShouldPass() throws AuthorizationException {
        RequestFactory<TestOutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        TestOutstandingRequest outstandingRequest = new TestOutstandingRequest("try", new DRPCRequest("args", "1"));
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
        drpcAuthOk.execute("try", "args", factory);
        DRPCExecutionException failure = new DRPCExecutionException();
        failure.set_msg("failed");

        drpcAuthOk.failRequest("1", failure);

        Assert.assertSame(failure, outstandingRequest.failure);
        Assert.assertEquals("failed", outstandingRequest.failure.get_msg());
    }

    /** Test failRequest method with existing id and null exception. Expected = default failed exception delivered. */
    @SuppressWarnings("unchecked")
    @Test
    public void failRequestCorrectIdNullExceptionAuthShouldUseDefaultFailure() throws AuthorizationException {
        RequestFactory<TestOutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        TestOutstandingRequest outstandingRequest = new TestOutstandingRequest("try", new DRPCRequest("args", "1"));
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
        drpcAuthOk.execute("try", "args", factory);

        drpcAuthOk.failRequest("1", null);

        Assert.assertNotNull(outstandingRequest.failure);
        Assert.assertEquals("Request failed", outstandingRequest.failure.get_msg());
    }

    /** Test failRequest method with existing id and exception without message. Expected = same exception delivered. */
    @SuppressWarnings("unchecked")
    @Test
    public void failRequestCorrectIdExceptionWithoutMessageAuthShouldPass() throws AuthorizationException {
        RequestFactory<TestOutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        TestOutstandingRequest outstandingRequest = new TestOutstandingRequest("try", new DRPCRequest("args", "1"));
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
        drpcAuthOk.execute("try", "args", factory);
        DRPCExecutionException failure = new DRPCExecutionException();

        drpcAuthOk.failRequest("1", failure);

        Assert.assertSame(failure, outstandingRequest.failure);
        Assert.assertNull(outstandingRequest.failure.get_msg());
    }

    /** Test failRequest method with not existing id and authorized state. Expected = no exception. */
    @Test
    public void failRequestNotCorrectIdAuthShouldDoNothing() throws AuthorizationException {
        DRPCExecutionException failure = new DRPCExecutionException();
        failure.set_msg("failed");

        drpcAuthOk.failRequest("999", failure);
    }

    /** Test failRequest method with null id and authorized state. Expected = throws NullPointerException. */
    @Test
    public void failRequestNullIdValidExceptionAuthThrowsNullPointerException() {
        DRPCExecutionException failure = new DRPCExecutionException();
        failure.set_msg("failed");

        Assert.assertThrows(NullPointerException.class, () -> drpcAuthOk.failRequest(null, failure));
    }

    /** Test failRequest method with existing id and authorization denied for the failRequest operation. Expected = throws AuthorizationException. */
    @SuppressWarnings("unchecked")
    @Test
    public void failRequestCorrectIdValidExceptionNotAuthorizedThrowsAuthorizationException() throws AuthorizationException {
        DRPC drpc = new DRPC(metricsRegistry, authorizerPermittingOnly("execute"), LONG_TIMEOUT_MS);
        RequestFactory<TestOutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        TestOutstandingRequest outstandingRequest = new TestOutstandingRequest("try", new DRPCRequest("args", "1"));
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
        DRPCExecutionException failure = new DRPCExecutionException();
        failure.set_msg("failed");
        try {
            drpc.execute("try", "args", factory);

            Assert.assertThrows(AuthorizationException.class, () -> drpc.failRequest("1", failure));
        } finally {
            drpc.close();
        }
    }

    /** Test executeBlocking method with functionName = "try", funcArgs = "args" and successful returnResult. Expected = returns "done". */
    @Test
    public void executeBlockingValidFunctionNameValidArgsAuthReturnResultShouldReturnDone() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> future = executor.submit(() -> drpcAuthOk.executeBlocking("try", "args"));
            DRPCRequest request = waitAndFetchRequest(drpcAuthOk, "try");

            drpcAuthOk.returnResult(request.get_request_id(), "done");

            Assert.assertEquals("done", future.get(2, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }

    /** Test executeBlocking method with functionName = "try", funcArgs = "args" and failed request. Expected = throws DRPCExecutionException containing "failed". */
    @Test
    public void executeBlockingValidFunctionNameValidArgsAuthFailRequestThrowsDRPCExecutionException() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> future = executor.submit(() -> drpcAuthOk.executeBlocking("try", "args"));
            DRPCRequest request = waitAndFetchRequest(drpcAuthOk, "try");
            DRPCExecutionException failure = new DRPCExecutionException();
            failure.set_msg("failed");

            drpcAuthOk.failRequest(request.get_request_id(), failure);

            ExecutionException actual = Assert.assertThrows(ExecutionException.class, () -> future.get(2, TimeUnit.SECONDS));
            Assert.assertTrue(actual.getCause() instanceof DRPCExecutionException);
            Assert.assertEquals("failed", ((DRPCExecutionException) actual.getCause()).get_msg());
        } finally {
            executor.shutdownNow();
        }
    }

    /** Test executeBlocking method with functionName = "try", funcArgs = "args" and state not authorized. Expected = throws AuthorizationException. */
    @Test
    public void executeBlockingValidFunctionNameValidArgsNotAuthorizedThrowsAuthorizationException() {
        Assert.assertThrows(AuthorizationException.class, () -> drpcAuthKo.executeBlocking("try", "args"));
    }

    /** Test executeBlocking method with functionName = null. Expected = throws IllegalArgumentException. */
    // @Test
    public void executeBlockingNullFunctionNameValidArgsAuthThrowsIllegalArgumentException() {
        Assert.assertThrows(IllegalArgumentException.class, () -> drpcAuthOk.executeBlocking(null, "args"));
    }

    /** Test executeBlocking method with functionName = "try", funcArgs = "args" and timeout. Expected = throws DRPCExecutionException with message "Timed Out". */
    @Test
    public void executeBlockingValidFunctionNameValidArgsAuthTimeoutThrowsDRPCExecutionException() throws Exception {
        DRPC timedDrpc = new DRPC(metricsRegistry, alwaysAuthorized, SHORT_TIMEOUT_MS);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> future = executor.submit(() -> timedDrpc.executeBlocking("try", "args"));

            ExecutionException actual = Assert.assertThrows(ExecutionException.class, () -> future.get(3, TimeUnit.SECONDS));
            Assert.assertTrue(actual.getCause() instanceof DRPCExecutionException);
            Assert.assertEquals("Timed Out", ((DRPCExecutionException) actual.getCause()).get_msg());
        } finally {
            timedDrpc.close();
            executor.shutdownNow();
        }
    }

    /** Test fetchRequest after timeout cleanup. Expected = DRPCRequest("", "") because the pending request has been removed. */
    @SuppressWarnings("unchecked")
    @Test
    public void fetchRequestCorrectFunctionNameAuthAfterTimeoutShouldReturnNothingRequest() throws Exception {
        DRPC timedDrpc = new DRPC(metricsRegistry, alwaysAuthorized, SHORT_TIMEOUT_MS);
        RequestFactory<TestOutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        TestOutstandingRequest outstandingRequest = new TestOutstandingRequest("try", new DRPCRequest("args", "1"));
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
        try {
            timedDrpc.execute("try", "args", factory);
            Thread.sleep(700L);

            DRPCRequest actual = timedDrpc.fetchRequest("try");

            Assert.assertEquals(new DRPCRequest("", ""), actual);
            Assert.assertNotNull(outstandingRequest.failure);
            Assert.assertEquals("Timed Out", outstandingRequest.failure.get_msg());
        } finally {
            timedDrpc.close();
        }
    }

    /** Test close method with outstanding blocking request. Expected = blocked request fails with server shutdown exception. */
    @Test
    public void closeWithOutstandingBlockingRequestShouldFailRequestWithShutdownException() throws Exception {
        DRPC drpc = new DRPC(metricsRegistry, alwaysAuthorized, LONG_TIMEOUT_MS);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> future = executor.submit(() -> drpc.executeBlocking("try", "args"));
            waitAndFetchRequest(drpc, "try");

            drpc.close();

            ExecutionException actual = Assert.assertThrows(ExecutionException.class, () -> future.get(2, TimeUnit.SECONDS));
            Assert.assertTrue(actual.getCause() instanceof DRPCExecutionException);
            Assert.assertEquals("Server Shutting Down", ((DRPCExecutionException) actual.getCause()).get_msg());
        } finally {
            executor.shutdownNow();
            drpc.close();
        }
    }

    /** Test execute method without authorization plugin. Expected = request is accepted. */
    @SuppressWarnings("unchecked")
    @Test
    public void executeValidFunctionNameValidArgsNullAuthorizerShouldPass() throws AuthorizationException {
        RequestFactory<TestOutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        TestOutstandingRequest expected = new TestOutstandingRequest("try", new DRPCRequest("args", "1"));
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(expected);

        TestOutstandingRequest actual = drpcNoAuth.execute("try", "args", factory);

        Assert.assertSame(expected, actual);
    }

    /** Test constructor with null metrics registry. Expected = throws NullPointerException because metrics meters cannot be registered. */
    @Test
    public void constructorNullMetricsRegistryThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> new DRPC(null, alwaysAuthorized, LONG_TIMEOUT_MS));
    }

    /** Test constructor with null conf map. Expected = throws NullPointerException. */
    @Test
    public void constructorNullConfThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> new DRPC(metricsRegistry, (Map<String, Object>) null));
    }


    private IAuthorizer authorizerPermittingOnly(final String permittedOperation) {
        return new IAuthorizer() {
            @Override
            public void prepare(Map<String, Object> conf) {
            }

            @Override
            public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
                return permittedOperation.equals(operation);
            }
        };
    }

    private DRPCRequest waitAndFetchRequest(DRPC drpc, String functionName) throws Exception {
        long deadline = System.currentTimeMillis() + 2000L;
        while (System.currentTimeMillis() < deadline) {
            DRPCRequest request = drpc.fetchRequest(functionName);
            if (!new DRPCRequest("", "").equals(request)) {
                return request;
            }
            Thread.sleep(20L);
        }
        throw new TimeoutException("Request was not queued in time");
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
        public void fail(DRPCExecutionException exception) {
            this.failure = exception;
        }
    }


    // ### Test END ###
}
