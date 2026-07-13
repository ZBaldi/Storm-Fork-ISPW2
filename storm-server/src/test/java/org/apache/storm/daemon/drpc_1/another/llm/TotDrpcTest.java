package org.apache.storm.daemon.drpc_1.another.llm;

import com.codahale.metrics.Meter;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.storm.daemon.drpc.OutstandingRequest;
import org.apache.storm.daemon.drpc.RequestFactory;
import org.apache.storm.daemon.drpc.refactored.one.DRPC;
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
import org.mockito.Mockito;

public class TotDrpcTest {
    // ### Test START ###

    private StormMetricsRegistry metricsRegistry;
    private Meter meter;
    private DRPC drpcAuthOk;
    private DRPC drpcAuthKo;
    private DRPC drpcNoAuth;
    private DRPC drpcNotValid;

    @Before
    public void setUp() {
        metricsRegistry = Mockito.mock(StormMetricsRegistry.class);
        meter = Mockito.mock(Meter.class);
        Mockito.when(metricsRegistry.registerMeter(Mockito.anyString())).thenReturn(meter);

        drpcAuthOk = new DRPC(metricsRegistry, new AlwaysAuthorizer(), 1000);
        drpcAuthKo = new DRPC(metricsRegistry, new NeverAuthorizer(), 1000);
        drpcNoAuth = new DRPC(metricsRegistry, null, 1000);

        StormMetricsRegistry invalidMetricsRegistry = Mockito.mock(StormMetricsRegistry.class);
        Mockito.when(invalidMetricsRegistry.registerMeter(Mockito.anyString())).thenReturn(null);
        drpcNotValid = new DRPC(invalidMetricsRegistry, new AlwaysAuthorizer(), 1000);
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
        if (drpcNotValid != null) {
            drpcNotValid.close();
        }
    }

    /** Test checkAuthorization method with valid context, always authorized authorizer, operation = "execute" and function = "try". Expected = no exception. */
    @Test
    public void checkAuthorizationAlwaysAuthorizedShouldPass() throws AuthorizationException {
        DRPC.checkAuthorization(ReqContext.context(), new AlwaysAuthorizer(), "execute", "try");
    }

    /** Test checkAuthorization method with valid context, never authorized authorizer, operation = "execute" and function = "try". Expected = throws AuthorizationException. */
    @Test
    public void checkAuthorizationNeverAuthorizedThrowsAuthorizationException() {
        Assert.assertThrows(AuthorizationException.class,
            () -> DRPC.checkAuthorization(ReqContext.context(), new NeverAuthorizer(), "execute", "try"));
    }

    /** Test checkAuthorization method with null authorizer. Expected = no exception. */
    @Test
    public void checkAuthorizationNullAuthorizerShouldPass() throws AuthorizationException {
        DRPC.checkAuthorization(ReqContext.context(), null, "execute", "try");
    }

    /** Test checkAuthorization method with invalid authorizer. Expected = throws RuntimeException. */
    @Test
    public void checkAuthorizationInvalidAuthorizerThrowsRuntimeException() {
        Assert.assertThrows(RuntimeException.class,
            () -> DRPC.checkAuthorization(ReqContext.context(), new ThrowingAuthorizer(), "execute", "try"));
    }

    /** Test execute method with functionName = "try", funcArgs = "args", valid factory and state authorized. Expected = generated OutstandingRequest. */
    @SuppressWarnings("unchecked")
    @Test
    public void executeValidFunctionNameValidFuncArgsValidFactoryAuthShouldPass() throws AuthorizationException {
        RequestFactory<DoNothingOutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        DoNothingOutstandingRequest expectedRequest = new DoNothingOutstandingRequest("try", new DRPCRequest("args", "1"));
        Mockito.when(factory.mkRequest(Mockito.eq("try"), Mockito.any(DRPCRequest.class))).thenReturn(expectedRequest);

        OutstandingRequest actual = drpcAuthOk.execute("try", "args", factory);

        Assert.assertSame(expectedRequest, actual);
        Assert.assertEquals("try", actual.getFunction());
        Assert.assertEquals(new DRPCRequest("args", "1"), actual.getRequest());
    }

    /** Test execute method with functionName = "try", funcArgs = null, valid factory and state authorized. Expected = OutstandingRequest with null args and request id 1. */
    @SuppressWarnings("unchecked")
    @Test
    public void executeValidFunctionNameNullFuncArgsValidFactoryAuthShouldPass() throws AuthorizationException {
        RequestFactory<DoNothingOutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        DoNothingOutstandingRequest expectedRequest = new DoNothingOutstandingRequest("try", new DRPCRequest(null, "1"));
        Mockito.when(factory.mkRequest(Mockito.eq("try"), Mockito.any(DRPCRequest.class))).thenReturn(expectedRequest);

        OutstandingRequest actual = drpcAuthOk.execute("try", null, factory);

        Assert.assertSame(expectedRequest, actual);
        Assert.assertEquals(new DRPCRequest(null, "1"), actual.getRequest());
    }

    /** Test execute method with functionName = "", funcArgs = "args", valid factory and state authorized. Expected = request accepted for empty function name. */
    @SuppressWarnings("unchecked")
    @Test
    public void executeEmptyFunctionNameValidFuncArgsAuthShouldPass() throws AuthorizationException {
        RequestFactory<DoNothingOutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        DoNothingOutstandingRequest expectedRequest = new DoNothingOutstandingRequest("", new DRPCRequest("args", "1"));
        Mockito.when(factory.mkRequest(Mockito.eq(""), Mockito.any(DRPCRequest.class))).thenReturn(expectedRequest);

        OutstandingRequest actual = drpcAuthOk.execute("", "args", factory);

        Assert.assertSame(expectedRequest, actual);
        Assert.assertEquals("", actual.getFunction());
    }

    /** Test execute method with functionName = null, funcArgs = "args", valid factory and state authorized. Expected = throws IllegalArgumentException. */
    @SuppressWarnings("unchecked")
    @Test
    public void executeNullFunctionNameValidFuncArgsAuthThrowsIllegalArgumentException() throws Exception {
        RequestFactory<DoNothingOutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        Mockito.when(factory.mkRequest(Mockito.isNull(), Mockito.any(DRPCRequest.class)))
            .thenReturn(new DoNothingOutstandingRequest(null, new DRPCRequest("args", "1")));

        try {
            Assert.assertThrows(IllegalArgumentException.class, () -> drpcAuthOk.execute(null, "args", factory));
        } finally {
            clearRequestStore(drpcAuthOk);
        }
    }

    /** Test execute method with functionName = "try", funcArgs = "args", factory = null and state authorized. Expected = throws NullPointerException. */
    @Test
    public void executeValidFunctionNameValidFuncArgsNullFactoryAuthThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> drpcAuthOk.execute("try", "args", null));
    }

    /** Test execute method with functionName = "try", funcArgs = "args", factory returning null and state authorized. Expected = throws NullPointerException. */
    @SuppressWarnings("unchecked")
    @Test
    public void executeValidFunctionNameValidFuncArgsInvalidFactoryAuthThrowsNullPointerException() {
        RequestFactory<DoNothingOutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        Mockito.when(factory.mkRequest(Mockito.eq("try"), Mockito.any(DRPCRequest.class))).thenReturn(null);

        Assert.assertThrows(NullPointerException.class, () -> drpcAuthOk.execute("try", "args", factory));
    }

    /** Test execute method with functionName = "try", funcArgs = "args" and state not authorized. Expected = throws AuthorizationException. */
    @SuppressWarnings("unchecked")
    @Test
    public void executeValidFunctionNameValidFuncArgsNotAuthThrowsAuthorizationException() {
        RequestFactory<DoNothingOutstandingRequest> factory = Mockito.mock(RequestFactory.class);

        Assert.assertThrows(AuthorizationException.class, () -> drpcAuthKo.execute("try", "args", factory));
    }

    /** Test execute method with valid inputs and invalid state. Expected = throws NullPointerException. */
    @SuppressWarnings("unchecked")
    @Test
    public void executeValidInputsInvalidStateThrowsNullPointerException() {
        RequestFactory<DoNothingOutstandingRequest> factory = Mockito.mock(RequestFactory.class);

        Assert.assertThrows(NullPointerException.class, () -> drpcNotValid.execute("try", "args", factory));
    }

    /** Test fetchRequest method with functionName = "try" after execute and state authorized. Expected = DRPCRequest previously generated by execute. */
    @SuppressWarnings("unchecked")
    @Test
    public void fetchRequestCorrectFunctionNameAuthShouldReturnGeneratedRequest() throws AuthorizationException {
        RequestFactory<DoNothingOutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        DoNothingOutstandingRequest outstandingRequest = new DoNothingOutstandingRequest("try", new DRPCRequest("args", "1"));
        Mockito.when(factory.mkRequest(Mockito.eq("try"), Mockito.any(DRPCRequest.class))).thenReturn(outstandingRequest);
        drpcAuthOk.execute("try", "args", factory);

        DRPCRequest actualRequest = drpcAuthOk.fetchRequest("try");

        Assert.assertEquals(new DRPCRequest("args", "1"), actualRequest);
        Assert.assertTrue(outstandingRequest.wasFetched());
    }

    /** Test fetchRequest method with functionName = "missing" and state authorized. Expected = empty DRPCRequest. */
    @Test
    public void fetchRequestNotCorrectFunctionNameAuthShouldReturnNothingRequest() throws AuthorizationException {
        DRPCRequest actualRequest = drpcAuthOk.fetchRequest("missing");

        Assert.assertEquals(new DRPCRequest("", ""), actualRequest);
    }

    /** Test fetchRequest method with functionName = "" and state authorized. Expected = empty DRPCRequest. */
    @Test
    public void fetchRequestEmptyFunctionNameAuthShouldReturnNothingRequest() throws AuthorizationException {
        DRPCRequest actualRequest = drpcAuthOk.fetchRequest("");

        Assert.assertEquals(new DRPCRequest("", ""), actualRequest);
    }

    /** Test fetchRequest method with functionName = null and state authorized. Expected = throws IllegalArgumentException. */
    @Test
    public void fetchRequestNullFunctionNameAuthThrowsIllegalArgumentException() {
        Assert.assertThrows(IllegalArgumentException.class, () -> drpcAuthOk.fetchRequest(null));
    }

    /** Test fetchRequest method with functionName = "try" and state not authorized. Expected = throws AuthorizationException. */
    @Test
    public void fetchRequestCorrectFunctionNameNotAuthThrowsAuthorizationException() {
        Assert.assertThrows(AuthorizationException.class, () -> drpcAuthKo.fetchRequest("try"));
    }

    /** Test fetchRequest method with valid input and invalid state. Expected = throws NullPointerException. */
    @Test
    public void fetchRequestValidFunctionNameInvalidStateThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> drpcNotValid.fetchRequest("try"));
    }

    /** Test fetchRequest method with functionName = "try" after timeout and state authorized. Expected = empty DRPCRequest. */
    @SuppressWarnings("unchecked")
    @Test
    public void fetchRequestCorrectFunctionNameAuthTimerTimeoutShouldPass() throws Exception {
        RequestFactory<DoNothingOutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        DoNothingOutstandingRequest outstandingRequest = new DoNothingOutstandingRequest("try", new DRPCRequest("args", "1"));
        Mockito.when(factory.mkRequest(Mockito.eq("try"), Mockito.any(DRPCRequest.class))).thenReturn(outstandingRequest);
        drpcAuthOk.execute("try", "args", factory);

        Thread.sleep(1500);
        DRPCRequest actualRequest = drpcAuthOk.fetchRequest("try");

        Assert.assertEquals(new DRPCRequest("", ""), actualRequest);
        Assert.assertNotNull(outstandingRequest.getFailure());
        Assert.assertEquals("Timed Out", outstandingRequest.getFailure().get_msg());
    }

    /** Test returnResult method with existing id = "1", result = "done" and state authorized. Expected = result stored in OutstandingRequest. */
    @SuppressWarnings("unchecked")
    @Test
    public void returnResultCorrectIdValidResultAuthShouldPass() throws AuthorizationException {
        RequestFactory<DoNothingOutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        DoNothingOutstandingRequest outstandingRequest = new DoNothingOutstandingRequest("try", new DRPCRequest("args", "1"));
        Mockito.when(factory.mkRequest(Mockito.eq("try"), Mockito.any(DRPCRequest.class))).thenReturn(outstandingRequest);
        drpcAuthOk.execute("try", "args", factory);

        drpcAuthOk.returnResult("1", "done");

        Assert.assertEquals("done", outstandingRequest.getReturnedResult());
    }

    /** Test returnResult method with existing id = "1", result = "" and state authorized. Expected = empty result stored. */
    @SuppressWarnings("unchecked")
    @Test
    public void returnResultCorrectIdEmptyResultAuthShouldPass() throws AuthorizationException {
        RequestFactory<DoNothingOutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        DoNothingOutstandingRequest outstandingRequest = new DoNothingOutstandingRequest("try", new DRPCRequest("args", "1"));
        Mockito.when(factory.mkRequest(Mockito.eq("try"), Mockito.any(DRPCRequest.class))).thenReturn(outstandingRequest);
        drpcAuthOk.execute("try", "args", factory);

        drpcAuthOk.returnResult("1", "");

        Assert.assertEquals("", outstandingRequest.getReturnedResult());
    }

    /** Test returnResult method with missing id = "missing", result = "done" and state authorized. Expected = no exception. */
    @Test
    public void returnResultNotCorrectIdValidResultAuthShouldPass() throws AuthorizationException {
        drpcAuthOk.returnResult("missing", "done");
    }

    /** Test returnResult method with id = null, result = "" and state authorized. Expected = throws NullPointerException. */
    @Test
    public void returnResultNullIdEmptyResultAuthThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> drpcAuthOk.returnResult(null, ""));
    }

    /** Test returnResult method with existing id = "1", result = "done" and state not authorized. Expected = throws AuthorizationException. */
    @SuppressWarnings("unchecked")
    @Test
    public void returnResultCorrectIdValidResultNotAuthThrowsAuthorizationException() throws Exception {
        DoNothingOutstandingRequest outstandingRequest = new DoNothingOutstandingRequest("try", new DRPCRequest("args", "1"));
        insertRequest(drpcAuthKo, "1", "try", outstandingRequest);

        Assert.assertThrows(AuthorizationException.class, () -> drpcAuthKo.returnResult("1", "done"));
    }

    /** Test returnResult method with valid input and invalid state. Expected = throws NullPointerException. */
    @Test
    public void returnResultValidInputsInvalidStateThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> drpcNotValid.returnResult("1", "done"));
    }

    /** Test failRequest method with existing id = "1", valid exception message = "failed" and state authorized. Expected = failure stored. */
    @SuppressWarnings("unchecked")
    @Test
    public void failRequestCorrectIdValidExceptionAuthShouldPass() throws AuthorizationException {
        RequestFactory<DoNothingOutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        DoNothingOutstandingRequest outstandingRequest = new DoNothingOutstandingRequest("try", new DRPCRequest("args", "1"));
        Mockito.when(factory.mkRequest(Mockito.eq("try"), Mockito.any(DRPCRequest.class))).thenReturn(outstandingRequest);
        drpcAuthOk.execute("try", "args", factory);
        DRPCExecutionException failure = new DRPCExecutionException("failed");

        drpcAuthOk.failRequest("1", failure);

        Assert.assertSame(failure, outstandingRequest.getFailure());
        Assert.assertEquals("failed", outstandingRequest.getFailure().get_msg());
    }

    /** Test failRequest method with existing id = "1", exception without message and state authorized. Expected = failure with null message stored. */
    @SuppressWarnings("unchecked")
    @Test
    public void failRequestCorrectIdInvalidExceptionAuthShouldPass() throws AuthorizationException {
        RequestFactory<DoNothingOutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        DoNothingOutstandingRequest outstandingRequest = new DoNothingOutstandingRequest("try", new DRPCRequest("args", "1"));
        Mockito.when(factory.mkRequest(Mockito.eq("try"), Mockito.any(DRPCRequest.class))).thenReturn(outstandingRequest);
        drpcAuthOk.execute("try", "args", factory);
        DRPCExecutionException failure = new DRPCExecutionException();

        drpcAuthOk.failRequest("1", failure);

        Assert.assertSame(failure, outstandingRequest.getFailure());
        Assert.assertNull(outstandingRequest.getFailure().get_msg());
    }

    /** Test failRequest method with existing id = "1", null exception and state authorized. Expected = default failure message. */
    @SuppressWarnings("unchecked")
    @Test
    public void failRequestCorrectIdNullExceptionAuthShouldUseDefaultFailure() throws AuthorizationException {
        RequestFactory<DoNothingOutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        DoNothingOutstandingRequest outstandingRequest = new DoNothingOutstandingRequest("try", new DRPCRequest("args", "1"));
        Mockito.when(factory.mkRequest(Mockito.eq("try"), Mockito.any(DRPCRequest.class))).thenReturn(outstandingRequest);
        drpcAuthOk.execute("try", "args", factory);

        drpcAuthOk.failRequest("1", null);

        Assert.assertNotNull(outstandingRequest.getFailure());
        Assert.assertEquals("Request failed", outstandingRequest.getFailure().get_msg());
    }

    /** Test failRequest method with missing id = "missing", valid exception and state authorized. Expected = no exception. */
    @Test
    public void failRequestNotCorrectIdValidExceptionAuthShouldPass() throws AuthorizationException {
        drpcAuthOk.failRequest("missing", new DRPCExecutionException("failed"));
    }

    /** Test failRequest method with id = null, valid exception and state authorized. Expected = throws NullPointerException. */
    @Test
    public void failRequestNullIdValidExceptionAuthThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class,
            () -> drpcAuthOk.failRequest(null, new DRPCExecutionException("failed")));
    }

    /** Test failRequest method with existing id = "1", valid exception and state not authorized. Expected = throws AuthorizationException. */
    @SuppressWarnings("unchecked")
    @Test
    public void failRequestCorrectIdValidExceptionNotAuthThrowsAuthorizationException() throws Exception {
        DoNothingOutstandingRequest outstandingRequest = new DoNothingOutstandingRequest("try", new DRPCRequest("args", "1"));
        insertRequest(drpcAuthKo, "1", "try", outstandingRequest);

        Assert.assertThrows(AuthorizationException.class,
            () -> drpcAuthKo.failRequest("1", new DRPCExecutionException("failed")));
    }

    /** Test failRequest method with valid input and invalid state. Expected = throws NullPointerException. */
    @Test
    public void failRequestValidInputsInvalidStateThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class,
            () -> drpcNotValid.failRequest("1", new DRPCExecutionException("failed")));
    }

    /** Test executeBlocking method with functionName = "try", funcArgs = "args" and successful returnResult. Expected = "done". */
    @Test
    public void executeBlockingValidFunctionNameValidFuncArgsAuthReturnResultShouldPass() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> future = executor.submit(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    return drpcAuthOk.executeBlocking("try", "args");
                }
            });

            DRPCRequest request = waitForRequest(drpcAuthOk, "try");
            drpcAuthOk.returnResult(request.get_request_id(), "done");

            Assert.assertEquals("done", future.get(3, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }

    /** Test executeBlocking method with functionName = "try", funcArgs = "args" and failRequest message = "failed". Expected = throws DRPCExecutionException with message "failed". */
    @Test
    public void executeBlockingValidFunctionNameValidFuncArgsAuthFailRequestThrowsDRPCExecutionException() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> future = executor.submit(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    return drpcAuthOk.executeBlocking("try", "args");
                }
            });

            DRPCRequest request = waitForRequest(drpcAuthOk, "try");
            drpcAuthOk.failRequest(request.get_request_id(), new DRPCExecutionException("failed"));

            try {
                future.get(3, TimeUnit.SECONDS);
                Assert.fail("Expected DRPCExecutionException");
            } catch (java.util.concurrent.ExecutionException exception) {
                Assert.assertTrue(exception.getCause() instanceof DRPCExecutionException);
                Assert.assertEquals("failed", ((DRPCExecutionException) exception.getCause()).get_msg());
            }
        } finally {
            executor.shutdownNow();
        }
    }

    /** Test executeBlocking method with functionName = "try", funcArgs = "args" and state not authorized. Expected = throws AuthorizationException. */
    @Test
    public void executeBlockingValidFunctionNameValidFuncArgsNotAuthThrowsAuthorizationException() {
        Assert.assertThrows(AuthorizationException.class, () -> drpcAuthKo.executeBlocking("try", "args"));
    }

    /** Test executeBlocking method with functionName = null, funcArgs = "args" and state authorized. Expected = throws IllegalArgumentException. */
    @Test
    public void executeBlockingNullFunctionNameValidFuncArgsAuthThrowsIllegalArgumentException() throws Exception {
        try {
            Assert.assertThrows(IllegalArgumentException.class, () -> drpcAuthOk.executeBlocking(null, "args"));
        } finally {
            clearRequestStore(drpcAuthOk);
        }
    }

    /** Test executeBlocking method with functionName = "try", funcArgs = "args" and invalid state. Expected = throws NullPointerException. */
    @Test
    public void executeBlockingValidFunctionNameValidFuncArgsInvalidStateThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> drpcNotValid.executeBlocking("try", "args"));
    }

    /** Test close method with outstanding request. Expected = outstanding request completed with shutdown failure. */
    @SuppressWarnings("unchecked")
    @Test
    public void closeWithOutstandingRequestShouldFailRequestAsServerShutdown() throws AuthorizationException {
        RequestFactory<DoNothingOutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        DoNothingOutstandingRequest outstandingRequest = new DoNothingOutstandingRequest("try", new DRPCRequest("args", "1"));
        Mockito.when(factory.mkRequest(Mockito.eq("try"), Mockito.any(DRPCRequest.class))).thenReturn(outstandingRequest);
        drpcAuthOk.execute("try", "args", factory);

        drpcAuthOk.close();

        Assert.assertNotNull(outstandingRequest.getFailure());
        Assert.assertEquals("Server Shutting Down", outstandingRequest.getFailure().get_msg());
    }

    private static void insertRequest(final DRPC drpc, final String requestId, final String function,
                                      final OutstandingRequest request) throws Exception {
        Object requestStore = getRequestStore(drpc);
        java.lang.reflect.Method add = requestStore.getClass()
            .getDeclaredMethod("add", String.class, String.class, OutstandingRequest.class);
        add.setAccessible(true);
        add.invoke(requestStore, requestId, function, request);
    }

    private static void clearRequestStore(final DRPC drpc) throws Exception {
        Object requestStore = getRequestStore(drpc);
        java.lang.reflect.Field requests = requestStore.getClass().getDeclaredField("requests");
        java.lang.reflect.Field queues = requestStore.getClass().getDeclaredField("queues");
        requests.setAccessible(true);
        queues.setAccessible(true);
        ((Map<?, ?>) requests.get(requestStore)).clear();
        ((Map<?, ?>) queues.get(requestStore)).clear();
    }

    private static Object getRequestStore(final DRPC drpc) throws Exception {
        java.lang.reflect.Field requestStore = DRPC.class.getDeclaredField("requestStore");
        requestStore.setAccessible(true);
        return requestStore.get(drpc);
    }

    private static DRPCRequest waitForRequest(final DRPC drpc, final String functionName) throws Exception {
        DRPCRequest request = new DRPCRequest("", "");
        long deadline = System.currentTimeMillis() + 3000;
        while (System.currentTimeMillis() < deadline) {
            request = drpc.fetchRequest(functionName);
            if (!"".equals(request.get_request_id())) {
                return request;
            }
            Thread.sleep(25);
        }
        Assert.fail("No request fetched for function " + functionName);
        return request;
    }

    private static final class AlwaysAuthorizer implements IAuthorizer {
        public void prepare(final Map<String, Object> conf) {
        }

        @Override
        public boolean permit(final ReqContext context, final String operation, final Map<String, Object> topoConf) {
            return true;
        }
    }

    private static final class NeverAuthorizer implements IAuthorizer {
        public void prepare(final Map<String, Object> conf) {
        }

        @Override
        public boolean permit(final ReqContext context, final String operation, final Map<String, Object> topoConf) {
            return false;
        }
    }

    private static final class ThrowingAuthorizer implements IAuthorizer {
        public void prepare(final Map<String, Object> conf) {
        }

        @Override
        public boolean permit(final ReqContext context, final String operation, final Map<String, Object> topoConf) {
            throw new RuntimeException("invalid authorizer");
        }
    }

    private static final class DoNothingOutstandingRequest extends OutstandingRequest {
        private String returnedResult;
        private DRPCExecutionException failure;

        DoNothingOutstandingRequest(final String function, final DRPCRequest request) {
            super(function, request);
        }

        @Override
        public void returnResult(final String result) {
            this.returnedResult = result;
        }

        @Override
        public void fail(final DRPCExecutionException exception) {
            this.failure = exception;
        }

        String getReturnedResult() {
            return returnedResult;
        }

        DRPCExecutionException getFailure() {
            return failure;
        }
    }

    // ### Test END ###
}
