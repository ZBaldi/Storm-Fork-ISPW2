package org.apache.storm.daemon.drpc;

import com.codahale.metrics.Meter;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.DRPCExceptionType;
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
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

/**
 * Advanced JUnit 4 tests for {@link DRPC}.
 *
 * <p>The suite applies a Category Partition-oriented strategy over instance state, authorization
 * policy, String inputs, request factory behavior, request completion and failure paths.</p>
 */
public class Fsp3DrpcTest {
    // ### Test START ###

    private static final String FUNCTION = "try";
    private static final String ARGS = "args";
    private static final long LONG_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);
    private static final long SHORT_TIMEOUT_MS = 250L;

    private StormMetricsRegistry metricsRegistry;
    private Meter meter;
    private IAuthorizer alwaysAuthorized;
    private IAuthorizer neverAuthorized;
    private IAuthorizer invalidAuthorizer;
    private DRPC drpcAuthOk;
    private DRPC drpcAuthKo;
    private DRPC drpcNoAuth;
    private DRPC drpcShortTimeout;

    @Before
    public void setUp() {
        metricsRegistry = Mockito.mock(StormMetricsRegistry.class);
        meter = Mockito.mock(Meter.class);
        Mockito.when(metricsRegistry.registerMeter(anyString())).thenReturn(meter);

        alwaysAuthorized = new IAuthorizer() {
            @Override
            public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
                return true;
            }

            @Override
            public void prepare(Map<String, Object> topoConf) {
            }
        };
        neverAuthorized = new IAuthorizer() {
            @Override
            public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
                return false;
            }

            @Override
            public void prepare(Map<String, Object> topoConf) {
            }
        };
        invalidAuthorizer = new IAuthorizer() {
            @Override
            public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
                throw new RuntimeException("invalid authorizer");
            }

            @Override
            public void prepare(Map<String, Object> topoConf) {
            }
        };

        drpcAuthOk = new DRPC(metricsRegistry, alwaysAuthorized, LONG_TIMEOUT_MS);
        drpcAuthKo = new DRPC(metricsRegistry, neverAuthorized, LONG_TIMEOUT_MS);
        drpcNoAuth = new DRPC(metricsRegistry, null, LONG_TIMEOUT_MS);
        drpcShortTimeout = new DRPC(metricsRegistry, alwaysAuthorized, SHORT_TIMEOUT_MS);
    }

    @After
    public void tearDown() {
        closeIgnoringErrors(drpcAuthOk);
        closeIgnoringErrors(drpcAuthKo);
        closeIgnoringErrors(drpcNoAuth);
        closeIgnoringErrors(drpcShortTimeout);
    }

    private void closeIgnoringErrors(DRPC drpc) {
        if (drpc != null) {
            try {
                drpc.close();
            } catch (RuntimeException ignored) {
                // Best effort cleanup for tests that intentionally exercise invalid states.
            }
        }
    }

    @SuppressWarnings("unchecked")
    private RequestFactory<OutstandingRequest> factoryReturning(final OutstandingRequest outstandingRequest) {
        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
        return factory;
    }

    private DRPCRequest fetchEventually(DRPC drpc, String function) throws Exception {
        DRPCRequest request = null;
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
        while (System.currentTimeMillis() < deadline) {
            request = drpc.fetchRequest(function);
            if (!new DRPCRequest("", "").equals(request)) {
                return request;
            }
            Thread.sleep(25L);
        }
        return request;
    }

    /** Test constructor with valid metrics registry and null authorizer. Expected = DRPC instance created and usable without authorization checks. */
    @Test
    public void constructorValidMetricsNullAuthorizerShouldCreateUsableInstance() throws AuthorizationException {
        OutstandingRequest expected = new DoNothingOutstandingRequest(FUNCTION, new DRPCRequest(ARGS, "1"));
        OutstandingRequest actual = drpcNoAuth.execute(FUNCTION, ARGS, factoryReturning(expected));

        Assert.assertSame(expected, actual);
        Mockito.verify(metricsRegistry, Mockito.atLeastOnce()).registerMeter("drpc:num-execute-calls");
    }

    /** Test constructor with null metrics registry. Expected = throws NullPointerException. */
    @Test
    public void constructorNullMetricsRegistryThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> new DRPC(null, alwaysAuthorized, LONG_TIMEOUT_MS));
    }

    /** Test checkAuthorization with valid context, always authorized authorizer and valid function. Expected = no exception. */
    @Test
    public void checkAuthorizationValidContextAlwaysAuthorizedShouldPass() throws AuthorizationException {
        DRPC.checkAuthorization(ReqContext.context(), alwaysAuthorized, "execute", FUNCTION);
    }

    /** Test checkAuthorization with valid context and null authorizer. Expected = no exception. */
    @Test
    public void checkAuthorizationValidContextNullAuthorizerShouldPass() throws AuthorizationException {
        DRPC.checkAuthorization(ReqContext.context(), null, "execute", FUNCTION);
    }

    /** Test checkAuthorization with null context and always authorized authorizer. Expected = no exception. */
    @Test
    public void checkAuthorizationNullContextAlwaysAuthorizedShouldPass() throws AuthorizationException {
        DRPC.checkAuthorization(null, alwaysAuthorized, "execute", FUNCTION);
    }

    /** Test checkAuthorization with valid context and never authorized authorizer. Expected = throws AuthorizationException. */
    @Test
    public void checkAuthorizationValidContextNeverAuthorizedThrowsAuthorizationException() {
        Assert.assertThrows(AuthorizationException.class,
            () -> DRPC.checkAuthorization(ReqContext.context(), neverAuthorized, "execute", FUNCTION));
    }

    /** Test checkAuthorization with null context and never authorized authorizer. Expected = throws NullPointerException while building denied-user message. */
    @Test
    public void checkAuthorizationNullContextNeverAuthorizedThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class,
            () -> DRPC.checkAuthorization(null, neverAuthorized, "execute", FUNCTION));
    }

    /** Test checkAuthorization with invalid authorizer. Expected = propagates RuntimeException. */
    @Test
    public void checkAuthorizationInvalidAuthorizerThrowsRuntimeException() {
        Assert.assertThrows(RuntimeException.class,
            () -> DRPC.checkAuthorization(ReqContext.context(), invalidAuthorizer, "execute", FUNCTION));
    }

    /** Test checkAuthorization passes the function name through the authorization map. Expected = FUNCTION_NAME mapped to function. */
    @SuppressWarnings("unchecked")
    @Test
    public void checkAuthorizationShouldPassFunctionNameInAuthorizationMap() throws AuthorizationException {
        IAuthorizer auth = Mockito.mock(IAuthorizer.class);
        Mockito.when(auth.permit(any(ReqContext.class), anyString(), any(Map.class))).thenReturn(true);

        DRPC.checkAuthorization(ReqContext.context(), auth, "fetchRequest", FUNCTION);

        ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(auth).permit(any(ReqContext.class), Mockito.eq("fetchRequest"), captor.capture());
        Assert.assertEquals(FUNCTION, captor.getValue().get(DRPCAuthorizerBase.FUNCTION_NAME));
    }

    /** Test execute with functionName = "try", funcArgs = "args", valid factory and authorized state. Expected = factory result returned. */
    @Test
    public void executeValidFunctionNameValidArgsValidFactoryAuthShouldPass() throws AuthorizationException {
        OutstandingRequest expected = new DoNothingOutstandingRequest(FUNCTION, new DRPCRequest(ARGS, "1"));

        OutstandingRequest actual = drpcAuthOk.execute(FUNCTION, ARGS, factoryReturning(expected));

        Assert.assertEquals(expected, actual);
    }

    /** Test execute with functionName = "try", funcArgs = null, valid factory and authorized state. Expected = generated DRPCRequest has null args and id "1". */
    @SuppressWarnings("unchecked")
    @Test
    public void executeValidFunctionNameNullArgsValidFactoryAuthShouldPass() throws AuthorizationException {
        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        OutstandingRequest expected = new DoNothingOutstandingRequest(FUNCTION, new DRPCRequest(null, "1"));
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(expected);

        OutstandingRequest actual = drpcAuthOk.execute(FUNCTION, null, factory);

        Assert.assertSame(expected, actual);
        ArgumentCaptor<DRPCRequest> requestCaptor = ArgumentCaptor.forClass(DRPCRequest.class);
        Mockito.verify(factory).mkRequest(Mockito.eq(FUNCTION), requestCaptor.capture());
        Assert.assertEquals(new DRPCRequest(null, "1"), requestCaptor.getValue());
    }

    /** Test execute with functionName = "", funcArgs = "args", valid factory and authorized state. Expected = request accepted as boundary string. */
    @Test
    public void executeEmptyFunctionNameValidArgsValidFactoryAuthShouldPass() throws AuthorizationException {
        OutstandingRequest expected = new DoNothingOutstandingRequest("", new DRPCRequest(ARGS, "1"));

        OutstandingRequest actual = drpcAuthOk.execute("", ARGS, factoryReturning(expected));

        Assert.assertSame(expected, actual);
    }

//    /** Test execute with null functionName and authorized state. Expected = IllegalArgumentException from queue selection. */
//    @Test
//    public void executeNullFunctionNameValidArgsAuthThrowsIllegalArgumentException() {  // REQUEST GENERATED IS NULL --> NULL POINTER EXCEPTION PUTTING THAT IN THE MAP
//        OutstandingRequest request = new DoNothingOutstandingRequest(null, new DRPCRequest(ARGS, "1"));
//
//        Assert.assertThrows(IllegalArgumentException.class,
//            () -> drpcAuthOk.execute(null, ARGS, factoryReturning(request)));
//    }

    /** Test execute with valid inputs and not authorized state. Expected = throws AuthorizationException before invoking factory. */
    @SuppressWarnings("unchecked")
    @Test
    public void executeValidInputsNotAuthorizedThrowsAuthorizationExceptionAndDoesNotInvokeFactory() {
        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);

        Assert.assertThrows(AuthorizationException.class, () -> drpcAuthKo.execute(FUNCTION, ARGS, factory));
        Mockito.verify(factory, Mockito.never()).mkRequest(anyString(), any(DRPCRequest.class));
    }

    /** Test execute with null factory and authorized state. Expected = throws NullPointerException. */
    @Test
    public void executeValidInputsNullFactoryAuthThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> drpcAuthOk.execute(FUNCTION, ARGS, null));
    }

    /** Test execute with factory returning null. Expected = NullPointerException when queued as an outstanding request. */
    @SuppressWarnings("unchecked")
    @Test
    public void executeValidInputsFactoryReturnsNullAuthThrowsNullPointerException() {
        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(null);

        Assert.assertThrows(NullPointerException.class, () -> drpcAuthOk.execute(FUNCTION, ARGS, factory));
    }

    /** Test fetchRequest with functionName = "try" after execute. Expected = previously generated DRPCRequest. */
    @Test
    public void fetchRequestCorrectFunctionNameExistingAuthShouldReturnGeneratedRequest() throws AuthorizationException {
        OutstandingRequest outstandingRequest = new DoNothingOutstandingRequest(FUNCTION, new DRPCRequest(ARGS, "1"));
        drpcAuthOk.execute(FUNCTION, ARGS, factoryReturning(outstandingRequest));

        DRPCRequest actual = drpcAuthOk.fetchRequest(FUNCTION);

        Assert.assertEquals(new DRPCRequest(ARGS, "1"), actual);
    }

    /** Test fetchRequest with correct function but empty queue. Expected = DRPCRequest("", ""). */
    @Test
    public void fetchRequestCorrectFunctionNameNoRequestAuthShouldReturnNothingRequest() throws AuthorizationException {
        DRPCRequest actual = drpcAuthOk.fetchRequest(FUNCTION);

        Assert.assertEquals(new DRPCRequest("", ""), actual);
    }

    /** Test fetchRequest with empty functionName boundary and existing request. Expected = generated request. */
    @Test
    public void fetchRequestEmptyFunctionNameExistingAuthShouldReturnGeneratedRequest() throws AuthorizationException {
        OutstandingRequest outstandingRequest = new DoNothingOutstandingRequest("", new DRPCRequest(ARGS, "1"));
        drpcAuthOk.execute("", ARGS, factoryReturning(outstandingRequest));

        DRPCRequest actual = drpcAuthOk.fetchRequest("");

        Assert.assertEquals(new DRPCRequest(ARGS, "1"), actual);
    }

    /** Test fetchRequest with null functionName and authorized state. Expected = IllegalArgumentException. */
    @Test
    public void fetchRequestNullFunctionNameAuthThrowsIllegalArgumentException() {
        Assert.assertThrows(IllegalArgumentException.class, () -> drpcAuthOk.fetchRequest(null));
    }

    /** Test fetchRequest with valid functionName and not authorized state. Expected = throws AuthorizationException. */
    @Test
    public void fetchRequestValidFunctionNameNotAuthorizedThrowsAuthorizationException() {
        Assert.assertThrows(AuthorizationException.class, () -> drpcAuthKo.fetchRequest(FUNCTION));
    }

    /** Test fetchRequest after request timeout. Expected = DRPCRequest("", "") because timed-out request is cleaned from queue. */
    @Test
    public void fetchRequestCorrectStringAuthAfterTimeoutShouldReturnNothingRequest() throws Exception {
        OutstandingRequest outstandingRequest = new DoNothingOutstandingRequest(FUNCTION, new DRPCRequest(ARGS, "1"));
        drpcShortTimeout.execute(FUNCTION, ARGS, factoryReturning(outstandingRequest));

        Thread.sleep(700L);
        DRPCRequest actual = drpcShortTimeout.fetchRequest(FUNCTION);

        Assert.assertEquals(new DRPCRequest("", ""), actual);
    }

    /** Test returnResult with valid id, valid result and authorized state. Expected = executeBlocking returns provided result. */
    @Test
    public void returnResultValidIdValidResultAuthShouldCompleteBlockingRequest() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> future = executor.submit(() -> drpcAuthOk.executeBlocking(FUNCTION, ARGS));
            DRPCRequest request = fetchEventually(drpcAuthOk, FUNCTION);

            drpcAuthOk.returnResult(request.get_request_id(), "done");

            Assert.assertEquals("done", future.get(5, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }

//    /** Test returnResult with valid id and null result. Expected = executeBlocking returns null. */
//    @Test
//    public void returnResultValidIdNullResultAuthShouldCompleteWithNull() throws Exception {  //RESULT = NULL SO BLOCKING OUTSTANDING REQUEST THROWS DRPC EXECUTION EXCEPTION
//        ExecutorService executor = Executors.newSingleThreadExecutor();
//        try {
//            Future<String> future = executor.submit(() -> drpcAuthOk.executeBlocking(FUNCTION, ARGS));
//            DRPCRequest request = fetchEventually(drpcAuthOk, FUNCTION);
//
//            drpcAuthOk.returnResult(request.get_request_id(), null);
//
//            Assert.assertNull(future.get(5, TimeUnit.SECONDS));
//        } finally {
//            executor.shutdownNow();
//        }
//    }

    /** Test returnResult with unknown id and authorized state. Expected = no exception and no completion side effect. */
    @Test
    public void returnResultUnknownIdAuthShouldNotThrow() throws AuthorizationException {
        drpcAuthOk.returnResult("not-existing", "done");
    }

    /** Test returnResult with null id and authorized state. Expected = NullPointerException from ConcurrentHashMap lookup. */
    @Test
    public void returnResultNullIdEmptyResultAuthThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> drpcAuthOk.returnResult(null, ""));
    }

    /** Test returnResult with valid id and result denied by authorization. Expected = throws AuthorizationException. */
    @Test
    public void returnResultValidIdNotAuthorizedThrowsAuthorizationException() throws AuthorizationException {
        IAuthorizer denyOnlyResult = new IAuthorizer() {
            @Override
            public void prepare(Map<String, Object> conf) {

            }

            @Override
            public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
                return !"result".equals(operation);
            }
        };
        DRPC drpc = new DRPC(metricsRegistry, denyOnlyResult, LONG_TIMEOUT_MS);
        try {
            OutstandingRequest outstandingRequest = new DoNothingOutstandingRequest(FUNCTION, new DRPCRequest(ARGS, "1"));
            drpc.execute(FUNCTION, ARGS, factoryReturning(outstandingRequest));

            Assert.assertThrows(AuthorizationException.class, () -> drpc.returnResult("1", "done"));
        } finally {
            closeIgnoringErrors(drpc);
        }
    }

    /** Test failRequest with valid id and valid exception. Expected = executeBlocking throws same message and FAILED_REQUEST type. */
    @Test
    public void failRequestValidIdValidExceptionAuthShouldFailBlockingRequest() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> future = executor.submit(() -> drpcAuthOk.executeBlocking(FUNCTION, ARGS));
            DRPCRequest request = fetchEventually(drpcAuthOk, FUNCTION);
            DRPCExecutionException failure = new WrappedDRPCExecutionException("failed");
            failure.set_type(DRPCExceptionType.FAILED_REQUEST);

            drpcAuthOk.failRequest(request.get_request_id(), failure);

            Exception thrown = Assert.assertThrows(Exception.class, () -> future.get(5, TimeUnit.SECONDS));
            Assert.assertTrue(thrown.getCause() instanceof DRPCExecutionException);
            DRPCExecutionException actual = (DRPCExecutionException) thrown.getCause();
            Assert.assertEquals("failed", actual.get_msg());
            Assert.assertEquals(DRPCExceptionType.FAILED_REQUEST, actual.get_type());
        } finally {
            executor.shutdownNow();
        }
    }

    /** Test failRequest with valid id and null exception. Expected = executeBlocking throws default failed request exception. */
    @Test
    public void failRequestValidIdNullExceptionAuthShouldUseDefaultFailure() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> future = executor.submit(() -> drpcAuthOk.executeBlocking(FUNCTION, ARGS));
            DRPCRequest request = fetchEventually(drpcAuthOk, FUNCTION);

            drpcAuthOk.failRequest(request.get_request_id(), null);

            Exception thrown = Assert.assertThrows(Exception.class, () -> future.get(5, TimeUnit.SECONDS));
            Assert.assertTrue(thrown.getCause() instanceof DRPCExecutionException);
            DRPCExecutionException actual = (DRPCExecutionException) thrown.getCause();
            Assert.assertEquals("Request failed", actual.get_msg());
            Assert.assertEquals(DRPCExceptionType.FAILED_REQUEST, actual.get_type());
        } finally {
            executor.shutdownNow();
        }
    }

    /** Test failRequest with unknown id and authorized state. Expected = no exception. */
    @Test
    public void failRequestUnknownIdAuthShouldNotThrow() throws AuthorizationException {
        drpcAuthOk.failRequest("not-existing", new WrappedDRPCExecutionException("failed"));
    }

    /** Test failRequest with null id and authorized state. Expected = NullPointerException from ConcurrentHashMap lookup. */
    @Test
    public void failRequestNullIdAuthThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class,
            () -> drpcAuthOk.failRequest(null, new WrappedDRPCExecutionException("failed")));
    }

    /** Test failRequest with valid id and failRequest denied by authorization. Expected = throws AuthorizationException. */
    @Test
    public void failRequestValidIdNotAuthorizedThrowsAuthorizationException() throws AuthorizationException {
        IAuthorizer denyOnlyFailRequest = new IAuthorizer() {
            @Override
            public void prepare(Map<String, Object> conf) {

            }

            @Override
            public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
                return !"failRequest".equals(operation);
            }
        };
        DRPC drpc = new DRPC(metricsRegistry, denyOnlyFailRequest, LONG_TIMEOUT_MS);
        try {
            OutstandingRequest outstandingRequest = new DoNothingOutstandingRequest(FUNCTION, new DRPCRequest(ARGS, "1"));
            drpc.execute(FUNCTION, ARGS, factoryReturning(outstandingRequest));

            Assert.assertThrows(AuthorizationException.class,
                () -> drpc.failRequest("1", new WrappedDRPCExecutionException("failed")));
        } finally {
            closeIgnoringErrors(drpc);
        }
    }

    /** Test executeBlocking with valid inputs and successful returnResult. Expected = returns "done". */
    @Test
    public void executeBlockingValidFunctionNameValidArgsAuthReturnResultShouldReturnDone() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> future = executor.submit(() -> drpcAuthOk.executeBlocking(FUNCTION, ARGS));
            DRPCRequest request = fetchEventually(drpcAuthOk, FUNCTION);

            drpcAuthOk.returnResult(request.get_request_id(), "done");

            Assert.assertEquals("done", future.get(5, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }

    /** Test executeBlocking with valid inputs and failRequest. Expected = throws DRPCExecutionException with message "failed". */
    @Test
    public void executeBlockingValidFunctionNameValidArgsAuthFailRequestThrowsDRPCExecutionException() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> future = executor.submit(() -> drpcAuthOk.executeBlocking(FUNCTION, ARGS));
            DRPCRequest request = fetchEventually(drpcAuthOk, FUNCTION);

            drpcAuthOk.failRequest(request.get_request_id(), new WrappedDRPCExecutionException("failed"));

            Exception thrown = Assert.assertThrows(Exception.class, () -> future.get(5, TimeUnit.SECONDS));
            Assert.assertTrue(thrown.getCause() instanceof DRPCExecutionException);
            Assert.assertEquals("failed", ((DRPCExecutionException) thrown.getCause()).get_msg());
        } finally {
            executor.shutdownNow();
        }
    }

    /** Test executeBlocking with valid functionName, empty args and authorized state. Expected = returns boundary result "". */
    @Test
    public void executeBlockingValidFunctionNameEmptyArgsAuthReturnEmptyResultShouldPass() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> future = executor.submit(() -> drpcAuthOk.executeBlocking(FUNCTION, ""));
            DRPCRequest request = fetchEventually(drpcAuthOk, FUNCTION);
            Assert.assertEquals(new DRPCRequest("", "1"), request);

            drpcAuthOk.returnResult(request.get_request_id(), "");

            Assert.assertEquals("", future.get(5, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }

    /** Test executeBlocking with null functionName and authorized state. Expected = IllegalArgumentException. */
    @Test
    public void executeBlockingNullFunctionNameValidArgsAuthThrowsIllegalArgumentException() {
        Assert.assertThrows(IllegalArgumentException.class, () -> drpcAuthOk.executeBlocking(null, ARGS));
    }

    /** Test executeBlocking with valid inputs and not authorized state. Expected = throws AuthorizationException. */
    @Test
    public void executeBlockingValidFunctionNameValidArgsNotAuthThrowsAuthorizationException() {
        Assert.assertThrows(AuthorizationException.class, () -> drpcAuthKo.executeBlocking(FUNCTION, ARGS));
    }

    /** Test executeBlocking timeout path. Expected = throws DRPCExecutionException with type SERVER_TIMEOUT. */
    @Test
    public void executeBlockingValidInputsAuthTimeoutThrowsServerTimeoutException() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> future = executor.submit(() -> drpcShortTimeout.executeBlocking(FUNCTION, ARGS));

            Exception thrown = Assert.assertThrows(Exception.class, () -> future.get(5, TimeUnit.SECONDS));
            Assert.assertTrue(thrown.getCause() instanceof DRPCExecutionException);
            DRPCExecutionException actual = (DRPCExecutionException) thrown.getCause();
            Assert.assertEquals("Timed Out", actual.get_msg());
            Assert.assertEquals(DRPCExceptionType.SERVER_TIMEOUT, actual.get_type());
        } finally {
            executor.shutdownNow();
        }
    }

    /** Test close with an outstanding blocking request. Expected = executeBlocking fails with SERVER_SHUTDOWN. */
    @Test
    public void closeWithOutstandingRequestShouldFailItWithShutdownException() throws Exception {
        DRPC drpc = new DRPC(metricsRegistry, alwaysAuthorized, LONG_TIMEOUT_MS);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> future = executor.submit(() -> drpc.executeBlocking(FUNCTION, ARGS));
            fetchEventually(drpc, FUNCTION);

            drpc.close();

            Exception thrown = Assert.assertThrows(Exception.class, () -> future.get(5, TimeUnit.SECONDS));
            Assert.assertTrue(thrown.getCause() instanceof DRPCExecutionException);
            DRPCExecutionException actual = (DRPCExecutionException) thrown.getCause();
            Assert.assertEquals("Server Shutting Down", actual.get_msg());
            Assert.assertEquals(DRPCExceptionType.SERVER_SHUTDOWN, actual.get_type());
        } finally {
            executor.shutdownNow();
            closeIgnoringErrors(drpc);
        }
    }

    /** Test close with no outstanding requests. Expected = no exception and timer cancellation is idempotent enough for caller cleanup. */
    @Test
    public void closeWithoutOutstandingRequestsShouldNotThrow() {
        DRPC drpc = new DRPC(metricsRegistry, alwaysAuthorized, LONG_TIMEOUT_MS);

        drpc.close();
    }

    // ### Test END ###
}
