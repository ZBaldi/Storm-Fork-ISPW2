package org.apache.storm.daemon.drpc_1.llm;

import com.codahale.metrics.Meter;
import org.apache.storm.daemon.drpc.refactored.one.DRPC;
import org.apache.storm.daemon.drpc.OutstandingRequest;
import org.apache.storm.daemon.drpc.RequestFactory;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.generated.DRPCRequest;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.security.auth.ReqContext;
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class TotWithExamplesDrpcTest {
    // ### Test START ###

    private DRPC drpcAuthOk;
    private DRPC drpcAuthKo;
    private DRPC drpcNoAuth;

    private static class DoNothingOutstandingRequest extends OutstandingRequest {
        private String returnedResult;
        private DRPCExecutionException failedException;

        DoNothingOutstandingRequest(String function, DRPCRequest req) {
            super(function, req);
        }

        @Override
        public void returnResult(String result) {
            this.returnedResult = result;
        }

        @Override
        public void fail(DRPCExecutionException e) {
            this.failedException = e;
        }

        String getReturnedResult() {
            return returnedResult;
        }

        DRPCExecutionException getFailedException() {
            return failedException;
        }
    }

    private final IAuthorizer alwaysAuthorized = new IAuthorizer() {
        @Override
        public void prepare(Map<String, Object> conf) {
            // no-op
        }

        @Override
        public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
            return true;
        }
    };

    private final IAuthorizer neverAuthorized = new IAuthorizer() {
        @Override
        public void prepare(Map<String, Object> conf) {
            // no-op
        }

        @Override
        public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
            return false;
        }
    };

    private final IAuthorizer invalidAuthorizer = new IAuthorizer() {
        @Override
        public void prepare(Map<String, Object> conf) {
            // no-op
        }

        @Override
        public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
            throw new RuntimeException("invalid authorizer");
        }
    };

    @Before
    public void setUp() {
        drpcAuthOk = new DRPC(metricsRegistry(), alwaysAuthorized, 1000L);
        drpcAuthKo = new DRPC(metricsRegistry(), neverAuthorized, 1000L);
        drpcNoAuth = new DRPC(metricsRegistry(), null, 1000L);
    }

    @After
    public void tearDown() {
        closeQuietly(drpcAuthOk);
        closeQuietly(drpcAuthKo);
        closeQuietly(drpcNoAuth);
    }

    private static void closeQuietly(DRPC drpc) {
        if (drpc != null) {
            try {
                drpc.close();
            } catch (Throwable ignored) {
                // ignored on purpose in tests
            }
        }
    }

    private static StormMetricsRegistry metricsRegistry() {
        StormMetricsRegistry registry = Mockito.mock(StormMetricsRegistry.class);
        when(registry.registerMeter(anyString())).thenReturn(Mockito.mock(Meter.class));
        return registry;
    }

    @SuppressWarnings("unchecked")
    private static RequestFactory<OutstandingRequest> doNothingFactory() {
        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        when(factory.mkRequest(any(), any(DRPCRequest.class))).thenAnswer(invocation ->
            new DoNothingOutstandingRequest(invocation.getArgument(0), invocation.getArgument(1)));
        return factory;
    }

    /** Test execute method with functionName = "try", funcArgs = "args", valid factory and authorized state. */
    @Test
    public void executeValidInputAuthShouldReturnOutstandingRequest() throws AuthorizationException {
        OutstandingRequest actual = drpcAuthOk.execute("try", "args", doNothingFactory());

        Assert.assertEquals("try", actual.getFunction());
        Assert.assertEquals(new DRPCRequest("args", "1"), actual.getRequest());
    }

    /** Test execute method with funcArgs = null and authorized state. */
    @Test
    public void executeValidFunctionNameNullFuncArgsAuthShouldPass() throws AuthorizationException {
        OutstandingRequest actual = drpcAuthOk.execute("try", null, doNothingFactory());

        Assert.assertEquals("try", actual.getFunction());
        Assert.assertEquals(new DRPCRequest(null, "1"), actual.getRequest());
    }

    /** Test execute method with functionName = null and authorized state. */
    @Test
    public void executeNullFunctionNameAuthThrowsIllegalArgumentException() {
        Assert.assertThrows(IllegalArgumentException.class,
            () -> drpcAuthOk.execute(null, "args", doNothingFactory()));
    }

    /** Test execute method with null factory and authorized state. */
    @Test
    public void executeValidInputNullFactoryAuthThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class,
            () -> drpcAuthOk.execute("try", "args", null));
    }

    /** Test execute method with factory returning null and authorized state. */
    @SuppressWarnings("unchecked")
    @Test
    public void executeValidInputInvalidFactoryAuthThrowsNullPointerException() {
        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(null);

        Assert.assertThrows(NullPointerException.class,
            () -> drpcAuthOk.execute("try", "args", factory));
    }

    /** Test execute method with valid input and not authorized state. */
    @Test
    public void executeValidInputNotAuthThrowsAuthorizationException() {
        Assert.assertThrows(AuthorizationException.class,
            () -> drpcAuthKo.execute("try", "args", doNothingFactory()));
    }

    /** Test constructor with invalid metric registry state. */
    @Test
    public void constructorNullMetricsRegistryThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class,
            () -> new DRPC(null, alwaysAuthorized, 1000L));
    }

    /** Test fetchRequest method after a request has been generated by execute. */
    @Test
    public void fetchRequestExistingFunctionAuthShouldReturnPreviousRequest() throws AuthorizationException {
        drpcAuthOk.execute("try", "args", doNothingFactory());

        DRPCRequest actual = drpcAuthOk.fetchRequest("try");

        Assert.assertEquals(new DRPCRequest("args", "1"), actual);
    }

    /** Test fetchRequest method with a function that has no queued requests. */
    @Test
    public void fetchRequestNotExistingFunctionAuthShouldReturnNothingRequest() throws AuthorizationException {
        DRPCRequest actual = drpcAuthOk.fetchRequest("missing");

        Assert.assertEquals(new DRPCRequest("", ""), actual);
    }

    /** Test fetchRequest method with empty functionName boundary value. */
    @Test
    public void fetchRequestEmptyFunctionNameAuthShouldReturnNothingRequest() throws AuthorizationException {
        DRPCRequest actual = drpcAuthOk.fetchRequest("");

        Assert.assertEquals(new DRPCRequest("", ""), actual);
    }

    /** Test fetchRequest method with functionName = null. */
    @Test
    public void fetchRequestNullFunctionNameAuthThrowsIllegalArgumentException() {
        Assert.assertThrows(IllegalArgumentException.class,
            () -> drpcAuthOk.fetchRequest(null));
    }

    /** Test fetchRequest method with valid functionName and not authorized state. */
    @Test
    public void fetchRequestValidFunctionNameNotAuthThrowsAuthorizationException() {
        Assert.assertThrows(AuthorizationException.class,
            () -> drpcAuthKo.fetchRequest("try"));
    }

    /** Test returnResult method with existing id and authorized state. */
    @Test
    public void returnResultExistingIdAuthShouldCompleteRequest() throws AuthorizationException {
        DoNothingOutstandingRequest req = (DoNothingOutstandingRequest) drpcAuthOk.execute("try", "args", doNothingFactory());

        drpcAuthOk.returnResult("1", "done");

        Assert.assertEquals("done", req.getReturnedResult());
    }

    /** Test returnResult method with non-existing id and authorized state. */
    @Test
    public void returnResultNotExistingIdAuthShouldDoNothing() throws AuthorizationException {
        drpcAuthOk.returnResult("404", "done");
    }

    /** Test returnResult method with id = null. */
    @Test
    public void returnResultNullIdAuthThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class,
            () -> drpcAuthOk.returnResult(null, "done"));
    }

    /** Test returnResult method with existing id and result = null. */
    @Test
    public void returnResultExistingIdNullResultAuthShouldPass() throws AuthorizationException {
        DoNothingOutstandingRequest req = (DoNothingOutstandingRequest) drpcAuthOk.execute("try", "args", doNothingFactory());

        drpcAuthOk.returnResult("1", null);

        Assert.assertNull(req.getReturnedResult());
    }

    /** Test returnResult-related unauthorized flow. Request creation is denied before a result can be returned. */
    @Test
    public void returnResultFlowNotAuthThrowsAuthorizationException() {
        Assert.assertThrows(AuthorizationException.class, () -> {
            drpcAuthKo.execute("try", "args", doNothingFactory());
            drpcAuthKo.returnResult("1", "done");
        });
    }

    /** Test failRequest method with existing id and valid exception. */
    @Test
    public void failRequestExistingIdValidExceptionAuthShouldFailRequest() throws AuthorizationException {
        DoNothingOutstandingRequest req = (DoNothingOutstandingRequest) drpcAuthOk.execute("try", "args", doNothingFactory());
        DRPCExecutionException exception = new WrappedDRPCExecutionException("failed");

        drpcAuthOk.failRequest("1", exception);

        Assert.assertSame(exception, req.getFailedException());
    }

    /** Test failRequest method with existing id and null exception. */
    @Test
    public void failRequestExistingIdNullExceptionAuthShouldUseDefaultFailure() throws AuthorizationException {
        DoNothingOutstandingRequest req = (DoNothingOutstandingRequest) drpcAuthOk.execute("try", "args", doNothingFactory());

        drpcAuthOk.failRequest("1", null);

        Assert.assertNotNull(req.getFailedException());
        Assert.assertEquals("Request failed", req.getFailedException().get_msg());
    }

    /** Test failRequest method with non-existing id and authorized state. */
    @Test
    public void failRequestNotExistingIdAuthShouldDoNothing() throws AuthorizationException {
        drpcAuthOk.failRequest("404", new WrappedDRPCExecutionException("failed"));
    }

    /** Test failRequest method with id = null. */
    @Test
    public void failRequestNullIdAuthThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class,
            () -> drpcAuthOk.failRequest(null, new WrappedDRPCExecutionException("failed")));
    }

    /** Test failRequest-related unauthorized flow. Request creation is denied before it can be failed. */
    @Test
    public void failRequestFlowNotAuthThrowsAuthorizationException() {
        Assert.assertThrows(AuthorizationException.class, () -> {
            drpcAuthKo.execute("try", "args", doNothingFactory());
            drpcAuthKo.failRequest("1", new WrappedDRPCExecutionException("failed"));
        });
    }

    /** Test executeBlocking method when request is completed successfully through returnResult. */
    @Test
    public void executeBlockingValidInputReturnResultShouldReturnDone() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> future = executor.submit(() -> drpcAuthOk.executeBlocking("try", "args"));
            DRPCRequest request = waitForFetchedRequest(drpcAuthOk, "try");

            drpcAuthOk.returnResult(request.get_request_id(), "done");

            Assert.assertEquals("done", future.get(2, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }

    /** Test executeBlocking method when request fails through failRequest. */
    @Test
    public void executeBlockingValidInputFailRequestShouldThrowExecutionException() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> future = executor.submit(() -> drpcAuthOk.executeBlocking("try", "args"));
            DRPCRequest request = waitForFetchedRequest(drpcAuthOk, "try");

            drpcAuthOk.failRequest(request.get_request_id(), new WrappedDRPCExecutionException("failed"));

            try {
                future.get(2, TimeUnit.SECONDS);
                Assert.fail("Expected DRPCExecutionException");
            } catch (java.util.concurrent.ExecutionException e) {
                Assert.assertTrue(e.getCause() instanceof DRPCExecutionException);
                Assert.assertEquals("failed", ((DRPCExecutionException) e.getCause()).get_msg());
            }
        } finally {
            executor.shutdownNow();
        }
    }

    /** Test executeBlocking method with not authorized state. */
    @Test
    public void executeBlockingValidInputNotAuthThrowsAuthorizationException() {
        Assert.assertThrows(AuthorizationException.class,
            () -> drpcAuthKo.executeBlocking("try", "args"));
    }


    /** Test executeBlocking method with null functionName. */
    @Test
    public void executeBlockingNullFunctionNameAuthThrowsIllegalArgumentException() {
        Assert.assertThrows(IllegalArgumentException.class,
            () -> drpcAuthOk.executeBlocking(null, "args"));
    }

    /** Test checkAuthorization with always authorized authorizer. */
    @Test
    public void checkAuthorizationAlwaysAuthorizedShouldPass() throws AuthorizationException {
        DRPC.checkAuthorization(ReqContext.context(), alwaysAuthorized, "execute", "try");
    }

    /** Test checkAuthorization with null authorizer. */
    @Test
    public void checkAuthorizationNullAuthorizerShouldPass() throws AuthorizationException {
        DRPC.checkAuthorization(ReqContext.context(), null, "execute", "try");
    }

    /** Test checkAuthorization with never authorized authorizer. */
    @Test
    public void checkAuthorizationNeverAuthorizedThrowsAuthorizationException() {
        Assert.assertThrows(AuthorizationException.class,
            () -> DRPC.checkAuthorization(ReqContext.context(), neverAuthorized, "execute", "try"));
    }

    /** Test checkAuthorization with invalid authorizer throwing RuntimeException. */
    @Test
    public void checkAuthorizationInvalidAuthorizerThrowsRuntimeException() {
        Assert.assertThrows(RuntimeException.class,
            () -> DRPC.checkAuthorization(ReqContext.context(), invalidAuthorizer, "execute", "try"));
    }

    /** Test checkAuthorization with null ReqContext and always authorized authorizer. */
    @Test
    public void checkAuthorizationNullReqContextAlwaysAuthorizedShouldPass() throws AuthorizationException {
        DRPC.checkAuthorization(null, alwaysAuthorized, "execute", "try");
    }

    /** Test close method on a valid DRPC instance. */
    @Test
    public void closeValidInstanceShouldNotThrow() {
        DRPC drpc = new DRPC(metricsRegistry(), alwaysAuthorized, 1000L);

        drpc.close();
    }

    private static DRPCRequest waitForFetchedRequest(DRPC drpc, String functionName) throws Exception {
        long deadline = System.currentTimeMillis() + 2000L;
        while (System.currentTimeMillis() < deadline) {
            DRPCRequest request = drpc.fetchRequest(functionName);
            if (!new DRPCRequest("", "").equals(request)) {
                return request;
            }
            Thread.sleep(10L);
        }
        Assert.fail("Expected queued DRPC request");
        return null;
    }

    // ### Test END ###
}
