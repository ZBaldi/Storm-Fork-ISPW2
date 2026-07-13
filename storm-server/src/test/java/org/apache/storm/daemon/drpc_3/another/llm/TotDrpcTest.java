package org.apache.storm.daemon.drpc_3.another.llm;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.Meter;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.storm.daemon.drpc.OutstandingRequest;
import org.apache.storm.daemon.drpc.RequestFactory;
import org.apache.storm.daemon.drpc.refactored.three.DRPC;
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

    private StormMetricsRegistry metricsRegistry;
    private Meter meter;
    private IAuthorizer authOk;
    private IAuthorizer authKo;
    private IAuthorizer authInvalid;
    private DRPC drpcAuthOk;
    private DRPC drpcAuthKo;
    private DRPC drpcNoAuth;

    @Before
    public void setUp() {
        metricsRegistry = mock(StormMetricsRegistry.class);
        meter = mock(Meter.class);
        lenient().when(metricsRegistry.registerMeter(anyString())).thenReturn(meter);

        authOk = mock(IAuthorizer.class);
        authKo = mock(IAuthorizer.class);
        authInvalid = mock(IAuthorizer.class);
        lenient().when(authOk.permit(any(ReqContext.class), anyString(), any(Map.class))).thenReturn(true);
        lenient().when(authKo.permit(any(ReqContext.class), anyString(), any(Map.class))).thenReturn(false);
        lenient().when(authInvalid.permit(any(ReqContext.class), anyString(), any(Map.class)))
            .thenThrow(new RuntimeException("invalid authorizer"));

        drpcAuthOk = new DRPC(metricsRegistry, authOk, 1000L);
        drpcAuthKo = new DRPC(metricsRegistry, authKo, 1000L);
        drpcNoAuth = new DRPC(metricsRegistry, null, 1000L);
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

    /** Test constructor with valid metrics registry, always-authorized authorizer and positive timeout. Expected = instance created. */
    @Test
    public void constructorValidMetricsValidAuthorizerShouldPass() {
        DRPC drpc = new DRPC(metricsRegistry, authOk, 1000L);
        Assert.assertNotNull(drpc);
        drpc.close();
    }

    /** Test constructor with null metrics registry. Expected = throws NullPointerException. */
    @Test
    public void constructorNullMetricsRegistryThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> new DRPC(null, authOk, 1000L));
    }

    /** Test checkAuthorization with null authorizer. Expected = no exception. */
    @Test
    public void checkAuthorizationNullAuthorizerShouldPass() throws AuthorizationException {
        DRPC.checkAuthorization(ReqContext.context(), null, "execute", "try");
    }

    /** Test checkAuthorization with always-authorized implementation. Expected = no exception. */
    @Test
    public void checkAuthorizationAlwaysAuthorizedShouldPass() throws AuthorizationException {
        DRPC.checkAuthorization(ReqContext.context(), authOk, "execute", "try");
    }

    /** Test checkAuthorization with never-authorized implementation. Expected = throws AuthorizationException. */
    @Test
    public void checkAuthorizationNeverAuthorizedThrowsAuthorizationException() {
        Assert.assertThrows(AuthorizationException.class,
            () -> DRPC.checkAuthorization(ReqContext.context(), authKo, "execute", "try"));
    }

    /** Test checkAuthorization with invalid authorizer. Expected = throws RuntimeException. */
    @Test
    public void checkAuthorizationInvalidAuthorizerThrowsRuntimeException() {
        Assert.assertThrows(RuntimeException.class,
            () -> DRPC.checkAuthorization(ReqContext.context(), authInvalid, "execute", "try"));
    }

    /** Test checkAuthorization with null request context and denied authorization. Expected = throws NullPointerException. */
    @Test
    public void checkAuthorizationNullContextNeverAuthorizedThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class,
            () -> DRPC.checkAuthorization(null, authKo, "execute", "try"));
    }

    /** Test execute method with functionName = "try", funcArgs = "args", valid factory and authorized state. Expected = factory request returned and queued. */
    @SuppressWarnings("unchecked")
    @Test
    public void executeValidFunctionNameValidArgsValidFactoryAuthShouldPass() throws AuthorizationException {
        RequestFactory<OutstandingRequest> factory = mock(RequestFactory.class);
        OutstandingRequest expectedRequest = mockOutstandingRequest("try", new DRPCRequest("args", "1"));
        when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(expectedRequest);

        OutstandingRequest actual = drpcAuthOk.execute("try", "args", factory);

        Assert.assertSame(expectedRequest, actual);
        ArgumentCaptor<DRPCRequest> requestCaptor = ArgumentCaptor.forClass(DRPCRequest.class);
        verify(factory).mkRequest(eq("try"), requestCaptor.capture());
        Assert.assertEquals(new DRPCRequest("args", "1"), requestCaptor.getValue());
    }

    /** Test execute method with functionName = null and authorized state. Expected = throws NullPointerException while queueing by null key. */
    @SuppressWarnings("unchecked")
    @Test
    public void executeNullFunctionNameValidArgsAuthThrowsNullPointerException() {
        RequestFactory<OutstandingRequest> factory = mock(RequestFactory.class);
        OutstandingRequest expectedRequest = mockOutstandingRequest(null, new DRPCRequest("args", "1"));
        when(factory.mkRequest(Mockito.isNull(), any(DRPCRequest.class))).thenReturn(expectedRequest);

        Assert.assertThrows(NullPointerException.class, () -> drpcAuthOk.execute(null, "args", factory));
    }

    /** Test execute method with functionName = "try", funcArgs = null, valid factory and authorized state. Expected = request contains null args and generated id. */
    @SuppressWarnings("unchecked")
    @Test
    public void executeValidFunctionNameNullFuncArgsValidFactoryAuthShouldPass() throws AuthorizationException {
        RequestFactory<OutstandingRequest> factory = mock(RequestFactory.class);
        OutstandingRequest expectedRequest = mockOutstandingRequest("try", new DRPCRequest(null, "1"));
        when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(expectedRequest);

        OutstandingRequest actual = drpcAuthOk.execute("try", null, factory);

        Assert.assertSame(expectedRequest, actual);
    }

    /** Test execute method with empty functionName and empty funcArgs. Expected = request created with empty values. */
    @SuppressWarnings("unchecked")
    @Test
    public void executeEmptyFunctionNameEmptyFuncArgsAuthShouldPass() throws AuthorizationException {
        RequestFactory<OutstandingRequest> factory = mock(RequestFactory.class);
        OutstandingRequest expectedRequest = mockOutstandingRequest("", new DRPCRequest("", "1"));
        when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(expectedRequest);

        OutstandingRequest actual = drpcAuthOk.execute("", "", factory);

        Assert.assertSame(expectedRequest, actual);
    }

    /** Test execute method with valid functionName and not-authorized state. Expected = throws AuthorizationException. */
    @SuppressWarnings("unchecked")
    @Test
    public void executeValidFunctionNameValidArgsNotAuthThrowsAuthorizationException() {
        RequestFactory<OutstandingRequest> factory = mock(RequestFactory.class);
        Assert.assertThrows(AuthorizationException.class, () -> drpcAuthKo.execute("try", "args", factory));
    }

    /** Test execute method with null factory and authorized state. Expected = throws NullPointerException. */
    @Test
    public void executeNullFactoryAuthThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> drpcAuthOk.execute("try", "args", null));
    }

    /** Test execute method with not valid factory that returns null. Expected = throws NullPointerException while queueing null request. */
    @SuppressWarnings("unchecked")
    @Test
    public void executeInvalidFactoryReturningNullThrowsNullPointerException() {
        RequestFactory<OutstandingRequest> factory = mock(RequestFactory.class);
        when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(null);

        Assert.assertThrows(NullPointerException.class, () -> drpcAuthOk.execute("try", "args", factory));
    }

    /** Test fetchRequest method with functionName = "try" and existing queued request. Expected = corresponding DRPCRequest. */
    @SuppressWarnings("unchecked")
    @Test
    public void fetchRequestCorrectFunctionNameExistingAuthShouldPass() throws AuthorizationException {
        RequestFactory<OutstandingRequest> factory = mock(RequestFactory.class);
        DRPCRequest expectedRequest = new DRPCRequest("args", "1");
        OutstandingRequest outstandingRequest = mockOutstandingRequest("try", expectedRequest);
        when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
        drpcAuthOk.execute("try", "args", factory);

        DRPCRequest actualRequest = drpcAuthOk.fetchRequest("try");

        Assert.assertEquals(expectedRequest, actualRequest);
        verify(outstandingRequest).fetched();
    }

    /** Test fetchRequest method with functionName = "try" and no queued request. Expected = empty DRPCRequest. */
    @Test
    public void fetchRequestCorrectFunctionNameNoQueuedRequestAuthShouldReturnNothing() throws AuthorizationException {
        DRPCRequest actualRequest = drpcAuthOk.fetchRequest("try");

        Assert.assertEquals(new DRPCRequest("", ""), actualRequest);
    }

    /** Test fetchRequest method with functionName = "missing" while another function is queued. Expected = empty DRPCRequest. */
    @SuppressWarnings("unchecked")
    @Test
    public void fetchRequestNotCorrectFunctionNameAuthShouldReturnNothing() throws AuthorizationException {
        RequestFactory<OutstandingRequest> factory = mock(RequestFactory.class);
        OutstandingRequest outstandingRequest = mockOutstandingRequest("try", new DRPCRequest("args", "1"));
        when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
        drpcAuthOk.execute("try", "args", factory);

        DRPCRequest actualRequest = drpcAuthOk.fetchRequest("missing");

        Assert.assertEquals(new DRPCRequest("", ""), actualRequest);
    }

    /** Test fetchRequest method with functionName = null and authorized state. Expected = throws IllegalArgumentException. */
    @Test
    public void fetchRequestNullFunctionNameAuthThrowsIllegalArgumentException() {
        Assert.assertThrows(IllegalArgumentException.class, () -> drpcAuthOk.fetchRequest(null));
    }

    /** Test fetchRequest method with functionName = "try" and not-authorized state. Expected = throws AuthorizationException. */
    @SuppressWarnings("unchecked")
    @Test
    public void fetchRequestCorrectFunctionNameNotAuthThrowsAuthorizationException() {
        Assert.assertThrows(AuthorizationException.class, () -> {
            RequestFactory<OutstandingRequest> factory = mock(RequestFactory.class);
            OutstandingRequest outstandingRequest = mockOutstandingRequest("try", new DRPCRequest("args", "1"));
            when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
            drpcNoAuth.execute("try", "args", factory);
            drpcAuthKo.fetchRequest("try");
        });
    }

    /** Test returnResult method with existing id and authorized state. Expected = request completed with provided result and removed. */
    @SuppressWarnings("unchecked")
    @Test
    public void returnResultCorrectIdValidResultAuthShouldPass() throws AuthorizationException {
        RequestFactory<OutstandingRequest> factory = mock(RequestFactory.class);
        OutstandingRequest outstandingRequest = mockOutstandingRequest("try", new DRPCRequest("args", "1"));
        when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
        drpcAuthOk.execute("try", "args", factory);

        drpcAuthOk.returnResult("1", "done");

        verify(outstandingRequest).returnResult("done");
        DRPCRequest actualRequest = drpcAuthOk.fetchRequest("try");
        Assert.assertEquals(new DRPCRequest("", ""), actualRequest);
    }

    /** Test returnResult method with correct id and null result. Expected = request completed with null result. */
    @SuppressWarnings("unchecked")
    @Test
    public void returnResultCorrectIdNullResultAuthShouldPass() throws AuthorizationException {
        RequestFactory<OutstandingRequest> factory = mock(RequestFactory.class);
        OutstandingRequest outstandingRequest = mockOutstandingRequest("try", new DRPCRequest("args", "1"));
        when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
        drpcAuthOk.execute("try", "args", factory);

        drpcAuthOk.returnResult("1", null);

        verify(outstandingRequest).returnResult(null);
    }

    /** Test returnResult method with missing id and authorized state. Expected = no exception and no completion. */
    @Test
    public void returnResultNotCorrectIdAuthShouldDoNothing() throws AuthorizationException {
        drpcAuthOk.returnResult("missing", "done");
    }

    /** Test returnResult method with null id and authorized state. Expected = no exception because no request is found. */
    @Test
    public void returnResultNullIdAuthShouldDoNothing() throws AuthorizationException {
        drpcAuthOk.returnResult(null, "done");
    }

    /** Test returnResult method with existing id and not-authorized state. Expected = throws AuthorizationException and request is not completed. */
    @SuppressWarnings("unchecked")
    @Test
    public void returnResultCorrectIdNotAuthThrowsAuthorizationException() throws AuthorizationException {
        AtomicBoolean authorized = new AtomicBoolean(true);
        IAuthorizer mutableAuthorizer = mock(IAuthorizer.class);
        when(mutableAuthorizer.permit(any(ReqContext.class), anyString(), any(Map.class)))
            .thenAnswer(invocation -> authorized.get());
        DRPC localDrpc = new DRPC(metricsRegistry, mutableAuthorizer, 1000L);
        try {
            RequestFactory<OutstandingRequest> factory = mock(RequestFactory.class);
            OutstandingRequest outstandingRequest = mockOutstandingRequest("try", new DRPCRequest("args", "1"));
            when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
            localDrpc.execute("try", "args", factory);
            authorized.set(false);

            Assert.assertThrows(AuthorizationException.class, () -> localDrpc.returnResult("1", "done"));
            verify(outstandingRequest, never()).returnResult("done");
        } finally {
            localDrpc.close();
        }
    }

    /** Test failRequest method with existing id and valid exception. Expected = request fails with provided exception. */
    @SuppressWarnings("unchecked")
    @Test
    public void failRequestCorrectIdValidExceptionAuthShouldPass() throws AuthorizationException {
        RequestFactory<OutstandingRequest> factory = mock(RequestFactory.class);
        OutstandingRequest outstandingRequest = mockOutstandingRequest("try", new DRPCRequest("args", "1"));
        DRPCExecutionException exception = new DRPCExecutionException("failed");
        when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
        drpcAuthOk.execute("try", "args", factory);

        drpcAuthOk.failRequest("1", exception);

        verify(outstandingRequest).fail(exception);
    }

    /** Test failRequest method with existing id and null exception. Expected = request fails with default failure exception. */
    @SuppressWarnings("unchecked")
    @Test
    public void failRequestCorrectIdNullExceptionAuthShouldUseDefaultFailure() throws AuthorizationException {
        RequestFactory<OutstandingRequest> factory = mock(RequestFactory.class);
        OutstandingRequest outstandingRequest = mockOutstandingRequest("try", new DRPCRequest("args", "1"));
        when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
        drpcAuthOk.execute("try", "args", factory);

        drpcAuthOk.failRequest("1", null);

        ArgumentCaptor<DRPCExecutionException> exceptionCaptor = ArgumentCaptor.forClass(DRPCExecutionException.class);
        verify(outstandingRequest).fail(exceptionCaptor.capture());
        Assert.assertEquals("Request failed", exceptionCaptor.getValue().get_msg());
    }

    /** Test failRequest method with missing id. Expected = no exception and no failure callback. */
    @Test
    public void failRequestNotCorrectIdAuthShouldDoNothing() throws AuthorizationException {
        drpcAuthOk.failRequest("missing", new DRPCExecutionException("failed"));
    }

    /** Test failRequest method with null id. Expected = no exception because no request is found. */
    @Test
    public void failRequestNullIdAuthShouldDoNothing() throws AuthorizationException {
        drpcAuthOk.failRequest(null, new DRPCExecutionException("failed"));
    }

    /** Test failRequest method with existing id and not-authorized state. Expected = throws AuthorizationException. */
    @SuppressWarnings("unchecked")
    @Test
    public void failRequestCorrectIdNotAuthThrowsAuthorizationException() throws AuthorizationException {
        AtomicBoolean authorized = new AtomicBoolean(true);
        IAuthorizer mutableAuthorizer = mock(IAuthorizer.class);
        when(mutableAuthorizer.permit(any(ReqContext.class), anyString(), any(Map.class)))
            .thenAnswer(invocation -> authorized.get());
        DRPC localDrpc = new DRPC(metricsRegistry, mutableAuthorizer, 1000L);
        try {
            RequestFactory<OutstandingRequest> factory = mock(RequestFactory.class);
            OutstandingRequest outstandingRequest = mockOutstandingRequest("try", new DRPCRequest("args", "1"));
            when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
            localDrpc.execute("try", "args", factory);
            authorized.set(false);

            Assert.assertThrows(AuthorizationException.class,
                () -> localDrpc.failRequest("1", new DRPCExecutionException("failed")));
            verify(outstandingRequest, never()).fail(any(DRPCExecutionException.class));
        } finally {
            localDrpc.close();
        }
    }

    /** Test executeBlocking method with functionName = "try", funcArgs = "args" and successful returnResult. Expected = returns "done". */
    @Test
    public void executeBlockingValidFunctionNameValidFuncArgsReturnResultShouldPass() throws Exception {
        Thread worker = new Thread(() -> {
            try {
                waitUntilOutstandingRequestIsQueued(drpcAuthOk, "try");
                drpcAuthOk.returnResult("1", "done");
            } catch (Exception exception) {
                throw new RuntimeException(exception);
            }
        });
        worker.start();

        String actual = drpcAuthOk.executeBlocking("try", "args");

        worker.join(1000L);
        Assert.assertEquals("done", actual);
    }

    /** Test executeBlocking method with functionName = "try", funcArgs = "args" and failRequest. Expected = throws DRPCExecutionException with message "failed". */
    @Test
    public void executeBlockingValidFunctionNameValidFuncArgsFailRequestThrowsDRPCExecutionException() throws Exception {
        Thread worker = new Thread(() -> {
            try {
                waitUntilOutstandingRequestIsQueued(drpcAuthOk, "try");
                drpcAuthOk.failRequest("1", new DRPCExecutionException("failed"));
            } catch (Exception exception) {
                throw new RuntimeException(exception);
            }
        });
        worker.start();

        DRPCExecutionException actual = Assert.assertThrows(DRPCExecutionException.class,
            () -> drpcAuthOk.executeBlocking("try", "args"));

        worker.join(1000L);
        Assert.assertEquals("failed", actual.get_msg());
    }

    /** Test executeBlocking method with functionName = "try", funcArgs = "args" and state not authorized. Expected = throws AuthorizationException. */
    @Test
    public void executeBlockingValidFunctionNameValidFuncArgsNotAuthThrowsAuthorizationException() {
        Assert.assertThrows(AuthorizationException.class, () -> drpcAuthKo.executeBlocking("try", "args"));
    }

    /** Test executeBlocking method with empty functionName and empty funcArgs. Expected = returns empty result when completed. */
    @Test
    public void executeBlockingEmptyFunctionNameEmptyFuncArgsReturnEmptyResultShouldPass() throws Exception {
        Thread worker = new Thread(() -> {
            try {
                waitUntilOutstandingRequestIsQueued(drpcAuthOk, "");
                drpcAuthOk.returnResult("1", "");
            } catch (Exception exception) {
                throw new RuntimeException(exception);
            }
        });
        worker.start();

        String actual = drpcAuthOk.executeBlocking("", "");

        worker.join(1000L);
        Assert.assertEquals("", actual);
    }

    /** Test timeout cleanup after execute queues an outstanding request. Expected = request fails with Timed Out and fetch returns NOTHING. */
    @SuppressWarnings("unchecked")
    @Test
    public void cleanupTimedOutRequestShouldFailRequestAndRemoveFromQueue() throws Exception {
        DRPC localDrpc = new DRPC(metricsRegistry, authOk, 10L);
        try {
            RequestFactory<OutstandingRequest> factory = mock(RequestFactory.class);
            OutstandingRequest outstandingRequest = mockOutstandingRequest("try", new DRPCRequest("args", "1"));
            when(outstandingRequest.isTimedOut(10L)).thenReturn(true);
            when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
            localDrpc.execute("try", "args", factory);

            verify(outstandingRequest, timeout(1000L)).fail(any(DRPCExecutionException.class));
            Assert.assertEquals(new DRPCRequest("", ""), localDrpc.fetchRequest("try"));
        } finally {
            localDrpc.close();
        }
    }

    /** Test close with an outstanding request. Expected = request fails with shutdown exception and is removed. */
    @SuppressWarnings("unchecked")
    @Test
    public void closeWithOutstandingRequestShouldFailAndClearQueues() throws AuthorizationException {
        RequestFactory<OutstandingRequest> factory = mock(RequestFactory.class);
        OutstandingRequest outstandingRequest = mockOutstandingRequest("try", new DRPCRequest("args", "1"));
        when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
        DRPC localDrpc = new DRPC(metricsRegistry, authOk, 1000L);
        localDrpc.execute("try", "args", factory);

        localDrpc.close();

        ArgumentCaptor<DRPCExecutionException> exceptionCaptor = ArgumentCaptor.forClass(DRPCExecutionException.class);
        verify(outstandingRequest).fail(exceptionCaptor.capture());
        Assert.assertEquals("Server Shutting Down", exceptionCaptor.getValue().get_msg());
        Assert.assertEquals(new DRPCRequest("", ""), localDrpc.fetchRequest("try"));
    }

    private OutstandingRequest mockOutstandingRequest(String function, DRPCRequest request) {
        OutstandingRequest outstandingRequest = Mockito.mock(OutstandingRequest.class);
        lenient().when(outstandingRequest.getFunction()).thenReturn(function);
        lenient().when(outstandingRequest.getRequest()).thenReturn(request);
        lenient().when(outstandingRequest.isTimedOut(Mockito.anyLong())).thenReturn(false);
        return outstandingRequest;
    }

    private void waitUntilOutstandingRequestIsQueued(DRPC drpc, String functionName) throws Exception {
        long deadline = System.currentTimeMillis() + 1000L;
        while (System.currentTimeMillis() < deadline) {
            DRPCRequest request = drpc.fetchRequest(functionName);
            if (!new DRPCRequest("", "").equals(request)) {
                return;
            }
            Thread.sleep(10L);
        }
        Assert.fail("Timed out waiting for outstanding request");
    }

    // ### Test END ###
}
