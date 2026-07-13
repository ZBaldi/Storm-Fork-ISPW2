package org.apache.storm.daemon.drpc_4.another.llm;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;

import com.codahale.metrics.Meter;
import java.util.Map;
import org.apache.storm.daemon.drpc.OutstandingRequest;
import org.apache.storm.daemon.drpc.RequestFactory;
import org.apache.storm.daemon.drpc.refactored.four.DRPC;
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
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class TotDrpcTest {
    // ### Test START ###

    private DRPC drpcAuthOk;
    private DRPC drpcAuthKo;
    private DRPC drpcNoAuth;
    private IAuthorizer alwaysAuthorized;
    private IAuthorizer neverAuthorized;

    @Before
    public void setUp() {
        alwaysAuthorized = Mockito.mock(IAuthorizer.class);
        Mockito.when(alwaysAuthorized.permit(any(ReqContext.class), anyString(), Mockito.<Map<String, Object>>any()))
            .thenReturn(true);

        neverAuthorized = Mockito.mock(IAuthorizer.class);
        Mockito.when(neverAuthorized.permit(any(ReqContext.class), anyString(), Mockito.<Map<String, Object>>any()))
            .thenReturn(false);

        drpcAuthOk = new DRPC(metricsRegistry(), alwaysAuthorized, 10000L);
        drpcAuthKo = new DRPC(metricsRegistry(), neverAuthorized, 10000L);
        drpcNoAuth = new DRPC(metricsRegistry(), null, 10000L);
    }

    @After
    public void tearDown() {
        closeQuietly(drpcAuthOk);
        closeQuietly(drpcAuthKo);
        closeQuietly(drpcNoAuth);
    }

    private static StormMetricsRegistry metricsRegistry() {
        StormMetricsRegistry registry = Mockito.mock(StormMetricsRegistry.class);
        Mockito.when(registry.registerMeter(anyString())).thenReturn(Mockito.mock(Meter.class));
        return registry;
    }

    private static void closeQuietly(DRPC drpc) {
        if (drpc != null) {
            drpc.close();
        }
    }

    private static OutstandingRequest outstanding(String function, DRPCRequest request) {
        OutstandingRequest outstandingRequest = Mockito.mock(OutstandingRequest.class);
        Mockito.when(outstandingRequest.getFunction()).thenReturn(function);
        Mockito.when(outstandingRequest.getRequest()).thenReturn(request);
        return outstandingRequest;
    }

    /** Test constructor with null authorizer. Expected = authorization checks are skipped and execute returns the factory request. */
    @SuppressWarnings("unchecked")
    @Test
    public void constructorNullAuthorizerExecuteShouldPass() throws AuthorizationException {
        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        DRPCRequest request = new DRPCRequest("args", "1");
        OutstandingRequest expected = outstanding("try", request);
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(expected);

        OutstandingRequest actual = drpcNoAuth.execute("try", "args", factory);

        Assert.assertEquals(expected, actual);
    }

    /** Test execute method with functionName = "try", funcArgs = "args", valid factory and state authorized. Expected = returned OutstandingRequest and generated id = "1". */
    @SuppressWarnings("unchecked")
    @Test
    public void executeValidFunctionNameValidFuncArgsValidFactoryAuthShouldPass() throws AuthorizationException {
        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        OutstandingRequest expected = outstanding("try", new DRPCRequest("args", "1"));
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(expected);

        OutstandingRequest actual = drpcAuthOk.execute("try", "args", factory);

        Assert.assertEquals(expected, actual);
        ArgumentCaptor<DRPCRequest> requestCaptor = ArgumentCaptor.forClass(DRPCRequest.class);
        Mockito.verify(factory).mkRequest(eq("try"), requestCaptor.capture());
        Assert.assertEquals("args", requestCaptor.getValue().get_func_args());
        Assert.assertEquals("1", requestCaptor.getValue().get_request_id());
    }

    /** Test execute method with functionName = "try", funcArgs = null, valid factory and state authorized. Expected = OutstandingRequest is created with null args. */
    @SuppressWarnings("unchecked")
    @Test
    public void executeValidFunctionNameNullFuncArgsValidFactoryAuthShouldPass() throws AuthorizationException {
        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        OutstandingRequest expected = outstanding("try", new DRPCRequest(null, "1"));
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(expected);

        OutstandingRequest actual = drpcAuthOk.execute("try", null, factory);

        Assert.assertEquals(expected, actual);
        ArgumentCaptor<DRPCRequest> requestCaptor = ArgumentCaptor.forClass(DRPCRequest.class);
        Mockito.verify(factory).mkRequest(eq("try"), requestCaptor.capture());
        Assert.assertNull(requestCaptor.getValue().get_func_args());
    }

    /** Test execute method with functionName = "", funcArgs = "args", valid factory and state authorized. Expected = request is accepted with boundary empty function name. */
    @SuppressWarnings("unchecked")
    @Test
    public void executeEmptyFunctionNameValidFuncArgsValidFactoryAuthShouldPass() throws AuthorizationException {
        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        OutstandingRequest expected = outstanding("", new DRPCRequest("args", "1"));
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(expected);

        OutstandingRequest actual = drpcAuthOk.execute("", "args", factory);

        Assert.assertEquals(expected, actual);
    }

    /** Test execute method with state not authorized. Expected = throws AuthorizationException. */
    @SuppressWarnings("unchecked")
    @Test
    public void executeValidFunctionNameValidFuncArgsNotAuthThrowsAuthorizationException() {
        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);

        Assert.assertThrows(AuthorizationException.class, () -> drpcAuthKo.execute("try", "args", factory));
        Mockito.verify(factory, Mockito.never()).mkRequest(anyString(), any(DRPCRequest.class));
    }

    /** Test execute method with null factory and state authorized. Expected = throws NullPointerException. */
    @Test
    public void executeValidFunctionNameValidFuncArgsNullFactoryAuthThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> drpcAuthOk.execute("try", "args", null));
    }

    /** Test execute method with factory returning null. Expected = throws NullPointerException when the implementation enqueues the null request. */
    @SuppressWarnings("unchecked")
    @Test
    public void executeValidFunctionNameValidFuncArgsInvalidFactoryAuthThrowsNullPointerException() {
        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(null);

        Assert.assertThrows(NullPointerException.class, () -> drpcAuthOk.execute("try", "args", factory));
    }

    /** Test fetchRequest method with functionName = "try" after execute. Expected = previously generated DRPCRequest. */
    @SuppressWarnings("unchecked")
    @Test
    public void fetchRequestCorrectFunctionNameAuthShouldReturnGeneratedRequest() throws AuthorizationException {
        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        DRPCRequest expectedRequest = new DRPCRequest("args", "1");
        OutstandingRequest expectedOutstanding = outstanding("try", expectedRequest);
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(expectedOutstanding);

        drpcAuthOk.execute("try", "args", factory);
        DRPCRequest actualRequest = drpcAuthOk.fetchRequest("try");

        Assert.assertEquals(expectedRequest, actualRequest);
        Mockito.verify(expectedOutstanding).fetched();
    }

    /** Test fetchRequest method with functionName = "missing" and state authorized. Expected = DRPCRequest with empty args and empty id. */
    @Test
    public void fetchRequestNotCorrectFunctionNameAuthShouldReturnNothingRequest() throws AuthorizationException {
        DRPCRequest actualRequest = drpcAuthOk.fetchRequest("missing");

        Assert.assertEquals(new DRPCRequest("", ""), actualRequest);
    }

    /** Test fetchRequest method with functionName = null and state authorized. Expected = DRPCRequest with empty args and empty id. */
    @Test
    public void fetchRequestNullFunctionNameAuthShouldReturnNothingRequest() throws AuthorizationException {
        DRPCRequest actualRequest = drpcAuthOk.fetchRequest(null);

        Assert.assertEquals(new DRPCRequest("", ""), actualRequest);
    }

    /** Test fetchRequest method with functionName = "try" and state not authorized. Expected = throws AuthorizationException. */
    @SuppressWarnings("unchecked")
    @Test
    public void fetchRequestCorrectFunctionNameNotAuthThrowsAuthorizationException() throws AuthorizationException {
        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        OutstandingRequest outstandingRequest = outstanding("try", new DRPCRequest("args", "1"));
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
        drpcAuthOk.execute("try", "args", factory);

        Assert.assertThrows(AuthorizationException.class, () -> drpcAuthKo.fetchRequest("try"));
    }

    /** Test returnResult method with an existing request and state authorized. Expected = delegates result to OutstandingRequest and removes it. */
    @SuppressWarnings("unchecked")
    @Test
    public void returnResultCorrectIdValidResultAuthShouldPassAndRemoveRequest() throws AuthorizationException {
        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        OutstandingRequest outstandingRequest = outstanding("try", new DRPCRequest("args", "1"));
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
        drpcAuthOk.execute("try", "args", factory);

        drpcAuthOk.returnResult("1", "done");
        drpcAuthOk.returnResult("1", "again");

        Mockito.verify(outstandingRequest).returnResult("done");
        Mockito.verify(outstandingRequest, Mockito.never()).returnResult("again");
    }

    /** Test returnResult method with id not present and state authorized. Expected = no exception and no delegated result. */
    @Test
    public void returnResultNotCorrectIdValidResultAuthShouldPass() throws AuthorizationException {
        drpcAuthOk.returnResult("999", "done");
    }

    /** Test returnResult method with null id and state authorized. Expected = throws NullPointerException from ConcurrentHashMap lookup. */
    @Test
    public void returnResultNullIdEmptyResultAuthThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> drpcAuthOk.returnResult(null, ""));
    }

    /** Test returnResult method with existing request and state not authorized. Expected = throws AuthorizationException and does not return the result. */
    @SuppressWarnings("unchecked")
    @Test
    public void returnResultCorrectIdValidResultNotAuthThrowsAuthorizationException() throws AuthorizationException {
        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        OutstandingRequest outstandingRequest = outstanding("try", new DRPCRequest("args", "1"));
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
        drpcNoAuth.execute("try", "args", factory);

        Assert.assertThrows(AuthorizationException.class, () -> drpcAuthKo.returnResult("1", "done"));
        Mockito.verify(outstandingRequest, Mockito.never()).returnResult(anyString());
    }

    /** Test failRequest method with existing request and valid failure. Expected = delegates the same failure and removes the request. */
    @SuppressWarnings("unchecked")
    @Test
    public void failRequestCorrectIdValidExceptionAuthShouldPassAndRemoveRequest() throws AuthorizationException {
        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        OutstandingRequest outstandingRequest = outstanding("try", new DRPCRequest("args", "1"));
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
        DRPCExecutionException failure = new WrappedDRPCExecutionException("failed");
        drpcAuthOk.execute("try", "args", factory);

        drpcAuthOk.failRequest("1", failure);
        drpcAuthOk.failRequest("1", new WrappedDRPCExecutionException("again"));

        Mockito.verify(outstandingRequest).fail(failure);
        Mockito.verify(outstandingRequest, Mockito.never()).fail(Mockito.argThat(e -> e != null && "again".equals(e.get_msg())));
    }

    /** Test failRequest method with existing request and null failure. Expected = delegates default failure exception. */
    @SuppressWarnings("unchecked")
    @Test
    public void failRequestCorrectIdNullExceptionAuthShouldUseDefaultFailure() throws AuthorizationException {
        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        OutstandingRequest outstandingRequest = outstanding("try", new DRPCRequest("args", "1"));
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
        drpcAuthOk.execute("try", "args", factory);

        drpcAuthOk.failRequest("1", null);

        ArgumentCaptor<DRPCExecutionException> captor = ArgumentCaptor.forClass(DRPCExecutionException.class);
        Mockito.verify(outstandingRequest).fail(captor.capture());
        Assert.assertEquals("Request failed", captor.getValue().get_msg());
    }

    /** Test failRequest method with id not present and state authorized. Expected = no exception. */
    @Test
    public void failRequestNotCorrectIdValidExceptionAuthShouldPass() throws AuthorizationException {
        drpcAuthOk.failRequest("999", new WrappedDRPCExecutionException("failed"));
    }

    /** Test failRequest method with null id and state authorized. Expected = throws NullPointerException from ConcurrentHashMap lookup. */
    @Test
    public void failRequestNullIdValidExceptionAuthThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class,
            () -> drpcAuthOk.failRequest(null, new WrappedDRPCExecutionException("failed")));
    }

    /** Test failRequest method with existing request and state not authorized. Expected = throws AuthorizationException and does not fail the request. */
    @SuppressWarnings("unchecked")
    @Test
    public void failRequestCorrectIdValidExceptionNotAuthThrowsAuthorizationException() throws AuthorizationException {
        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        OutstandingRequest outstandingRequest = outstanding("try", new DRPCRequest("args", "1"));
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
        drpcNoAuth.execute("try", "args", factory);

        Assert.assertThrows(AuthorizationException.class,
            () -> drpcAuthKo.failRequest("1", new WrappedDRPCExecutionException("failed")));
        Mockito.verify(outstandingRequest, Mockito.never()).fail(any(DRPCExecutionException.class));
    }

    /** Test executeBlocking method completed by returnResult. Expected = returns the supplied result. */
    @Test
    public void executeBlockingValidFunctionNameValidFuncArgsReturnResultAuthShouldReturnResult() throws Exception {
        final String[] actual = new String[1];
        Thread worker = new Thread(() -> {
            try {
                actual[0] = drpcAuthOk.executeBlocking("try", "args");
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });

        worker.start();
        DRPCRequest request = waitForRequest(drpcAuthOk, "try");
        drpcAuthOk.returnResult(request.get_request_id(), "done");
        worker.join(3000L);

        Assert.assertFalse(worker.isAlive());
        Assert.assertEquals("done", actual[0]);
    }

    /** Test executeBlocking method completed by failRequest. Expected = throws DRPCExecutionException with provided message. */
    @Test
    public void executeBlockingValidFunctionNameValidFuncArgsFailRequestAuthThrowsDRPCExecutionException() throws Exception {
        final DRPCExecutionException[] actual = new DRPCExecutionException[1];
        Thread worker = new Thread(() -> {
            try {
                drpcAuthOk.executeBlocking("try", "args");
            } catch (DRPCExecutionException e) {
                actual[0] = e;
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });

        worker.start();
        DRPCRequest request = waitForRequest(drpcAuthOk, "try");
        drpcAuthOk.failRequest(request.get_request_id(), new WrappedDRPCExecutionException("failed"));
        worker.join(3000L);

        Assert.assertFalse(worker.isAlive());
        Assert.assertNotNull(actual[0]);
        Assert.assertEquals("failed", actual[0].get_msg());
    }

    /** Test executeBlocking method with functionName = "try", funcArgs = "args" and state not authorized. Expected = throws AuthorizationException. */
    @Test
    public void executeBlockingValidFunctionNameValidFuncArgsNotAuthThrowsAuthorizationException() {
        Assert.assertThrows(AuthorizationException.class, () -> drpcAuthKo.executeBlocking("try", "args"));
    }

    /** Test executeBlocking method with functionName = "try", funcArgs = null and returnResult. Expected = returns the supplied result. */
    @Test
    public void executeBlockingValidFunctionNameNullFuncArgsReturnResultAuthShouldReturnResult() throws Exception {
        final String[] actual = new String[1];
        Thread worker = new Thread(() -> {
            try {
                actual[0] = drpcAuthOk.executeBlocking("try", null);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });

        worker.start();
        DRPCRequest request = waitForRequest(drpcAuthOk, "try");
        Assert.assertNull(request.get_func_args());
        drpcAuthOk.returnResult(request.get_request_id(), "done");
        worker.join(3000L);

        Assert.assertFalse(worker.isAlive());
        Assert.assertEquals("done", actual[0]);
    }

    /** Test checkAuthorization with auth = null. Expected = no exception. */
    @Test
    public void checkAuthorizationNullAuthShouldPass() throws AuthorizationException {
        DRPC.checkAuthorization(ReqContext.context(), null, "execute", "try");
    }

    /** Test checkAuthorization with always authorized IAuthorizer. Expected = no exception and function in topoConf. */
    @Test
    public void checkAuthorizationAlwaysAuthorizedShouldPass() throws AuthorizationException {
        DRPC.checkAuthorization(ReqContext.context(), alwaysAuthorized, "execute", "try");

        ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(alwaysAuthorized, Mockito.atLeastOnce()).permit(any(ReqContext.class), eq("execute"), captor.capture());
        Assert.assertEquals("try", captor.getValue().get("function"));
    }

    /** Test checkAuthorization with never authorized IAuthorizer. Expected = throws AuthorizationException. */
    @Test
    public void checkAuthorizationNeverAuthorizedThrowsAuthorizationException() {
        Assert.assertThrows(AuthorizationException.class,
            () -> DRPC.checkAuthorization(ReqContext.context(), neverAuthorized, "execute", "try"));
    }

    /** Test checkAuthorization with null ReqContext and always authorized IAuthorizer. Expected = no exception. */
    @Test
    public void checkAuthorizationNullContextAlwaysAuthorizedShouldPass() throws AuthorizationException {
        DRPC.checkAuthorization(null, alwaysAuthorized, "execute", "try");
    }

    /** Test checkAuthorization with invalid authorizer throwing RuntimeException. Expected = RuntimeException is propagated. */
    @Test
    public void checkAuthorizationInvalidAuthorizerThrowsRuntimeException() {
        IAuthorizer invalidAuthorizer = Mockito.mock(IAuthorizer.class);
        Mockito.when(invalidAuthorizer.permit(any(ReqContext.class), anyString(), Mockito.<Map<String, Object>>any()))
            .thenThrow(new RuntimeException("invalid authorizer"));

        RuntimeException actual = Assert.assertThrows(RuntimeException.class,
            () -> DRPC.checkAuthorization(ReqContext.context(), invalidAuthorizer, "execute", "try"));
        Assert.assertEquals("invalid authorizer", actual.getMessage());
    }

    /** Test close method with a pending request. Expected = pending request fails with shutdown message and queues are cleared. */
    @SuppressWarnings("unchecked")
    @Test
    public void closeWithPendingRequestShouldFailRequestAndClearQueue() throws AuthorizationException {
        DRPC localDrpc = new DRPC(metricsRegistry(), alwaysAuthorized, 10000L);
        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        OutstandingRequest outstandingRequest = outstanding("try", new DRPCRequest("args", "1"));
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
        localDrpc.execute("try", "args", factory);

        localDrpc.close();
        DRPCRequest actual = localDrpc.fetchRequest("try");

        ArgumentCaptor<DRPCExecutionException> captor = ArgumentCaptor.forClass(DRPCExecutionException.class);
        Mockito.verify(outstandingRequest).fail(captor.capture());
        Assert.assertEquals("Server Shutting Down", captor.getValue().get_msg());
        Assert.assertEquals(new DRPCRequest("", ""), actual);
    }

    /** Test cleanup timeout. Expected = pending request fails with timeout message and is no longer fetchable. */
    @SuppressWarnings("unchecked")
    @Test
    public void cleanupTimeoutShouldFailPendingRequestAndRemoveFromQueue() throws Exception {
        DRPC localDrpc = new DRPC(metricsRegistry(), alwaysAuthorized, 50L);
        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        OutstandingRequest outstandingRequest = outstanding("try", new DRPCRequest("args", "1"));
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
        localDrpc.execute("try", "args", factory);

        Thread.sleep(200L);
        DRPCRequest actual = localDrpc.fetchRequest("try");
        localDrpc.close();

        ArgumentCaptor<DRPCExecutionException> captor = ArgumentCaptor.forClass(DRPCExecutionException.class);
        Mockito.verify(outstandingRequest, Mockito.timeout(1000L)).fail(captor.capture());
        Assert.assertEquals("Timed Out", captor.getValue().get_msg());
        Assert.assertEquals(new DRPCRequest("", ""), actual);
    }

    private static DRPCRequest waitForRequest(DRPC drpc, String functionName) throws Exception {
        long deadline = System.currentTimeMillis() + 3000L;
        DRPCRequest request;
        do {
            request = drpc.fetchRequest(functionName);
            if (!"".equals(request.get_request_id())) {
                return request;
            }
            Thread.sleep(10L);
        } while (System.currentTimeMillis() < deadline);
        Assert.fail("Request not available before timeout");
        return null;
    }

    // ### Test END ###
}
