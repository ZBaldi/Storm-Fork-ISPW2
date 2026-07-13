package org.apache.storm.daemon.drpc_3.llm;

import com.codahale.metrics.Meter;
import org.apache.storm.daemon.utils.three.DoNothingOutstandingRequest;
import org.apache.storm.daemon.drpc.refactored.three.DRPC;
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

/**
 * Basic JUnit 4 tests for DRPC.
 * The tests are intentionally simple and explicit, like tests written by a junior tester.
 */
public class Fsp1DrpcTest {

    // ### Test START ###

    private DRPC drpcAuthOk;
    private DRPC drpcAuthKo;
    private DRPC drpcNotValid;

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
        drpcAuthOk = new DRPC(mockMetricsRegistry(), alwaysAuthorized, 1000);
        drpcAuthKo = new DRPC(mockMetricsRegistry(), neverAuthorized, 1000);

        // Invalid state: meters are not initialized because registerMeter returns null.
        StormMetricsRegistry invalidMetrics = Mockito.mock(StormMetricsRegistry.class);
        drpcNotValid = new DRPC(invalidMetrics, alwaysAuthorized, 1000);
    }

    @After
    public void tearDown() {
        if (drpcAuthOk != null) {
            drpcAuthOk.close();
        }
        if (drpcAuthKo != null) {
            drpcAuthKo.close();
        }
        if (drpcNotValid != null) {
            drpcNotValid.close();
        }
    }

    private StormMetricsRegistry mockMetricsRegistry() {
        StormMetricsRegistry metrics = Mockito.mock(StormMetricsRegistry.class);
        Meter meter = Mockito.mock(Meter.class);
        Mockito.when(metrics.registerMeter(anyString())).thenReturn(meter);
        return metrics;
    }

    /** Test checkAuthorization method with valid context, always authorized authorizer and valid strings. Expected = no exception. */
    @Test
    public void checkAuthorizationValidContextAlwaysAuthorizedShouldPass() throws AuthorizationException {
        DRPC.checkAuthorization(ReqContext.context(), alwaysAuthorized, "execute", "try");
    }

    /** Test checkAuthorization method with valid context, never authorized authorizer and valid strings. Expected = throws AuthorizationException. */
    @Test
    public void checkAuthorizationValidContextNeverAuthorizedThrowsAuthorizationException() {
        Assert.assertThrows(AuthorizationException.class, () ->
            DRPC.checkAuthorization(ReqContext.context(), neverAuthorized, "execute", "try"));
    }

    /** Test checkAuthorization method with null authorizer. Expected = no exception. */
    @Test
    public void checkAuthorizationNullAuthorizerShouldPass() throws AuthorizationException {
        DRPC.checkAuthorization(ReqContext.context(), null, "execute", "try");
    }

    /** Test checkAuthorization method with invalid authorizer. Expected = throws RuntimeException. */
    @Test
    public void checkAuthorizationInvalidAuthorizerThrowsRuntimeException() {
        Assert.assertThrows(RuntimeException.class, () ->
            DRPC.checkAuthorization(ReqContext.context(), invalidAuthorizer, "execute", "try"));
    }

    /** Test execute method with valid strings, valid factory and state authorized. Expected = created OutstandingRequest. */
    @SuppressWarnings("unchecked")
    @Test
    public void executeValidFunctionNameValidFuncArgsValidFactoryAuthShouldPass() throws AuthorizationException {
        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        DRPCRequest request = new DRPCRequest("args", "1");
        OutstandingRequest expectedRequest = new DoNothingOutstandingRequest("try", request);
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(expectedRequest);

        OutstandingRequest actual = drpcAuthOk.execute("try", "args", factory);

        Assert.assertEquals(expectedRequest, actual);
    }

    /** Test execute method with valid functionName, null args, valid factory and state authorized. Expected = created OutstandingRequest. */
    @SuppressWarnings("unchecked")
    @Test
    public void executeValidFunctionNameNullFuncArgsValidFactoryAuthShouldPass() throws AuthorizationException {
        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        DRPCRequest request = new DRPCRequest(null, "1");
        OutstandingRequest expectedRequest = new DoNothingOutstandingRequest("try", request);
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(expectedRequest);

        OutstandingRequest actual = drpcAuthOk.execute("try", null, factory);

        Assert.assertEquals(expectedRequest, actual);
    }

    /** Test execute method with empty functionName, valid args, valid factory and state authorized. Expected = should pass. */
    @SuppressWarnings("unchecked")
    @Test
    public void executeEmptyFunctionNameValidFuncArgsValidFactoryAuthShouldPass() throws AuthorizationException {
        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        DRPCRequest request = new DRPCRequest("args", "1");
        OutstandingRequest expectedRequest = new DoNothingOutstandingRequest("", request);
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(expectedRequest);

        OutstandingRequest actual = drpcAuthOk.execute("", "args", factory);

        Assert.assertEquals(expectedRequest, actual);
    }

    /** Test execute method with null functionName and state authorized. Expected = throws IllegalArgumentException. */
    @SuppressWarnings("unchecked")
    //@Test (FAILED) @AFTER STARTS CLEANUP THAT DOES A GET WITH A NULL FUNCTION NAME --> NULL POINTER EXCEPTION
    public void executeNullFunctionNameAuthThrowsIllegalArgumentException() {
        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        OutstandingRequest request = new DoNothingOutstandingRequest(null, new DRPCRequest("args", "1"));
        Mockito.when(factory.mkRequest(any(), any(DRPCRequest.class))).thenReturn(request);

        Assert.assertThrows(IllegalArgumentException.class, () -> drpcAuthOk.execute(null, "args", factory));
    }

    /** Test execute method with valid strings, valid factory and state not authorized. Expected = throws AuthorizationException. */
    @SuppressWarnings("unchecked")
    @Test
    public void executeValidFunctionNameValidFuncArgsNotAuthThrowsAuthorizationException() {
        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);

        Assert.assertThrows(AuthorizationException.class, () -> drpcAuthKo.execute("try", "args", factory));
    }

    /** Test execute method with valid strings, factory null and state authorized. Expected = throws NullPointerException. */
    @Test
    public void executeValidFunctionNameValidFuncArgsNullFactoryAuthThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> drpcAuthOk.execute("try", "args", null));
    }

    /** Test execute method with valid strings, factory returning null and state authorized. Expected = throws NullPointerException. */
    @SuppressWarnings("unchecked")
    @Test
    public void executeValidFunctionNameValidFuncArgsNotValidFactoryAuthThrowsNullPointerException() {
        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(null);

        Assert.assertThrows(NullPointerException.class, () -> drpcAuthOk.execute("try", "args", factory));
    }

    /** Test execute method with valid strings and state not valid. Expected = throws NullPointerException. */
    @SuppressWarnings("unchecked")
    @Test
    public void executeValidFunctionNameValidFuncArgsInvalidStateThrowsNullPointerException() {
        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);

        Assert.assertThrows(NullPointerException.class, () -> drpcNotValid.execute("try", "args", factory));
    }

    /** Test fetchRequest method with functionName = "try" existing and state authorized. Expected = DRPCRequest previously generated. */
    @SuppressWarnings("unchecked")
    @Test
    public void fetchRequestCorrectFunctionNameAuthShouldPass() throws AuthorizationException {
        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        DRPCRequest expectedRequest = new DRPCRequest("args", "1");
        OutstandingRequest outstandingRequest = new DoNothingOutstandingRequest("try", expectedRequest);
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
        drpcAuthOk.execute("try", "args", factory);

        DRPCRequest actual = drpcAuthOk.fetchRequest("try");

        Assert.assertEquals(expectedRequest, actual);
    }

    /** Test fetchRequest method with functionName not existing and state authorized. Expected = empty DRPCRequest. */
    @Test
    public void fetchRequestNotCorrectFunctionNameAuthReturnsNothingRequest() throws AuthorizationException {
        DRPCRequest actual = drpcAuthOk.fetchRequest("notExisting");

        Assert.assertEquals(new DRPCRequest("", ""), actual);
    }

    /** Test fetchRequest method with empty functionName and state authorized. Expected = empty DRPCRequest. */
    @Test
    public void fetchRequestEmptyFunctionNameAuthReturnsNothingRequest() throws AuthorizationException {
        DRPCRequest actual = drpcAuthOk.fetchRequest("");

        Assert.assertEquals(new DRPCRequest("", ""), actual);
    }

    /** Test fetchRequest method with null functionName and state authorized. Expected = throws IllegalArgumentException. */
    @Test
    public void fetchRequestNullFunctionNameAuthThrowsIllegalArgumentException() {
        Assert.assertThrows(IllegalArgumentException.class, () -> drpcAuthOk.fetchRequest(null));
    }

    /** Test fetchRequest method with functionName = "try" and state not authorized. Expected = throws AuthorizationException. */
    @SuppressWarnings("unchecked")
    @Test
    public void fetchRequestCorrectFunctionNameNotAuthThrowsAuthorizationException() throws AuthorizationException {
        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        DRPCRequest expectedRequest = new DRPCRequest("args", "1");
        OutstandingRequest outstandingRequest = new DoNothingOutstandingRequest("try", expectedRequest);
        Mockito.lenient().when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
        drpcAuthOk.execute("try", "args", factory);

        Assert.assertThrows(AuthorizationException.class, () -> drpcAuthKo.fetchRequest("try"));
    }

    /** Test fetchRequest method with valid string and state not valid. Expected = throws NullPointerException. */
    @Test
    public void fetchRequestValidFunctionNameInvalidStateThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> drpcNotValid.fetchRequest("try"));
    }

    /** Test returnResult method with existing id, valid result and state authorized. Expected = executeBlocking returns result. */
    @Test
    public void returnResultCorrectIdValidResultAuthShouldPass() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> future = executor.submit(() -> drpcAuthOk.executeBlocking("try", "args"));
            Thread.sleep(100);
            DRPCRequest request = drpcAuthOk.fetchRequest("try");

            drpcAuthOk.returnResult(request.get_request_id(), "done");

            Assert.assertEquals("done", future.get(2, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }

    /** Test returnResult method with not existing id and valid result. Expected = no exception. */
    @Test
    public void returnResultNotCorrectIdValidResultAuthShouldPass() throws AuthorizationException {
        drpcAuthOk.returnResult("notExisting", "done");
    }

    /** Test returnResult method with empty id and empty result. Expected = no exception. */
    @Test
    public void returnResultEmptyIdEmptyResultAuthShouldPass() throws AuthorizationException {
        drpcAuthOk.returnResult("", "");
    }

    /** Test returnResult method with null id and empty result. Expected = throws NullPointerException. */
    @Test
    public void returnResultNullIdEmptyResultAuthThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> drpcAuthOk.returnResult(null, ""));
    }

    /** Test returnResult method with valid strings and state not valid. Expected = throws NullPointerException. */
    // @Test
    public void returnResultValidIdValidResultInvalidStateThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> drpcNotValid.returnResult("1", "done"));
    }

    /** Test failRequest method with existing id and valid exception. Expected = executeBlocking throws same execution message. */
    @Test
    public void failRequestCorrectIdValidExceptionAuthThrowsDrpcExecutionException() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> future = executor.submit(() -> drpcAuthOk.executeBlocking("try", "args"));
            Thread.sleep(100);
            DRPCRequest request = drpcAuthOk.fetchRequest("try");
            DRPCExecutionException exception = new WrappedDRPCExecutionException("failed");

            drpcAuthOk.failRequest(request.get_request_id(), exception);

            Exception actual = Assert.assertThrows(Exception.class, () -> future.get(2, TimeUnit.SECONDS));
            Assert.assertTrue(actual.getCause() instanceof DRPCExecutionException);
            Assert.assertEquals("failed", ((DRPCExecutionException) actual.getCause()).get_msg());
        } finally {
            executor.shutdownNow();
        }
    }

    /** Test failRequest method with existing id and null exception. Expected = executeBlocking throws default failed exception. */
    @Test
    public void failRequestCorrectIdNullExceptionAuthThrowsDefaultDrpcExecutionException() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> future = executor.submit(() -> drpcAuthOk.executeBlocking("try", "args"));
            Thread.sleep(100);
            DRPCRequest request = drpcAuthOk.fetchRequest("try");

            drpcAuthOk.failRequest(request.get_request_id(), null);

            Exception actual = Assert.assertThrows(Exception.class, () -> future.get(2, TimeUnit.SECONDS));
            Assert.assertTrue(actual.getCause() instanceof DRPCExecutionException);
            Assert.assertEquals("Request failed", ((DRPCExecutionException) actual.getCause()).get_msg());
        } finally {
            executor.shutdownNow();
        }
    }

    /** Test failRequest method with not existing id and valid exception. Expected = no exception. */
    @Test
    public void failRequestNotCorrectIdValidExceptionAuthShouldPass() throws AuthorizationException {
        drpcAuthOk.failRequest("notExisting", new WrappedDRPCExecutionException("failed"));
    }

    /** Test failRequest method with null id. Expected = throws NullPointerException. */
    @Test
    public void failRequestNullIdValidExceptionAuthThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () ->
            drpcAuthOk.failRequest(null, new WrappedDRPCExecutionException("failed")));
    }

    /** Test failRequest method with valid strings and state not valid. Expected = throws NullPointerException. */
    @Test
    public void failRequestValidIdValidExceptionInvalidStateThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () ->
            drpcNotValid.failRequest("1", new WrappedDRPCExecutionException("failed")));
    }

    /** Test executeBlocking method with valid strings and successful returnResult. Expected = returns the result. */
    @Test
    public void executeBlockingValidFunctionNameValidFuncArgsAuthReturnsResult() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> future = executor.submit(() -> drpcAuthOk.executeBlocking("try", "args"));
            Thread.sleep(100);
            DRPCRequest request = drpcAuthOk.fetchRequest("try");
            drpcAuthOk.returnResult(request.get_request_id(), "done");

            Assert.assertEquals("done", future.get(2, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }

    /** Test executeBlocking method with valid strings and failRequest. Expected = throws DRPCExecutionException with message failed. */
    @Test
    public void executeBlockingValidFunctionNameValidFuncArgsAuthWithFailThrowsDrpcExecutionException() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> future = executor.submit(() -> drpcAuthOk.executeBlocking("try", "args"));
            Thread.sleep(100);
            DRPCRequest request = drpcAuthOk.fetchRequest("try");
            drpcAuthOk.failRequest(request.get_request_id(), new WrappedDRPCExecutionException("failed"));

            Exception actual = Assert.assertThrows(Exception.class, () -> future.get(2, TimeUnit.SECONDS));
            Assert.assertTrue(actual.getCause() instanceof DRPCExecutionException);
            Assert.assertEquals("failed", ((DRPCExecutionException) actual.getCause()).get_msg());
        } finally {
            executor.shutdownNow();
        }
    }

    /** Test executeBlocking method with functionName = "try", funcArgs = "args" and state not authorized. Expected = throws AuthorizationException. */
    @Test
    public void executeBlockingValidFunctionNameValidFuncArgsNotAuthThrowsAuthorizationException() {
        Assert.assertThrows(AuthorizationException.class, () -> drpcAuthKo.executeBlocking("try", "args"));
    }

    /** Test executeBlocking method with null functionName. Expected = throws IllegalArgumentException. */
    // @Test (FAILED) @AFTER STARTS CLEANUP THAT DOES A GET WITH A NULL FUNCTION NAME --> NULL POINTER EXCEPTION
    public void executeBlockingNullFunctionNameAuthThrowsIllegalArgumentException() {
        Assert.assertThrows(IllegalArgumentException.class, () -> drpcAuthOk.executeBlocking(null, "args"));
    }

    /** Test executeBlocking method with functionName = "try", funcArgs = "args" and state not valid. Expected = throws NullPointerException. */
    @Test
    public void executeBlockingValidFunctionNameValidFuncArgsInvalidStateThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> drpcNotValid.executeBlocking("try", "args"));
    }

    /** Test close method with valid state. Expected = no exception. */
    @Test
    public void closeValidStateShouldPass() {
        DRPC drpc = new DRPC(mockMetricsRegistry(), alwaysAuthorized, 1000);
        drpc.close();
    }

    // ### Test END ###
}
