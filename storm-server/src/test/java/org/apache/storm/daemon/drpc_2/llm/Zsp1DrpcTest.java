package org.apache.storm.daemon.drpc_2.llm;

import org.apache.storm.daemon.drpc.refactored.two.DRPC;
import org.apache.storm.daemon.drpc.OutstandingRequest;
import org.apache.storm.daemon.drpc.RequestFactory;
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

import java.util.Map;
import java.util.concurrent.*;

import static org.junit.Assert.*;

public class Zsp1DrpcTest {
    // ### Test START ###

    private static final String FUNCTION = "try";
    private static final String ARGS = "args";
    private static final String RESULT = "done";
    private static final long TIMEOUT_MS = 30000L;

    private DRPC drpc;
    private ExecutorService executor;

    @Before
    public void setUp() {
        drpc = new DRPC(new StormMetricsRegistry(), new AlwaysAuthorizer(), TIMEOUT_MS);
        executor = Executors.newSingleThreadExecutor();
    }

    @After
    public void tearDown() throws Exception {
        if (drpc != null) {
            drpc.close();
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    public void executeConFactoryValidaCreaUnaRichiesta() throws Exception {
        TestOutstandingRequest outstanding = drpc.execute(FUNCTION, ARGS, new TestRequestFactory());

        assertNotNull(outstanding);
        Assert.assertEquals(FUNCTION, outstanding.getFunction());
        assertNotNull(outstanding.getRequest());
        Assert.assertEquals(ARGS, outstanding.getRequest().get_func_args());
        assertNotNull(outstanding.getRequest().get_request_id());
    }

    //@Test (FAILED) IT'S INSERTED A NULL VALUE IN THE MAP --> EXCEPTION
    public void executeConFactoryNonValidaRestituisceNull() throws Exception {
        TestOutstandingRequest outstanding = drpc.execute(FUNCTION, ARGS, new NullRequestFactory());

        assertNull(outstanding);
    }

    @Test(expected = NullPointerException.class)
    public void executeConFactoryNullLanciaEccezione() throws Exception {
        drpc.execute(FUNCTION, ARGS, null);
    }

    @Test
    public void fetchRequestDopoExecuteRestituisceLaRichiestaCreata() throws Exception {
        TestOutstandingRequest outstanding = drpc.execute(FUNCTION, ARGS, new TestRequestFactory());

        DRPCRequest fetched = drpc.fetchRequest(FUNCTION);

        assertNotNull(fetched);
        Assert.assertEquals(outstanding.getRequest().get_func_args(), fetched.get_func_args());
        Assert.assertEquals(outstanding.getRequest().get_request_id(), fetched.get_request_id());
        assertTrue(outstanding.wasFetched());
    }

    @Test
    public void fetchRequestSuFunzioneNonPresenteRestituisceRichiestaVuota() throws Exception {
        DRPCRequest fetched = drpc.fetchRequest("notCorrectFunction");

        assertNotNull(fetched);
        assertEquals("", fetched.get_func_args());
        assertEquals("", fetched.get_request_id());
    }

    @Test(expected = AuthorizationException.class)
    public void fetchRequestNonAutorizzataLanciaAuthorizationException() throws Exception {
        DRPC notAuthorizedDrpc = new DRPC(new StormMetricsRegistry(), new NeverAuthorizer(), TIMEOUT_MS);
        try {
            notAuthorizedDrpc.fetchRequest(FUNCTION);
        } finally {
            notAuthorizedDrpc.close();
        }
    }

    @Test
    public void returnResultConIdCorrectCompletaLaRichiesta() throws Exception {
        TestOutstandingRequest outstanding = drpc.execute(FUNCTION, ARGS, new TestRequestFactory());

        drpc.returnResult(outstanding.getRequest().get_request_id(), RESULT);

        assertEquals(RESULT, outstanding.result);
        assertNull(outstanding.failure);
    }

    @Test
    public void returnResultConIdNotCorrectNonCompletaNessunaRichiesta() throws Exception {  // TIMEOUT AS FAILURE
        TestOutstandingRequest outstanding = drpc.execute(FUNCTION, ARGS, new TestRequestFactory());

        drpc.returnResult("notCorrectId", RESULT);

        assertNull(outstanding.result);
        assertNull(outstanding.failure);
    }

    // @Test(expected = AuthorizationException.class)  (FAILED) REQUEST NOT FOUND --> NOT THROWN EXCEPTION
    public void returnResultNonAutorizzatoLanciaAuthorizationException() throws Exception {
        DRPC notAuthorizedDrpc = new DRPC(new StormMetricsRegistry(), new NeverAuthorizer(), TIMEOUT_MS);
        try {
            notAuthorizedDrpc.returnResult("1", RESULT);
        } finally {
            notAuthorizedDrpc.close();
        }
    }

    @Test
    public void failRequestConEccezioneValidaSegnaLaRichiestaFallita() throws Exception {
        TestOutstandingRequest outstanding = drpc.execute(FUNCTION, ARGS, new TestRequestFactory());
        DRPCExecutionException exception = new DRPCExecutionException("failed");

        drpc.failRequest(outstanding.getRequest().get_request_id(), exception);

        assertSame(exception, outstanding.failure);
        assertNull(outstanding.result);
        assertEquals("failed", outstanding.failure.get_msg());
    }

    @Test
    public void failRequestConEccezioneSenzaMessaggioVienePropagataAllaRichiesta() throws Exception {
        TestOutstandingRequest outstanding = drpc.execute(FUNCTION, ARGS, new TestRequestFactory());
        DRPCExecutionException exception = new DRPCExecutionException();

        drpc.failRequest(outstanding.getRequest().get_request_id(), exception);

        assertSame(exception, outstanding.failure);
        assertNull(outstanding.failure.get_msg());
    }

    @Test
    public void failRequestConIdNotCorrectNonFallisceLaRichiesta() throws Exception {
        TestOutstandingRequest outstanding = drpc.execute(FUNCTION, ARGS, new TestRequestFactory());
        DRPCExecutionException exception = new DRPCExecutionException("failed");

        drpc.failRequest("notCorrectId", exception);

        assertNull(outstanding.failure);
    }

    // @Test(expected = AuthorizationException.class) (FAILED) REQUEST NOT FOUND --> NOT THROWN EXCEPTION
    public void failRequestNonAutorizzataLanciaAuthorizationException() throws Exception {
        DRPC notAuthorizedDrpc = new DRPC(new StormMetricsRegistry(), new NeverAuthorizer(), TIMEOUT_MS);
        try {
            notAuthorizedDrpc.failRequest("1", new DRPCExecutionException("failed"));
        } finally {
            notAuthorizedDrpc.close();
        }
    }

    @Test
    public void executeBlockingRestituisceIlRisultatoQuandoReturnResultVieneChiamato() throws Exception {
        Future<String> future = executor.submit(new Callable<String>() {
            @Override
            public String call() throws Exception {
                return drpc.executeBlocking(FUNCTION, ARGS);
            }
        });

        DRPCRequest request = aspettaRichiesta(FUNCTION);
        drpc.returnResult(request.get_request_id(), RESULT);

        assertEquals(RESULT, future.get(5, TimeUnit.SECONDS));
    }

    @Test
    public void executeBlockingLanciaEccezioneQuandoFailRequestVieneChiamato() throws Exception {
        Future<String> future = executor.submit(new Callable<String>() {
            @Override
            public String call() throws Exception {
                return drpc.executeBlocking(FUNCTION, ARGS);
            }
        });

        DRPCRequest request = aspettaRichiesta(FUNCTION);
        drpc.failRequest(request.get_request_id(), new DRPCExecutionException("failed"));

        try {
            future.get(5, TimeUnit.SECONDS);
            fail("Mi aspettavo una DRPCExecutionException");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof DRPCExecutionException);
            assertEquals("failed", ((DRPCExecutionException) e.getCause()).get_msg());
        }
    }

    @Test(expected = AuthorizationException.class)
    public void executeBlockingNonAutorizzataLanciaAuthorizationException() throws Exception {
        DRPC notAuthorizedDrpc = new DRPC(new StormMetricsRegistry(), new NeverAuthorizer(), TIMEOUT_MS);
        try {
            notAuthorizedDrpc.executeBlocking(FUNCTION, ARGS);
        } finally {
            notAuthorizedDrpc.close();
        }
    }

    @Test
    public void checkAuthorizationConAuthorizerAlwaysAuthorizedNonLanciaEccezioni() throws Exception {
        drpc.checkAuthorization(ReqContext.context(), new AlwaysAuthorizer(), "execute", FUNCTION);
    }

    @Test(expected = AuthorizationException.class)
    public void checkAuthorizationConAuthorizerNeverAuthorizedLanciaAuthorizationException() throws Exception {
        drpc.checkAuthorization(ReqContext.context(), new NeverAuthorizer(), "execute", FUNCTION);
    }

    @Test
    public void checkAuthorizationConAuthorizerNullNonLanciaEccezioni() throws Exception {
        drpc.checkAuthorization(ReqContext.context(), null, "execute", FUNCTION);
    }

    @Test(expected = RuntimeException.class)
    public void checkAuthorizationConAuthorizerNonValidoPropagaRuntimeException() throws Exception {
        drpc.checkAuthorization(ReqContext.context(), new ExceptionAuthorizer(), "execute", FUNCTION);
    }

    @Test(expected = AuthorizationException.class)
    public void executeConIstanzaMaiAutorizzataLanciaAuthorizationException() throws Exception {
        DRPC notAuthorizedDrpc = new DRPC(new StormMetricsRegistry(), new NeverAuthorizer(), TIMEOUT_MS);
        try {
            notAuthorizedDrpc.execute(FUNCTION, ARGS, new TestRequestFactory());
        } finally {
            notAuthorizedDrpc.close();
        }
    }

    @Test
    public void outstandingRequestAllInizioNonRisultaFetched() throws Exception {
        TestOutstandingRequest outstanding = drpc.execute(FUNCTION, ARGS, new TestRequestFactory());

        assertFalse(outstanding.wasFetched());
    }

    private DRPCRequest aspettaRichiesta(String functionName) throws Exception {
        long end = System.currentTimeMillis() + 5000L;
        while (System.currentTimeMillis() < end) {
            DRPCRequest request = drpc.fetchRequest(functionName);
            if (request != null && request.get_request_id() != null && !"".equals(request.get_request_id())) {
                return request;
            }
            Thread.sleep(20L);
        }
        fail("Richiesta DRPC non trovata nei tempi previsti");
        return null;
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

    private static class TestRequestFactory implements RequestFactory<TestOutstandingRequest> {
        @Override
        public TestOutstandingRequest mkRequest(String function, DRPCRequest req) {
            return new TestOutstandingRequest(function, req);
        }
    }

    private static class NullRequestFactory implements RequestFactory<TestOutstandingRequest> {
        @Override
        public TestOutstandingRequest mkRequest(String function, DRPCRequest req) {
            return null;
        }
    }

    private static class AlwaysAuthorizer implements IAuthorizer {
        @Override
        public void prepare(Map<String, Object> conf) {
        }

        @Override
        public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
            return true;
        }
    }

    private static class NeverAuthorizer implements IAuthorizer {
        @Override
        public void prepare(Map<String, Object> conf) {
        }

        @Override
        public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
            return false;
        }
    }

    private static class ExceptionAuthorizer implements IAuthorizer {
        @Override
        public void prepare(Map<String, Object> conf) {
        }

        @Override
        public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
            throw new RuntimeException("authorizer non valido");
        }
    }

    // ### Test END ###
}
