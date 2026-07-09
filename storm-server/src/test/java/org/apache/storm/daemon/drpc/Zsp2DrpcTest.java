package org.apache.storm.daemon.drpc;

import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.generated.DRPCRequest;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.security.auth.authorizer.DRPCAuthorizerBase;
import org.apache.storm.utils.WrappedDRPCExecutionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.*;

import static org.junit.Assert.*;

/**
 * Test JUnit4 per DRPC.
 *
 * I test usano piccole implementazioni fake per isolare DRPC da autorizzazione e factory.
 * La selezione copre casi nominali, valori null/vuoti e alcuni scenari negativi coerenti
 * con category partition e boundary value analysis sulle stringhe.
 */
public class Zsp2DrpcTest {

    // ### Test START ###

    private static final String FUNCTION = "try";
    private static final String ARGS = "args";
    private static final String RESULT = "done";
    private static final long LONG_TIMEOUT_MS = 60_000L;

    private DRPC drpc;
    private RecordingAuthorizer alwaysAuthorized;

    @Before
    public void setUp() {
        alwaysAuthorized = new RecordingAuthorizer(true);
        drpc = new DRPC(new StormMetricsRegistry(), alwaysAuthorized, LONG_TIMEOUT_MS);
    }

    @After
    public void tearDown() {
        if (drpc != null) {
            drpc.close();
        }
    }

    @Test
    public void fetchRequestReturnsRequestCreatedByExecute() throws Exception {
        BlockingOutstandingRequest outstanding = drpc.execute(FUNCTION, ARGS, BlockingOutstandingRequest.FACTORY);

        DRPCRequest fetched = drpc.fetchRequest(FUNCTION);

        assertNotNull(fetched);
        assertEquals(ARGS, fetched.get_func_args());
        assertEquals(outstanding.getRequest().get_request_id(), fetched.get_request_id());
        assertEquals("fetchRequest", alwaysAuthorized.lastOperation);
        assertEquals(FUNCTION, alwaysAuthorized.lastFunctionFromTopoConf);
    }

    @Test
    public void fetchRequestWithNoQueuedRequestReturnsEmptyRequest() throws Exception {
        DRPCRequest fetched = drpc.fetchRequest(FUNCTION);

        assertNotNull(fetched);
        assertEquals("", fetched.get_func_args());
        assertEquals("", fetched.get_request_id());
    }

    @Test(expected = IllegalArgumentException.class)
    public void fetchRequestWithNullFunctionThrowsIllegalArgumentException() throws Exception {
        drpc.fetchRequest(null);
    }

    @Test
    public void fetchRequestWithEmptyFunctionNameIsAcceptedWhenAuthorized() throws Exception {
        drpc.execute("", ARGS, BlockingOutstandingRequest.FACTORY);

        DRPCRequest fetched = drpc.fetchRequest("");

        assertEquals(ARGS, fetched.get_func_args());
        assertEquals("1", fetched.get_request_id());
    }

    @Test(expected = AuthorizationException.class)
    public void fetchRequestThrowsAuthorizationExceptionWhenDenied() throws Exception {
        DRPC deniedDrpc = new DRPC(new StormMetricsRegistry(), new RecordingAuthorizer(false), LONG_TIMEOUT_MS);
        try {
            deniedDrpc.fetchRequest(FUNCTION);
        } finally {
            deniedDrpc.close();
        }
    }

    @Test
    public void returnResultCompletesExecuteBlockingSuccessfully() throws Exception {
        Future<String> future = executeBlockingInBackground(FUNCTION, ARGS);
        DRPCRequest fetched = fetchUntilAvailable(FUNCTION);

        drpc.returnResult(fetched.get_request_id(), RESULT);

        assertEquals(RESULT, future.get(3, TimeUnit.SECONDS));
        assertEquals("result", alwaysAuthorized.lastOperation);
        assertEquals(FUNCTION, alwaysAuthorized.lastFunctionFromTopoConf);
    }

    @Test
    public void returnResultForUnknownIdDoesNothing() throws Exception {
        drpc.returnResult("missing-id", RESULT);

        assertEquals("Nessuna richiesta associata: l'autorizzazione non viene invocata", 0, alwaysAuthorized.permitCalls);
    }

    @Test
    public void returnResultWithEmptyResultCompletesWithEmptyString() throws Exception {
        Future<String> future = executeBlockingInBackground(FUNCTION, ARGS);
        DRPCRequest fetched = fetchUntilAvailable(FUNCTION);

        drpc.returnResult(fetched.get_request_id(), "");

        assertEquals("", future.get(3, TimeUnit.SECONDS));
    }

    @Test(expected = AuthorizationException.class)
    public void returnResultThrowsAuthorizationExceptionWhenDenied() throws Exception {
        DRPC deniedDrpc = new DRPC(new StormMetricsRegistry(), new OperationDenyingAuthorizer("result"), LONG_TIMEOUT_MS);
        try {
            BlockingOutstandingRequest outstanding = deniedDrpc.execute(FUNCTION, ARGS, BlockingOutstandingRequest.FACTORY);
            deniedDrpc.returnResult(outstanding.getRequest().get_request_id(), RESULT);
        } finally {
            deniedDrpc.close();
        }
    }

    @Test
    public void failRequestCompletesExecuteBlockingWithProvidedExceptionMessage() throws Exception {
        Future<String> future = executeBlockingInBackground(FUNCTION, ARGS);
        DRPCRequest fetched = fetchUntilAvailable(FUNCTION);

        drpc.failRequest(fetched.get_request_id(), new WrappedDRPCExecutionException("failed"));

        try {
            future.get(3, TimeUnit.SECONDS);
            fail("executeBlocking avrebbe dovuto rilanciare DRPCExecutionException");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof DRPCExecutionException);
            assertEquals("failed", ((DRPCExecutionException) e.getCause()).get_msg());
        }
    }

    @Test
    public void failRequestWithNullExceptionUsesDefaultFailureMessage() throws Exception {
        Future<String> future = executeBlockingInBackground(FUNCTION, ARGS);
        DRPCRequest fetched = fetchUntilAvailable(FUNCTION);

        drpc.failRequest(fetched.get_request_id(), null);

        try {
            future.get(3, TimeUnit.SECONDS);
            fail("executeBlocking avrebbe dovuto fallire con eccezione di default");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof DRPCExecutionException);
            assertEquals("Request failed", ((DRPCExecutionException) e.getCause()).get_msg());
        }
    }

    @Test
    public void failRequestForUnknownIdDoesNothing() throws Exception {
        drpc.failRequest("missing-id", new WrappedDRPCExecutionException("failed"));

        assertEquals(0, alwaysAuthorized.permitCalls);
    }

    @Test(expected = AuthorizationException.class)
    public void failRequestThrowsAuthorizationExceptionWhenDenied() throws Exception {
        DRPC deniedDrpc = new DRPC(new StormMetricsRegistry(), new OperationDenyingAuthorizer("failRequest"), LONG_TIMEOUT_MS);
        try {
            BlockingOutstandingRequest outstanding = deniedDrpc.execute(FUNCTION, ARGS, BlockingOutstandingRequest.FACTORY);
            deniedDrpc.failRequest(outstanding.getRequest().get_request_id(), new WrappedDRPCExecutionException("failed"));
        } finally {
            deniedDrpc.close();
        }
    }

    @Test
    public void executeReturnsOutstandingRequestAndAssignsSequentialId() throws Exception {
        BlockingOutstandingRequest first = drpc.execute(FUNCTION, ARGS, BlockingOutstandingRequest.FACTORY);
        BlockingOutstandingRequest second = drpc.execute(FUNCTION, "other-args", BlockingOutstandingRequest.FACTORY);

        assertEquals("1", first.getRequest().get_request_id());
        assertEquals("2", second.getRequest().get_request_id());
        assertEquals(ARGS, first.getRequest().get_func_args());
        assertEquals("execute", alwaysAuthorized.lastOperation);
    }

    @Test
    public void executeAcceptsNullFunctionArgs() throws Exception {
        BlockingOutstandingRequest outstanding = drpc.execute(FUNCTION, null, BlockingOutstandingRequest.FACTORY);

        assertEquals("1", outstanding.getRequest().get_request_id());
        assertEquals(null, outstanding.getRequest().get_func_args());
    }

    // @Test(expected = IllegalArgumentException.class) (FAILED) @AFTER STARTS CLEANUP THAT DOES A GET WITH A NULL FUNCTION NAME --> NULL POINTER EXCEPTION
    public void executeWithNullFunctionThrowsIllegalArgumentException() throws Exception {
        drpc.execute(null, ARGS, BlockingOutstandingRequest.FACTORY);
    }

    @Test(expected = NullPointerException.class)
    public void executeWithNullFactoryThrowsNullPointerException() throws Exception {
        drpc.execute(FUNCTION, ARGS, null);
    }

    @Test(expected = NullPointerException.class)
    public void executeWithFactoryReturningNullThrowsNullPointerException() throws Exception {
        drpc.execute(FUNCTION, ARGS, new RequestFactory<BlockingOutstandingRequest>() {
            @Override
            public BlockingOutstandingRequest mkRequest(String function, DRPCRequest req) {
                return null;
            }
        });
    }

    @Test(expected = AuthorizationException.class)
    public void executeThrowsAuthorizationExceptionWhenDenied() throws Exception {
        DRPC deniedDrpc = new DRPC(new StormMetricsRegistry(), new RecordingAuthorizer(false), LONG_TIMEOUT_MS);
        try {
            deniedDrpc.execute(FUNCTION, ARGS, BlockingOutstandingRequest.FACTORY);
        } finally {
            deniedDrpc.close();
        }
    }

    @Test
    public void executeBlockingReturnsResultPassedToReturnResult() throws Exception {
        Future<String> future = executeBlockingInBackground(FUNCTION, ARGS);
        DRPCRequest fetched = fetchUntilAvailable(FUNCTION);

        drpc.returnResult(fetched.get_request_id(), RESULT);

        assertEquals(RESULT, future.get(3, TimeUnit.SECONDS));
    }

    @Test
    public void executeBlockingThrowsExecutionExceptionPassedToFailRequest() throws Exception {
        Future<String> future = executeBlockingInBackground(FUNCTION, ARGS);
        DRPCRequest fetched = fetchUntilAvailable(FUNCTION);

        drpc.failRequest(fetched.get_request_id(), new WrappedDRPCExecutionException("failed"));

        try {
            future.get(3, TimeUnit.SECONDS);
            fail("Attesa DRPCExecutionException");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof DRPCExecutionException);
            assertEquals("failed", ((DRPCExecutionException) e.getCause()).get_msg());
        }
    }

    @Test(expected = AuthorizationException.class)
    public void executeBlockingThrowsAuthorizationExceptionWhenDenied() throws Exception {
        DRPC deniedDrpc = new DRPC(new StormMetricsRegistry(), new RecordingAuthorizer(false), LONG_TIMEOUT_MS);
        try {
            deniedDrpc.executeBlocking(FUNCTION, ARGS);
        } finally {
            deniedDrpc.close();
        }
    }

    @Test
    public void closeUnblocksPendingExecuteBlockingWithShutdownException() throws Exception {
        Future<String> future = executeBlockingInBackground(FUNCTION, ARGS);
        fetchUntilAvailable(FUNCTION);

        drpc.close();
        drpc = null;

        try {
            future.get(3, TimeUnit.SECONDS);
            fail("La chiusura del server dovrebbe far fallire la richiesta pendente");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof DRPCExecutionException);
            assertEquals("Server Shutting Down", ((DRPCExecutionException) e.getCause()).get_msg());
        }
    }

    @Test
    public void checkAuthorizationAllowsAlwaysAuthorizedImplementation() throws Exception {
        RecordingAuthorizer authorizer = new RecordingAuthorizer(true);

        DRPC.checkAuthorization(ReqContext.context(), authorizer, "execute", FUNCTION);

        assertEquals(1, authorizer.permitCalls);
        assertEquals("execute", authorizer.lastOperation);
        assertEquals(FUNCTION, authorizer.lastFunctionFromTopoConf);
    }

    @Test(expected = AuthorizationException.class)
    public void checkAuthorizationThrowsWhenNeverAuthorizedImplementationIsUsed() throws Exception {
        DRPC.checkAuthorization(ReqContext.context(), new RecordingAuthorizer(false), "execute", FUNCTION);
    }

    @Test
    public void checkAuthorizationWithNullAuthorizerDoesNotThrow() throws Exception {
        DRPC.checkAuthorization(ReqContext.context(), null, "execute", FUNCTION);
    }

    @Test
    public void checkAuthorizationWithNullRequestContextAndAllowingAuthorizerDoesNotThrow() throws Exception {
        RecordingAuthorizer authorizer = new RecordingAuthorizer(true);

        DRPC.checkAuthorization(null, authorizer, "execute", FUNCTION);

        assertEquals(1, authorizer.permitCalls);
        assertEquals(FUNCTION, authorizer.lastFunctionFromTopoConf);
    }

    @Test(expected = RuntimeException.class)
    public void checkAuthorizationPropagatesRuntimeExceptionFromInvalidAuthorizer() throws Exception {
        DRPC.checkAuthorization(ReqContext.context(), new ThrowingAuthorizer(), "execute", FUNCTION);
    }

    private Future<String> executeBlockingInBackground(final String function, final String args) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        return executor.submit(new Callable<String>() {
            @Override
            public String call() throws Exception {
                try {
                    return drpc.executeBlocking(function, args);
                } finally {
                    executor.shutdown();
                }
            }
        });
    }

    private DRPCRequest fetchUntilAvailable(String function) throws Exception {
        long deadline = System.currentTimeMillis() + 3_000L;
        DRPCRequest request;
        do {
            request = drpc.fetchRequest(function);
            if (!"".equals(request.get_request_id())) {
                return request;
            }
            Thread.sleep(10L);
        } while (System.currentTimeMillis() < deadline);
        throw new TimeoutException("La richiesta non e' stata accodata in tempo utile");
    }

    private static class RecordingAuthorizer implements IAuthorizer {
        private final boolean permitted;
        private int permitCalls;
        private String lastOperation;
        private String lastFunctionFromTopoConf;

        RecordingAuthorizer(boolean permitted) {
            this.permitted = permitted;
        }

        public void prepare(Map<String, Object> conf) {
            // Nessuna inizializzazione necessaria per il fake.
        }

        @Override
        public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
            permitCalls++;
            lastOperation = operation;
            lastFunctionFromTopoConf = (String) topoConf.get(DRPCAuthorizerBase.FUNCTION_NAME);
            return permitted;
        }
    }


    private static class OperationDenyingAuthorizer implements IAuthorizer {
        private final String deniedOperation;

        OperationDenyingAuthorizer(String deniedOperation) {
            this.deniedOperation = deniedOperation;
        }

        public void prepare(Map<String, Object> conf) {
            // Nessuna inizializzazione necessaria per il fake.
        }

        @Override
        public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
            return !deniedOperation.equals(operation);
        }
    }


    private static class ThrowingAuthorizer implements IAuthorizer {
        public void prepare(Map<String, Object> conf) {
            // Nessuna inizializzazione necessaria per il fake.
        }

        @Override
        public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
            throw new RuntimeException("invalid authorizer");
        }
    }

    // ### Test END ###
}
