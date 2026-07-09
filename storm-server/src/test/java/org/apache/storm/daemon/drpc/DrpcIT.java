package org.apache.storm.daemon.drpc;

import com.codahale.metrics.Meter;
import org.apache.storm.daemon.drpc.utils.DummyWorkerClass;
import org.apache.storm.daemon.drpc.utils.NotBlockingOutstandingRequest;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.generated.DRPCRequest;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.DRPCClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/** Test class which runs integration test on DRPC class with bottom-up strategy*/
@RunWith(MockitoJUnitRunner.class)
public class DrpcIT {

    private DRPC drpcAuthOk;
    private DRPC drpcAuthKo;
    private final IAuthorizer mockAuthOk = Mockito.mock(IAuthorizer.class);
    private final IAuthorizer mockAuthKo = Mockito.mock(IAuthorizer.class);
    private final StormMetricsRegistry mockMetricRegistry = Mockito.mock(StormMetricsRegistry.class);
    private DRPCThrift drpcThriftAuthOk;
    private DRPCThrift drpcThriftAuthKo;

    /** Setting the context of the integration tests */
    @Before
    public void configureTestInstance() {

        Mockito.when(mockAuthOk.permit(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(true);
        Mockito.when(mockAuthKo.permit(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(false);
        Mockito.when(mockMetricRegistry.registerMeter(ArgumentMatchers.any())).thenReturn(new Meter());
        drpcAuthOk = new DRPC(mockMetricRegistry, mockAuthOk, 2000);
        drpcAuthKo = new DRPC(mockMetricRegistry, mockAuthKo, 2000);
        drpcThriftAuthOk = new DRPCThrift(drpcAuthOk);
        drpcThriftAuthKo = new DRPCThrift(drpcAuthKo);
    }

    // DRPC FLOWS INTEGRATION TESTS

    // EXECUTE --> FETCH --> RETURN RESULT

    /** test execute, fetchRequest and returnResult flow with functionName = "try", funcArgs = "args", valid factory and authorized context. Expected = "done" */
    @Test
    public void executeFetchRequestReturnResultFlowValidFunctionNameValidFuncArgsValidFactoryAuthShouldPass() throws AuthorizationException, DRPCExecutionException {

        NotBlockingOutstandingRequest outstandingRequest = drpcAuthOk.execute("try", "args", NotBlockingOutstandingRequest.FACTORY);
        DRPCRequest drpcRequestFetched = drpcAuthOk.fetchRequest("try");
        drpcAuthOk.returnResult(drpcRequestFetched.get_request_id(), "done");
        Assert.assertEquals("done", outstandingRequest.getResult());
    }

    /** test execute, fetchRequest and returnResult flow with functionName = "try", funcArgs = "args", valid factory 2 times and authorized context. Expected = true */
    @Test
    public void executeFetchRequestReturnResultFlowValidFunctionNameValidFuncArgsValidFactory2TimesAuthShouldPass() throws AuthorizationException, DRPCExecutionException {

        NotBlockingOutstandingRequest notBlockingOutstandingRequest1 = drpcAuthOk.execute("try", "args", NotBlockingOutstandingRequest.FACTORY);
        NotBlockingOutstandingRequest notBlockingOutstandingRequest2 = drpcAuthOk.execute("try", "args", NotBlockingOutstandingRequest.FACTORY);
        DRPCRequest drpcRequestFetched1 = drpcAuthOk.fetchRequest("try");
        drpcAuthOk.returnResult(drpcRequestFetched1.get_request_id(), "done");
        DRPCRequest drpcRequestFetched2 = drpcAuthOk.fetchRequest("try");
        drpcAuthOk.returnResult(drpcRequestFetched2.get_request_id(), "done");
        boolean notOverwrite =  notBlockingOutstandingRequest1.getResult().equals("done") && notBlockingOutstandingRequest2.getResult().equals("done");
        Assert.assertTrue(notOverwrite);
    }

    // OTHER COMBINATIONS IN THIS FLOW ARE NOT TESTED BECAUSE I EXPECT THE BEHAVIOR IS EQUAL TO UNIT TESTS

    // EXECUTE BLOCKING --> FETCH --> RETURN RESULT

    /** test executeBlocking, fetchRequest and returnResult flow with functionName = "try", funcArgs = "args" and authorized context. Expected = "done" */
    @Test
    public void executeBlockingFetchRequestReturnResultFlowValidFunctionNameValidFuncArgsAuthShouldPass() throws AuthorizationException, ExecutionException, InterruptedException {

        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            Future<String> future = executor.submit(() -> drpcAuthOk.executeBlocking("try", "args"));
            Thread.sleep(1000);  // FETCH REQUEST HAS TO BE DONE AFTER EXECUTE CALLED BY EXECUTE BLOCKING !
            DRPCRequest drpcRequestFetched = drpcAuthOk.fetchRequest("try");
            drpcAuthOk.returnResult(drpcRequestFetched.get_request_id(), "done");
            Assert.assertEquals("done", future.get());
        }
    }

    /** test executeBlocking, fetchRequest and returnResult flow with functionName = "try", funcArgs = "args" 2 times and authorized context. Expected = "true" */
    @Test
    public void executeBlockingFetchRequestReturnResultFlowValidFunctionNameValidFuncArgs2TimesAuthShouldPass() throws AuthorizationException, ExecutionException, InterruptedException {

        try (ExecutorService executor = Executors.newSingleThreadExecutor();
            ExecutorService executor2 = Executors.newSingleThreadExecutor()) {
            Future<String> future = executor.submit(() -> drpcAuthOk.executeBlocking("try", "args"));
            Future<String> future2 = executor2.submit(() -> drpcAuthOk.executeBlocking("try", "args"));
            Thread.sleep(1000);  // FETCH REQUEST HAS TO BE DONE AFTER EXECUTE CALLED BY EXECUTE BLOCKING !
            DRPCRequest drpcRequestFetched = drpcAuthOk.fetchRequest("try");
            DRPCRequest drpcRequestFetched2 = drpcAuthOk.fetchRequest("try");
            drpcAuthOk.returnResult(drpcRequestFetched.get_request_id(), "done");
            drpcAuthOk.returnResult(drpcRequestFetched2.get_request_id(), "done");
            boolean notOverwrite =  future.get().equals("done") && future2.get().equals("done");
            Assert.assertTrue(notOverwrite);
        }
    }

    // OTHER COMBINATIONS IN THIS FLOW ARE NOT TESTED BECAUSE I EXPECT THE BEHAVIOR IS EQUAL TO UNIT TESTS, (EXECUTEBLOCKING WRAPS EXECUTE)

    // EXECUTE --> FETCH --> FAIL REQUEST

    /** test execute, fetchRequest and failRequest flow with functionName = "try", funcArgs = "args", valid factory and authorized context. Expected = "failed" */
    @Test
    public void executeFetchRequestFailRequestFlowValidFunctionNameValidFuncArgsValidFactoryAuthShouldPass() throws AuthorizationException, DRPCExecutionException {

        NotBlockingOutstandingRequest outstandingRequest = drpcAuthOk.execute("try", "args", NotBlockingOutstandingRequest.FACTORY);
        DRPCRequest drpcRequestFetched = drpcAuthOk.fetchRequest("try");
        drpcAuthOk.failRequest(drpcRequestFetched.get_request_id(), new DRPCExecutionException("failed"));
        String exceptionMessage;

        try {
            outstandingRequest.getResult();
            exceptionMessage = "not failed";

        } catch (DRPCExecutionException e){
            exceptionMessage = e.get_msg();
        }
        Assert.assertEquals("failed", exceptionMessage);
    }

    /** test execute, fetchRequest and failRequest flow with functionName = "try", funcArgs = "args", valid factory 2 times and authorized context. Expected = true */
    @Test
    public void executeFetchRequestFailRequestFlowValidFunctionNameValidFuncArgsValidFactory2TimesAuthShouldPass() throws AuthorizationException, DRPCExecutionException {

        NotBlockingOutstandingRequest notBlockingOutstandingRequest1 = drpcAuthOk.execute("try", "args", NotBlockingOutstandingRequest.FACTORY);
        NotBlockingOutstandingRequest notBlockingOutstandingRequest2 = drpcAuthOk.execute("try", "args", NotBlockingOutstandingRequest.FACTORY);
        DRPCRequest drpcRequestFetched1 = drpcAuthOk.fetchRequest("try");
        drpcAuthOk.failRequest(drpcRequestFetched1.get_request_id(), new DRPCExecutionException("failed"));
        DRPCRequest drpcRequestFetched2 = drpcAuthOk.fetchRequest("try");
        drpcAuthOk.failRequest(drpcRequestFetched2.get_request_id(), new DRPCExecutionException("failed"));
        String exceptionMessage1;
        String exceptionMessage2;

        try {
            notBlockingOutstandingRequest1.getResult();
            exceptionMessage1 = "not failed";

        } catch (DRPCExecutionException e){
            exceptionMessage1 = e.get_msg();
        }

        try {
            notBlockingOutstandingRequest2.getResult();
            exceptionMessage2 = "not failed";

        } catch (DRPCExecutionException e){
            exceptionMessage2 = e.get_msg();
        }
        boolean notOverwrite =  exceptionMessage1.equals("failed") && exceptionMessage2.equals("failed");
        Assert.assertTrue(notOverwrite);
    }

    // OTHER COMBINATIONS IN THIS FLOW ARE NOT TESTED BECAUSE I EXPECT THE BEHAVIOR IS EQUAL TO UNIT TESTS

    // EXECUTE BLOCKING --> FETCH --> FAIL REQUEST

    /** test executeBlocking, fetchRequest and failRequest flow with functionName = "try", funcArgs = "args" and authorized context. Expected = "failed" */
    @Test
    public void executeBlockingFetchRequestFailRequestFlowValidFunctionNameValidFuncArgsAuthShouldPass() throws AuthorizationException, ExecutionException, InterruptedException {

        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            Future<String> future = executor.submit(() -> {
                String result = "not failed";
                try {
                    result = drpcAuthOk.executeBlocking("try", "args");

                }catch (AuthorizationException ignored){

                } catch (DRPCExecutionException e) {
                    result = e.get_msg();
                }
                return result;
            });
            Thread.sleep(1000);  // FETCH REQUEST HAS TO BE DONE AFTER EXECUTE CALLED BY EXECUTE BLOCKING !
            DRPCRequest drpcRequestFetched = drpcAuthOk.fetchRequest("try");
            drpcAuthOk.failRequest(drpcRequestFetched.get_request_id(), new  DRPCExecutionException("failed"));
            Assert.assertEquals("failed", future.get());
        }
    }

    /** test executeBlocking, fetchRequest and failRequest flow with functionName = "try", funcArgs = "args" 2 times and authorized context. Expected = "true" */
    @Test
    public void executeBlockingFetchRequestFailRequestFlowValidFunctionNameValidFuncArgs2TimesAuthShouldPass() throws AuthorizationException, ExecutionException, InterruptedException {

        try (ExecutorService executor = Executors.newSingleThreadExecutor();
             ExecutorService executor2 = Executors.newSingleThreadExecutor()) {
            Future<String> future = executor.submit(() -> {
                String result = "not failed";
                try {
                    result = drpcAuthOk.executeBlocking("try", "args");

                }catch (AuthorizationException ignored){

                } catch (DRPCExecutionException e) {
                    result = e.get_msg();
                }
                return result;
            });
            Future<String> future2 = executor2.submit(() -> {
                String result = "not failed";
                try {
                    result = drpcAuthOk.executeBlocking("try", "args");

                }catch (AuthorizationException ignored){

                } catch (DRPCExecutionException e) {
                    result = e.get_msg();
                }
                return result;
            });
            Thread.sleep(1000);  // FETCH REQUEST HAS TO BE DONE AFTER EXECUTE CALLED BY EXECUTE BLOCKING !
            DRPCRequest drpcRequestFetched = drpcAuthOk.fetchRequest("try");
            DRPCRequest drpcRequest2 = drpcAuthOk.fetchRequest("try");
            drpcAuthOk.failRequest(drpcRequestFetched.get_request_id(), new  DRPCExecutionException("failed"));
            drpcAuthOk.failRequest(drpcRequest2.get_request_id(), new DRPCExecutionException("failed"));
            boolean notOverwrite =  future.get().equals("failed") && future2.get().equals("failed");
            Assert.assertTrue(notOverwrite);
        }
    }

    // OTHER COMBINATIONS IN THIS FLOW ARE NOT TESTED BECAUSE I EXPECT THE BEHAVIOR IS EQUAL TO UNIT TESTS (EXECUTEBLOCKING WRAPS EXECUTE)

    // DRPC THRIFT + DRPC FLOWS

    // EXECUTE --> FETCH REQUEST --> RESULT (AUTH)

    /** test execute, fetchRequest and result flow through DrpcThrift with functionName = "try", funcArgs = "args" and authorized context. Expected = "done" */
    @Test
    public void executeFetchRequestResultFlowThroughDrpcThriftValidFunctionNameValidFuncArgsAuthShouldPass() throws InterruptedException, AuthorizationException, ExecutionException {

        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            Future<String> future = executor.submit(() -> drpcThriftAuthOk.execute("try", "args"));
            Thread.sleep(1000);
            DRPCRequest drpcRequest = drpcThriftAuthOk.fetchRequest("try");
            drpcThriftAuthOk.result(drpcRequest.get_request_id(), "done");
            Assert.assertEquals("done", future.get());
        }
    }

    /** test execute, fetchRequest and result flow through DrpcThrift with functionName = null, funcArgs = "" and authorized context. Expected = IllegalArgumentException */
    @Test
    public void executeFetchRequestResultFlowThroughDrpcThriftNullFunctionNameEmptyFuncArgsAuthThrowsIllegalArgumentException() {

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
                Future<String> future = executor.submit(() -> drpcThriftAuthOk.execute(null, ""));
                Thread.sleep(1000);
                DRPCRequest drpcRequest = drpcThriftAuthOk.fetchRequest(null);
                drpcThriftAuthOk.result(drpcRequest.get_request_id(), "done");
            }
        });
    }

    /** test execute, fetchRequest and result flow through DrpcThrift  with functionName = "", funcArgs = null and authorized context. Expected = "done" */
    @Test
    public void executeFetchRequestResultFlowThroughDrpcThriftEmptyFunctionNameNullFuncArgsAuthShouldPass() throws InterruptedException, AuthorizationException, ExecutionException {

        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            Future<String> future = executor.submit(() -> drpcThriftAuthOk.execute("", null));
            Thread.sleep(1000);
            DRPCRequest drpcRequest = drpcThriftAuthOk.fetchRequest("");
            drpcThriftAuthOk.result(drpcRequest.get_request_id(), "done");
            Assert.assertEquals("done", future.get());
        }
    }

    // EXECUTE --> FETCH REQUEST --> RESULT (NOT AUTH)

    /** test execute, fetchRequest and result flow through DrpcThrift with functionName = "try", funcArgs = "args" and not authorized context. Expected = throws AuthorizationException */
    @Test
    public void executeFetchRequestResultFlowThroughDrpcThriftValidFunctionNameValidFuncArgsNotAuthThrowsAuthorizationException() {

        Assert.assertThrows(AuthorizationException.class, () -> {
            try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
                Future<String> future = executor.submit(() -> drpcThriftAuthKo.execute("try", "args"));
                Thread.sleep(1000);
                DRPCRequest drpcRequest = drpcThriftAuthKo.fetchRequest("try");
                drpcThriftAuthKo.result(drpcRequest.get_request_id(), "done");
            }
        });
    }

    /** test execute, fetchRequest and result flow through DrpcThrift with functionName = null, funcArgs = "" and not authorized context. Expected = throws AuthorizationException */
    @Test
    public void executeFetchRequestResultFlowThroughDrpcThriftNullFunctionNameEmptyFuncArgsNotAuthThrowsAuthorizationException() {

        Assert.assertThrows(AuthorizationException.class, () -> {
            try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
                Future<String> future = executor.submit(() -> drpcThriftAuthKo.execute(null, ""));
                Thread.sleep(1000);
                DRPCRequest drpcRequest = drpcThriftAuthKo.fetchRequest(null);
                drpcThriftAuthKo.result(drpcRequest.get_request_id(), "done");
            }
        });
    }

    /** test execute, fetchRequest and result flow through DrpcThrift with functionName = "", funcArgs = null and not authorized context. Expected = throws AuthorizationException */
    @Test
    public void executeFetchRequestResultFlowThroughDrpcThriftEmptyFunctionNameNullFuncArgsNotAuthThrowsAuthorizationException() {

        Assert.assertThrows(AuthorizationException.class, () -> {

            try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
                Future<String> future = executor.submit(() -> drpcThriftAuthKo.execute(null, ""));
                Thread.sleep(1000);
                DRPCRequest drpcRequest = drpcThriftAuthKo.fetchRequest(null);
                drpcThriftAuthKo.result(drpcRequest.get_request_id(), "done");
            }
        });
    }

    // EXECUTE --> FETCH REQUEST --> FAIL REQUEST V2 (AUTH)

    /** test execute, fetchRequest and failRequestV2 flow through DrpcThrift with functionName = "try", funcArgs = "args" and authorized context. Expected = "failed" */
    @Test
    public void executeFetchRequestFailRequestV2FlowThroughDrpcThriftValidFunctionNameValidFuncArgsAuthShouldPass() throws InterruptedException, AuthorizationException, ExecutionException {

        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            Future<String> future = executor.submit(() ->{
                String result = "not failed";

                try{
                    drpcThriftAuthOk.execute("try", "args");

                }catch(DRPCExecutionException e){
                    result = e.get_msg();
                }
                return result;
            });
            Thread.sleep(1000);
            DRPCRequest drpcRequest = drpcThriftAuthOk.fetchRequest("try");
            drpcThriftAuthOk.failRequestV2(drpcRequest.get_request_id(), new DRPCExecutionException("failed"));
            Assert.assertEquals("failed", future.get());
        }
    }

    /** test execute, fetchRequest and failRequestV2 flow through DrpcThrift with functionName = null, funcArgs = "" and authorized context. Expected = IllegalArgumentException */
    @Test
    public void executeFetchRequestFailRequestV2FlowThroughDrpcThriftNullFunctionNameEmptyFuncArgsAuthThrowsIllegalArgumentException() {

        Assert.assertThrows(IllegalArgumentException.class, () -> {

            try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
                Future<String> future = executor.submit(() -> {
                    String result = "not failed";

                    try {
                        drpcThriftAuthOk.execute(null, "");

                    } catch (DRPCExecutionException e) {
                        result = e.get_msg();
                    }
                    return result;
                });
                Thread.sleep(1000);
                DRPCRequest drpcRequest = drpcThriftAuthOk.fetchRequest(null);
                drpcThriftAuthOk.failRequestV2(drpcRequest.get_request_id(), new DRPCExecutionException("failed"));
            }
        });
    }

    /** test execute, fetchRequest and failRequestV2 flow through DrpcThrift with functionName = "", funcArgs = null and authorized context. Expected = "failed" */
    @Test
    public void executeFetchRequestFailRequestV2FlowThroughDrpcThriftEmptyFunctionNameNullFuncArgsAuthShouldPass() throws InterruptedException, AuthorizationException, ExecutionException {

        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            Future<String> future = executor.submit(() ->{
                String result = "not failed";
                try{
                    drpcThriftAuthOk.execute("", null);

                }catch(DRPCExecutionException e){
                    result = e.get_msg();
                }
                return result;
            });
            Thread.sleep(1000);
            DRPCRequest drpcRequest = drpcThriftAuthOk.fetchRequest("");
            drpcThriftAuthOk.failRequestV2(drpcRequest.get_request_id(), new DRPCExecutionException("failed"));
            Assert.assertEquals("failed", future.get());
        }
    }

    /** test execute, fetchRequest and failRequestV2 flow through DrpcThrift with functionName = "try", funcArgs = "args" and not authorized context. Expected = throws AuthorizationException */
    @Test
    public void executeFetchRequestFailRequestV2FlowThroughDrpcThriftValidFunctionNameValidFuncArgsNotAuthThrowsAuthorizationException() {

        Assert.assertThrows(AuthorizationException.class, () -> {
            try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
                Future<String> future = executor.submit(() -> {
                    String result = "not failed";
                    try {
                        drpcThriftAuthKo.execute("try", "args");
                    } catch (DRPCExecutionException e) {
                        result = e.get_msg();
                    }
                    return result;
                });
                Thread.sleep(1000);
                DRPCRequest drpcRequest = drpcThriftAuthKo.fetchRequest("try");
                drpcThriftAuthKo.failRequestV2(drpcRequest.get_request_id(), new  DRPCExecutionException("failed"));
            }
        });
    }

    /** test execute, fetchRequest and failRequestV2 flow through DrpcThrift with functionName = null, funcArgs = "" and not authorized context. Expected = throws AuthorizationException */
    @Test
    public void executeFetchRequestFailRequestV2FlowThroughDrpcThriftNullFunctionNameEmptyFuncArgsNotAuthThrowsAuthorizationException() {

        Assert.assertThrows(AuthorizationException.class, () -> {
            try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
                Future<String> future = executor.submit(() -> {
                    String result = "not failed";
                    try {
                        drpcThriftAuthKo.execute(null, "");
                    } catch (DRPCExecutionException e) {
                        result = e.get_msg();
                    }
                    return result;
                });
                Thread.sleep(1000);
                DRPCRequest drpcRequest = drpcThriftAuthKo.fetchRequest(null);
                drpcThriftAuthKo.failRequestV2(drpcRequest.get_request_id(), new DRPCExecutionException("failed"));
            }
        });
    }

    /** test execute, fetchRequest and failRequestV2 flow through DrpcThrift with functionName = "", funcArgs = null and not authorized context. Expected = throws AuthorizationException */
    @Test
    public void executeFetchRequestFailRequestV2FlowThroughDrpcThriftEmptyFunctionNameNullFuncArgsNotAuthThrowsAuthorizationException() {

        Assert.assertThrows(AuthorizationException.class, () -> {
            try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
                Future<String> future = executor.submit(() -> {
                    String result = "not failed";
                    try {
                        drpcThriftAuthKo.execute("", null);
                    } catch (Exception e) {
                        result = e.getMessage();
                    }
                    return result;
                });
                Thread.sleep(1000);
                DRPCRequest drpcRequest = drpcThriftAuthKo.fetchRequest("");
                drpcThriftAuthKo.failRequestV2(drpcRequest.get_request_id(), new DRPCExecutionException("failed"));
            }
        });
    }

    // NOT IMPLEMENTED TESTS FOR FAIL REQUEST METHOD BECAUSE IT'S AN ALTERED VERSION OF FAIL REQUEST V2 WHERE IS USED DEFAULT DRPC EXECUTION EXCEPTION

    // MOCKED DRPC CLIENT + DRPC THRIFT + DRPC + DUMMY WORKER CLASS FLOWS

    // EXECUTE --> RETURN RESULT

    /** test execute through DRPC client and return result through DummyWorkerClass with functionName = "try", funcArgs = "args" and authorized context. Expected = "done" */
    @Test
    public void executeThroughDrpcClientReturnResultThroughDummyWorkerClassAuthShouldPass() throws TException, InterruptedException, ExecutionException {

        DRPCClient mockDRPCClient = Mockito.mock(DRPCClient.class);
        Mockito.when(mockDRPCClient.execute(Mockito.any(), Mockito.anyString())).thenAnswer(answer -> drpcThriftAuthOk.execute("try", "args"));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<String> future = executor.submit(() -> mockDRPCClient.execute("try", "args"));
        Thread.sleep(1000);
        DummyWorkerClass dummyWorkerClass = new DummyWorkerClass(drpcThriftAuthOk);
        dummyWorkerClass.executeFunctionAndReturnResult("try");
        Assert.assertEquals("done", future.get());
    }

    /** test execute through DRPC client and return result through DummyWorkerClass with functionName = "try", funcArgs = "args" and not authorized context. Expected = throws AuthorizationException */
    @Test
    public void executeThroughDrpcClientReturnResultThroughDummyWorkerClassNotAuthThrowsAuthorizationException() {

        Assert.assertThrows(AuthorizationException.class, () -> {
            DRPCClient mockDRPCClient = Mockito.mock(DRPCClient.class);
            Mockito.when(mockDRPCClient.execute(Mockito.any(), Mockito.anyString())).thenAnswer(answer -> drpcThriftAuthKo.execute("try", "args"));
            ExecutorService executor = Executors.newSingleThreadExecutor();
            Future<String> future = executor.submit(() -> mockDRPCClient.execute("try", "args"));
            Thread.sleep(1000);
            DummyWorkerClass dummyWorkerClass = new DummyWorkerClass(drpcThriftAuthKo);
            dummyWorkerClass.executeFunctionAndReturnResult("try");
        });
    }

    // EXECUTE --> FAIL REQUEST

    /** test execute through DRPC client and fail request through DummyWorkerClass with functionName = "try", funcArgs = "args" and authorized context. Expected = "failed" */
    @Test
    public void executeThroughDrpcClientFailRequestThroughDummyWorkerClassAuthShouldPass() throws TException, InterruptedException, ExecutionException {

        DRPCClient mockDRPCClient = Mockito.mock(DRPCClient.class);
        Mockito.when(mockDRPCClient.execute(Mockito.any(), Mockito.anyString())).thenAnswer(answer -> drpcThriftAuthOk.execute("try", "args"));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<String> future = executor.submit(() -> {
            String result = "not failed";
            try {
                mockDRPCClient.execute("try", "args");

            } catch (DRPCExecutionException e) {
                result = e.get_msg();
            } catch (TException ignored) {
            }
            return result;
        });
        Thread.sleep(1000);
        DummyWorkerClass dummyWorkerClass = new DummyWorkerClass(drpcThriftAuthOk);
        dummyWorkerClass.executeFunctionAndFailRequest("try");
        Assert.assertEquals("failed", future.get());
    }

    /** test execute through DRPC client and fail request through DummyWorkerClass with functionName = "try", funcArgs = "args" and not authorized context. Expected = throws AuthorizationException */
    @Test
    public void executeThroughDrpcClientFailRequestThroughDummyWorkerClassNotAuthThrowsAuthorizationException() {

        Assert.assertThrows(AuthorizationException.class, () -> {
            DRPCClient mockDRPCClient = Mockito.mock(DRPCClient.class);
            Mockito.when(mockDRPCClient.execute(Mockito.any(), Mockito.anyString())).thenAnswer(answer -> drpcThriftAuthKo.execute("try", "args"));
            ExecutorService executor = Executors.newSingleThreadExecutor();
            Future<String> future = executor.submit(() -> {
                String result = "not failed";
                try {
                    mockDRPCClient.execute("try", "args");

                } catch (DRPCExecutionException e) {
                    result = e.get_msg();
                } catch (TException ignored) {
                }
                return result;
            });
            Thread.sleep(1000);
            DummyWorkerClass dummyWorkerClass = new DummyWorkerClass(drpcThriftAuthKo);
            dummyWorkerClass.executeFunctionAndReturnResult("try");
        });
    }
}
