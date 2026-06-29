package org.apache.storm.daemon.drpc;

import com.codahale.metrics.Meter;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.generated.DRPCRequest;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.security.auth.ReqContext;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;

/** Test class which runs unit test on DRPC class */
@RunWith(value = MockitoJUnitRunner.class)
public class DrpcTest {

    private final IAuthorizer mockAuthKo = Mockito.mock(IAuthorizer.class);
    private final IAuthorizer mockAuthOk = Mockito.mock(IAuthorizer.class);
    private final IAuthorizer dummyMockAuth = Mockito.mock(IAuthorizer.class);
    private final StormMetricsRegistry mockMetricRegistry = Mockito.mock(StormMetricsRegistry.class);
    private final StormMetricsRegistry dummyMockMetricRegistry = Mockito.mock(StormMetricsRegistry.class);
    private DRPC drpcAuthKo;
    private DRPC drpcAuthOk;
    private DRPC drpcNotValid;

    /** Setting the context of the unit tests */
    @Before
    public void configureTestInstance() {

        Mockito.when(mockAuthKo.permit(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(false);
        Mockito.when(mockAuthOk.permit(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(true);
        Mockito.when(mockMetricRegistry.registerMeter(ArgumentMatchers.any())).thenReturn(new Meter());
        drpcAuthKo = new DRPC(mockMetricRegistry, mockAuthKo, 1000);
        drpcAuthOk = new DRPC(mockMetricRegistry, mockAuthOk, 1000);
        drpcNotValid = new DRPC(dummyMockMetricRegistry, dummyMockAuth, 1000);
    }

    //NOT AUTH FETCH REQUEST TESTS

    /** Test fetchRequest method with functionName = "" and state not authorized. Expected = throws AuthorizationException */
    @Test
    public void fetchRequestEmptyFunctionNameNotAuthThrowsAuthorizationException() {

        Assert.assertThrows(AuthorizationException.class, () -> drpcAuthKo.fetchRequest(""));
    }

    /** Test fetchRequest method with functionName = null and state not authorized. Expected = throws AuthorizationException */
    @Test
    public void fetchRequestNullFunctionNameNotAuthThrowsAuthorizationException() {

        Assert.assertThrows(AuthorizationException.class, () -> drpcAuthKo.fetchRequest(null));
    }

    /** Test fetchRequest method with functionName = "try" (not existing) and state not authorized. Expected = throws AuthorizationException */
    @Test
    public void fetchRequestNotCorrectFunctionNameNotAuthThrowsAuthorizationException() {

        Assert.assertThrows(AuthorizationException.class, () -> drpcAuthKo.fetchRequest("try"));
    }

    /** Test fetchRequest method with functionName = "try" (existing) and state not authorized. Expected = throws AuthorizationException */
    @SuppressWarnings("unchecked")
    @Test
    public void fetchRequestCorrectFunctionNameNotAuthThrowsAuthorizationException() {

        Assert.assertThrows(AuthorizationException.class, () ->{
            RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
            DRPCRequest expectedRequest = new DRPCRequest("args", "1");
            OutstandingRequest outstandingRequest = new DoNothingOutstandingRequest("function", expectedRequest);
            Mockito.lenient().when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
            drpcAuthKo.execute("try", "args",  factory);
            drpcAuthKo.fetchRequest("try");
        });
    }

    //AUTH FETCH REQUEST TESTS

    /** Test fetchRequest method with functionName = "" and state authorized. Expected = DRPCRequest with func_args = "" and request_id = "" */
    @Test
    public void fetchRequestEmptyFunctionNameAuthShouldPass() throws AuthorizationException {

        DRPCRequest request = drpcAuthOk.fetchRequest("");
        Assert.assertEquals(new DRPCRequest("",""), request);
    }

    /** Test fetchRequest method with functionName = null and state authorized. Expected = throws IllegalArgumentException */
    @Test
    public void fetchRequestNullFunctionNameAuthThrowsIllegalArgumentException() {

        Assert.assertThrows(IllegalArgumentException.class, () -> drpcAuthOk.fetchRequest(null));
    }

    /** Test fetchRequest method with functionName = "try" (not existing) and state authorized. Expected = DRPCRequest with func_Args = "" and request_id = "" */
    @Test
    public void fetchRequestNotCorrectFunctionNameAuthShouldPass() throws AuthorizationException {

        DRPCRequest request = drpcAuthOk.fetchRequest("try");
        Assert.assertEquals(new DRPCRequest("",""), request);
    }

    /** Test fetchRequest method with functionName = "try" (existing) and state authorized. Expected = DRPCRequest with func_Args = "args" and request_id = "1" */
    @SuppressWarnings("unchecked")
    @Test
    public void fetchRequestCorrectFunctionNameAuthShouldPass() throws AuthorizationException {

        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        DRPCRequest expectedRequest = new DRPCRequest("args", "1");
        OutstandingRequest outstandingRequest = new  DoNothingOutstandingRequest("try", expectedRequest);
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
        drpcAuthOk.execute("try", "args", factory);
        DRPCRequest actualRequest = drpcAuthOk.fetchRequest("try");
        Assert.assertEquals(expectedRequest, actualRequest);
    }

    //NOT AUTH EXECUTE TESTS

    /** Test execute method with functionName = "", funcArgs = null, valid factory instance and state not authorized. Expected = throws AuthorizationException */
    @SuppressWarnings("unchecked")
    @Test
    public void executeEmptyFunctionNameNullFuncArgsValidFactoryNotAuthThrowsAuthorizationException() {

        Assert.assertThrows(AuthorizationException.class, () -> {
            RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
            Mockito.lenient().when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(Mockito.mock(OutstandingRequest.class));
            drpcAuthKo.execute("", null, factory);
        });
    }

    /** Test execute method with functionName = null, funcArgs = "", factory = null and state not authorized. Expected = throws AuthorizationException */
    @Test
    public void executeNullFunctionNameEmptyFuncArgsNullFactoryNotAuthThrowsAuthorizationException(){

        Assert.assertThrows(AuthorizationException.class, () -> drpcAuthKo.execute(null, "", null));
    }

    /** Test execute method with functionName = "try", funcArgs = "args", not valid factory (mkRequest return always null) and state not authorized. Expected = throws AuthorizationException */
    @SuppressWarnings("unchecked")
    @Test
    public void executeValidFunctionNameValidFuncArgsNotValidFactoryNotAuthThrowsAuthorizationException(){

        Assert.assertThrows(AuthorizationException.class, () -> {
            RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
            Mockito.lenient().when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(null);
            drpcAuthKo.execute("try", "args", factory);
        });
    }

    //AUTH EXECUTE TESTS

    /** Test execute method with functionName = "try", funcArgs = null, valid factory instance and state authorized. Expected = OutstandingRequest with function = "try" and DRPCRequest(null, 1) */
    @SuppressWarnings("unchecked")
    @Test
    public void executeValidFunctionNameNullFuncArgsValidFactoryAuthShouldPass() throws AuthorizationException {

        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        DRPCRequest drpcRequest = new DRPCRequest(null, "1");
        OutstandingRequest expectedRequest = new DoNothingOutstandingRequest("try", drpcRequest);
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(expectedRequest);
        OutstandingRequest actual = drpcAuthOk.execute("try", null, factory);
        Assert.assertEquals(expectedRequest, actual);
    }

    /** Test execute method with functionName = null, funcArgs = "", factory = null and state authorized. Expected = throws NullPointerException */
    @Test
    public void executeNullFunctionNameEmptyFuncArgsNullFactoryAuthThrowsNullPointerException(){

        Assert.assertThrows(NullPointerException.class, () -> drpcAuthOk.execute(null, "", null));
    }

    /** Test execute method with functionName = "try", funcArgs = "args", not valid factory (mkRequest return always null) and state authorized. Expected = throws NullPointerException */
    @SuppressWarnings("unchecked")
    @Test
    public void executeValidFunctionNameValidFuncArgsNotValidFactoryAuthThrowsNullPointerException(){ //CONCURRENT HASHMAP DOESN'T ALLOW NULL KEY AND/OR VALUE

        Assert.assertThrows(NullPointerException.class, () -> {
            RequestFactory<OutstandingRequest> factory = mock(RequestFactory.class);
            Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(null);
            drpcAuthOk.execute("try", "args", factory);
        });
    }

    //NOT AUTH EXECUTE BLOCKING TESTS

    /** Test executeBlocking method with functionName = "", funcArgs = null and state not authorized. Expected = throws AuthorizationException */
    @Test
    public void executeBlockingEmptyFunctionNameNullFuncArgsNotAuthThrowsAuthorizationException() {

        Assert.assertThrows(AuthorizationException.class, () -> drpcAuthKo.executeBlocking("", null));
    }

    /** Test executeBlocking method with functionName = null, funcArgs = "" and state not authorized. Expected = throws AuthorizationException */
    @Test
    public void executeBlockingNullFunctionNameEmptyFuncArgsNotAuthThrowsAuthorizationException(){

        Assert.assertThrows(AuthorizationException.class, () -> drpcAuthKo.executeBlocking(null, ""));
    }

    /** Test executeBlocking method with functionName = "try", funcArgs = "args" and state not authorized. Expected = throws AuthorizationException */
    @Test
    public void executeBlockingValidFunctionNameValidFuncArgsNotAuthThrowsAuthorizationException(){

        Assert.assertThrows(AuthorizationException.class, () -> drpcAuthKo.executeBlocking("try", "args"));
    }

    //AUTH EXECUTE BLOCKING TEST

    /** Test executeBlocking method with functionName = "", funcArgs = null and state authorized. Expected = "done" */
    @Test
    public void executeBlockingEmptyFunctionNameNullFuncArgsAuthShouldPass() throws DRPCExecutionException, AuthorizationException {

        BlockingOutstandingRequest blockingOutstandingRequest = Mockito.mock(BlockingOutstandingRequest.class);
        Mockito.when(blockingOutstandingRequest.getResult()).thenReturn("done");
        Mockito.when(blockingOutstandingRequest.getRequest()).thenReturn(new DRPCRequest(null, "1"));
        DRPC drpcSpy = Mockito.spy(new DRPC(mockMetricRegistry, mockAuthOk, 1000));
        Mockito.doReturn(blockingOutstandingRequest).when(drpcSpy).execute(any(), any(), any());
        String result = drpcSpy.executeBlocking("", null);
        Assert.assertEquals("done", result);
    }

    /** Test executeBlocking method with functionName = null, funcArgs = "" and state authorized. Expected = throws IllegalArgumentException */
    @Test
    public void executeBlockingNullFunctionNameEmptyFuncArgsAuthThrowsIllegalArgumentException(){

        Assert.assertThrows(IllegalArgumentException.class, () -> drpcAuthOk.executeBlocking(null, ""));
    }

    /** Test executeBlocking method with functionName = "try", funcArgs = "args" and state authorized. Expected = "done" */
    @Test
    public void executeBlockingValidFunctionNameValidFuncArgsAuthShouldPass() throws DRPCExecutionException, AuthorizationException {

        BlockingOutstandingRequest blockingOutstandingRequest = Mockito.mock(BlockingOutstandingRequest.class);
        Mockito.when(blockingOutstandingRequest.getResult()).thenReturn("done");
        Mockito.when(blockingOutstandingRequest.getRequest()).thenReturn(new DRPCRequest("args", "1"));
        DRPC drpcSpy = Mockito.spy(new DRPC(mockMetricRegistry, mockAuthOk, 1000));
        Mockito.doReturn(blockingOutstandingRequest).when(drpcSpy).execute(any(), any(), any());
        String result = drpcSpy.executeBlocking("try", "args");
        Assert.assertEquals("done", result);
    }

    //NOT AUTH RETURN RESULT TESTS

    /** Test returnResult method with id = null, result = "" and state not authorized. Expected = throws NullPointerException */
    @Test
    public void returnResultNullIdEmptyResultNotAuthThrowsNullPointerException() {

        Assert.assertThrows(NullPointerException.class, () -> drpcAuthKo.returnResult(null, ""));
    }

    /** Test returnResult method with id = "", result = null and state not authorized. Expected = throws AuthorizationException */
    @SuppressWarnings("unchecked")
    @Test
    public void returnResultEmptyIdNullResultNotAuthThrowsAuthorizationException() {      // USED REFLECTION OTHERWISE I WOULD HAVE TO HANDLE IAuthorizer IN SOME WAY TO ALLOW EXECUTION BUT THEN NOT RETURN RESULT

        Assert.assertThrows(AuthorizationException.class, () -> {
            Field field = DRPC.class.getDeclaredField("requests");
            field.setAccessible(true);
            ConcurrentHashMap<String, OutstandingRequest> requests = (ConcurrentHashMap<String, OutstandingRequest>) field.get(drpcAuthKo);
            requests.put("", Mockito.mock(OutstandingRequest.class));
            drpcAuthKo.returnResult("", null);
        });
    }

    /** Test returnResult method with id = "1" (existing), result = "done" and state not authorized. Expected = throws AuthorizationException */
    @SuppressWarnings("unchecked")
    @Test
    public void returnResultCorrectIdValidResultNotAuthThrowsAuthorizationException() {      // USED REFLECTION OTHERWISE I WOULD HAVE TO HANDLE IAuthorizer IN SOME WAY TO ALLOW EXECUTION BUT THEN NOT RETURN RESULT

        Assert.assertThrows(AuthorizationException.class, () -> {
            Field field = DRPC.class.getDeclaredField("requests");
            field.setAccessible(true);
            ConcurrentHashMap<String, OutstandingRequest> requests = (ConcurrentHashMap<String, OutstandingRequest>) field.get(drpcAuthKo);
            requests.put("1", Mockito.mock(OutstandingRequest.class));
            drpcAuthKo.returnResult("1", "done");
        });
    }

    //AUTH RETURN RESULT TESTS

    /** Test returnResult method with id = null, result = "" and state authorized. Expected = throws NullPointerException */
    @Test
    public void returnResultNullIdEmptyResultAuthThrowsNullPointerException() {

        Assert.assertThrows(NullPointerException.class, () -> drpcAuthOk.returnResult(null, ""));
    }

    /** Test returnResult method with id = "", result = null and state authorized. Expected = throws DRPCExecutionException */
    @SuppressWarnings("unchecked")
    @Test
    public void returnResultEmptyIdNullResultAuthThrowsDrpcExecutionException() {  // SEMAPHORE TIMEOUT, RETURN RESULTS CANNOT FIND THE CORRECT ID BECAUSE EXECUTE SETS 1! WE ARE LOOKING FOR ""

        Assert.assertThrows(DRPCExecutionException.class, () -> {
            RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
            DRPCRequest expectedRequest = new DRPCRequest("args", "");
            BlockingOutstandingRequest outstandingRequest = new BlockingOutstandingRequest("try", expectedRequest);
            Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
            drpcAuthOk.execute("try", "args", factory);
            drpcAuthOk.returnResult("", null);
            outstandingRequest.getResult();
        });
    }

    /** Test returnResult method with id = "1" (existing), result = "done" and state authorized. Expected = "done" */
    @SuppressWarnings("unchecked")
    @Test
    public void returnResultCorrectIdValidResultAuthShouldPass() throws AuthorizationException, DRPCExecutionException {

        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        DRPCRequest expectedRequest = new DRPCRequest("args", "1");
        BlockingOutstandingRequest outstandingRequest = new BlockingOutstandingRequest("try", expectedRequest);
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
        drpcAuthOk.execute("try", "args", factory);
        drpcAuthOk.returnResult("1", "done");
        outstandingRequest.getResult();
        Assert.assertEquals("done", outstandingRequest.getResult());
    }

    //NOT AUTH FAIL REQUEST TESTS

    /** Test failRequest method with id = null, not valid DRPCExecutionException (without message) and state not authorized. Expected = throws NullPointerException */
    @Test
    public void failRequestNullIdNotValidExceptionNotAuthThrowsNullPointerException() {

        Assert.assertThrows(NullPointerException.class, () -> drpcAuthKo.failRequest(null, new DRPCExecutionException()));
    }

    /** Test failRequest method with id = "", DRPCExecutionException = null and state not authorized. Expected = throws AuthorizationException */
    @SuppressWarnings("unchecked")
    @Test
    public void failRequestEmptyIdNullExceptionNotAuthThrowsAuthorizationException(){      // USED REFLECTION OTHERWISE I WOULD HAVE TO HANDLE IAuthorizer IN SOME WAY TO ALLOW EXECUTION BUT THEN NOT RETURN RESULT

        Assert.assertThrows(AuthorizationException.class, () -> {
            Field field = DRPC.class.getDeclaredField("requests");
            field.setAccessible(true);
            ConcurrentHashMap<String, OutstandingRequest> requests = (ConcurrentHashMap<String, OutstandingRequest>) field.get(drpcAuthKo);
            requests.put("", Mockito.mock(OutstandingRequest.class));
            drpcAuthKo.failRequest("", null);
        });
    }

    /** Test failRequest method with id = "1", valid DRPCExecutionException (with message) and state not authorized. Expected = throws AuthorizationException*/
    @SuppressWarnings("unchecked")
    @Test
    public void failRequestCorrectIdValidExceptionNotAuthThrowsAuthorizationException(){     // USED REFLECTION OTHERWISE I WOULD HAVE TO HANDLE IAuthorizer IN SOME WAY TO ALLOW EXECUTION BUT THEN NOT RETURN RESULT

        Assert.assertThrows(AuthorizationException.class, () -> {
            Field field = DRPC.class.getDeclaredField("requests");
            field.setAccessible(true);
            ConcurrentHashMap<String, OutstandingRequest> requests = (ConcurrentHashMap<String, OutstandingRequest>) field.get(drpcAuthKo);
            requests.put("1", Mockito.mock(OutstandingRequest.class));
            drpcAuthKo.failRequest("1", new DRPCExecutionException("msg"));
        });
    }

    //AUTH FAIL REQUEST TESTS

    /** Test failRequest method with id = null, not valid DRPCExecutionException (without message) and state authorized. Expected = throws NullPointerException */
    @Test
    public void failRequestNullIdNotValidExceptionAuthThrowsNullPointerException(){

        Assert.assertThrows(NullPointerException.class, () -> drpcAuthOk.failRequest(null, new DRPCExecutionException()));
    }

    /** Test failRequest method with id = "", DRPCExecutionException = null and state authorized. Expected = throws DRPCExecutionException */
    @SuppressWarnings("unchecked")
    @Test
    public void failRequestEmptyIdNullExceptionAuthThrowsDrpcExecutionException(){      // SEMAPHORE TIMEOUT, RETURN RESULTS CANNOT FIND THE CORRECT ID BECAUSE EXECUTE SETS 1! WE ARE LOOKING FOR ""

        Assert.assertThrows(DRPCExecutionException.class, () -> {
            RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
            DRPCRequest expectedRequest = new DRPCRequest("args", "");
            BlockingOutstandingRequest outstandingRequest = new BlockingOutstandingRequest("try", expectedRequest);
            Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
            drpcAuthOk.execute("try", "args", factory);
            drpcAuthOk.failRequest("", null);
            outstandingRequest.getResult();
        });
    }

    /** Test failRequest method with id = "1", valid DRPCExecutionException (with message) and state authorized. Expected = DRPCExecutionException with message = "msg" */
    @SuppressWarnings("unchecked")
    @Test
    public void failRequestCorrectIdValidExceptionAuthShouldPass() throws AuthorizationException, NoSuchFieldException, IllegalAccessException {    //USED REFLECTION TO GET THE EXCEPTION

        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        DRPCRequest expectedRequest = new DRPCRequest("args", "1");
        BlockingOutstandingRequest outstandingRequest = new BlockingOutstandingRequest("try", expectedRequest);
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
        drpcAuthOk.execute("try", "args", factory);
        DRPCExecutionException drpcExecutionException = new DRPCExecutionException("msg");
        drpcAuthOk.failRequest("1", drpcExecutionException);
        Field field = BlockingOutstandingRequest.class.getDeclaredField("drpcExecutionException");
        field.setAccessible(true);
        DRPCExecutionException drpcExecutionExceptionActual = (DRPCExecutionException) field.get(outstandingRequest);
        Assert.assertEquals(drpcExecutionException, drpcExecutionExceptionActual);
    }

    //CHECK AUTHORIZATION TESTS

    /** Test checkAuthorization method with reqContext = null, IAuthorizer = valid ok (always permits) , operation = null, function = null. Expected = false */
    @Test
    public void checkAuthorizationNullContextValidAuthNullOperationNullFunctionShouldPass() throws AuthorizationException {

        boolean throwed;
        try{
            throwed = false;
            DRPC.checkAuthorization(null, mockAuthOk, null, null);
        }catch (NullPointerException e){
            throwed = true;
        }
        Assert.assertFalse(throwed);
    }

    /** Test checkAuthorization method with reqContext = null, IAuthorizer = valid ko (never permits) , operation = "", function = "". Expected = throws NullPointerException */
    @Test
    public void checkAuthorizationNullContextValidAuth2EmptyOperationEmptyFunctionThrowsNullPointerException() {

        Assert.assertThrows(NullPointerException.class, () -> DRPC.checkAuthorization(null, mockAuthKo, "", ""));
    }

    /** Test checkAuthorization method with valid reqContext, IAuthorizer = valid ko (never permits) , operation = "execute", function = "try". Expected = throws AuthorizationException */
    @Test
    public void checkAuthorizationValidContextValidAuth2ValidOperationValidFunctionThrowsAuthorizationException() {

        Assert.assertThrows(AuthorizationException.class, () -> DRPC.checkAuthorization(ReqContext.context(), mockAuthKo, "execute", "try"));
    }

    /** Test checkAuthorization method with not valid reqContext, IAuthorizer = null , operation = "execute", function = "try". Expected = false */
    @Test
    public void checkAuthorizationNotValidContextNullAuthValidOperationValidFunctionShouldPass() {

        boolean throwed;
        try(MockedStatic<ReqContext> mockReqContext = Mockito.mockStatic(ReqContext.class)){   //EMULATE NO THREAD LOCAL
            throwed = false;
            mockReqContext.when(ReqContext::context).thenReturn(null);
            DRPC.checkAuthorization(ReqContext.context(), null, "execute", "try");

        }catch (AuthorizationException e){
            throwed = true;
        }
        Assert.assertFalse(throwed);
    }

    /** Test checkAuthorization method with valid reqContext, not valid IAuthorizer (permit throws exception), operation = "execute", function = "try". Expected = false */
    @Test
    public void checkAuthorizationValidContextNotValidAuthValidOperationValidFunctionShouldPass() {

        boolean throwed = false;
        try{
            DRPC.checkAuthorization(ReqContext.context(), new ThrowExceptionIAuthorizer(), "execute", "try");
        }catch (UnsupportedOperationException e){
            //DO NOTHING
        }catch (AuthorizationException e){
            throwed = true;
        }
        Assert.assertFalse(throwed);
    }

    // TIMER TESTS

    /** Test fetchRequest method with functionName = "try" and state authorized + timeout. Expected = DRPCRequest with func_args = "" and request_id = "" */
    @SuppressWarnings("unchecked")
    @Test
    public void fetchRequestCorrectStringAuthTimerTimeoutShouldPass() throws AuthorizationException, InterruptedException {

        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        DRPCRequest request = new DRPCRequest("args", "id");
        OutstandingRequest outstandingRequest = new DoNothingOutstandingRequest("try", request);
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
        drpcAuthOk.execute("try", "args", factory);
        Thread.sleep(2000);
        DRPCRequest actualRequest = drpcAuthOk.fetchRequest("try");
        Assert.assertEquals(new DRPCRequest("",""), actualRequest);
    }

    /** Test returnResult method with id = "1", result = "done"  and state authorized + timeout. Expected = throws DRPCExecutionException */
    @SuppressWarnings("unchecked")
    @Test
    public void returnResultCorrectStringAuthTimerTimeoutThrowsDrpcExecutionException() {    //SEMAPHORE NOT UNLOCKED

        Assert.assertThrows(DRPCExecutionException.class, () -> {
            RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
            DRPCRequest expectedRequest = new DRPCRequest("args", "1");
            BlockingOutstandingRequest outstandingRequest = new BlockingOutstandingRequest("try", expectedRequest);
            Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
            drpcAuthOk.execute("try", "args", factory);
            Thread.sleep(2000);
            drpcAuthOk.returnResult("1", "done");
            outstandingRequest.getResult();
        });
    }

    /** Test failRequest method with id = "1", valid DRPCExecutionException (with message)  and state authorized + timeout. Expected = not equal to DRPCExecutionException with message = "msg" */
    @SuppressWarnings("unchecked")
    @Test
    public void failRequestCorrectStringAuthTimerTimeoutShouldPass() throws AuthorizationException, InterruptedException, NoSuchFieldException, IllegalAccessException {   //USED REFLECTION TO GET EXCEPTION

        RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
        DRPCRequest expectedRequest = new DRPCRequest("args", "1");
        BlockingOutstandingRequest outstandingRequest = new BlockingOutstandingRequest("try", expectedRequest);
        Mockito.when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
        drpcAuthOk.execute("try", "args", factory);
        Thread.sleep(2000);  //ENTRY REMOVED BY TIMER
        DRPCExecutionException drpcExecutionException = new DRPCExecutionException("msg");
        drpcAuthOk.failRequest("1", drpcExecutionException);
        Field field = BlockingOutstandingRequest.class.getDeclaredField("drpcExecutionException");
        field.setAccessible(true);
        DRPCExecutionException drpcExecutionExceptionActual = (DRPCExecutionException) field.get(outstandingRequest);
        Assert.assertNotEquals(drpcExecutionException, drpcExecutionExceptionActual);
    }

    // INVALID INSTANCE TESTS

     /** Test fetchRequest method with functionName = "try" and state not valid. Expected = throws NullPointerException */
    @Test
    public void fetchRequestValidFunctionNameInvalidStateThrowsNullPointerException() {

        Assert.assertThrows(NullPointerException.class, () -> drpcNotValid.fetchRequest("try"));
    }

    /** Test execute method with functionName = "try", funcArgs = "args", valid factory and state not valid. Expected = throws NullPointerException */
    @SuppressWarnings("unchecked")
    @Test
    public void executeValidFunctionNameValidFuncArgsValidFactoryInvalidStateThrowsNullPointerException() {

        Assert.assertThrows(NullPointerException.class, () -> {
            RequestFactory<OutstandingRequest> factory = Mockito.mock(RequestFactory.class);
            DRPCRequest expectedRequest = new DRPCRequest("args", "1");
            OutstandingRequest outstandingRequest = new DoNothingOutstandingRequest("try", expectedRequest);
            Mockito.lenient().when(factory.mkRequest(anyString(), any(DRPCRequest.class))).thenReturn(outstandingRequest);
            drpcNotValid.execute("try", "args", factory);
        });
    }

    /** Test executeBlocking method with functionName = "try", funcArgs = "args" and state not valid. Expected = throws NullPointerException */
    @Test
    public void executeBlockingValidFunctionNameValidFuncArgsInvalidStateThrowsNullPointerException() {

        Assert.assertThrows(NullPointerException.class, () -> drpcNotValid.executeBlocking("try", "args"));
    }

    /** Test returnResult method with id = "1", result = "done" and state not valid. Expected = throws NullPointerException */
    @Test
    public void returnResultValidIdValidResultInvalidStateThrowsNullPointerException() {

        Assert.assertThrows(NullPointerException.class, () -> drpcNotValid.returnResult("1","done"));
    }

    /** Test failRequest method with id = "1", valid DRPCExecutionException (with message) and state not valid. Expected = throws NullPointerException */
    @Test
    public void failRequestValidIdValidDrpcExecutionExceptionInvalidInstanceThrowsNullPointerException() {

        Assert.assertThrows(NullPointerException.class, () -> drpcNotValid.failRequest("1",new DRPCExecutionException("msg")));
    }
}
