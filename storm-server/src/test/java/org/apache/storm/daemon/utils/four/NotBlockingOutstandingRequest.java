package org.apache.storm.daemon.utils.four;

import org.apache.storm.daemon.drpc.OutstandingRequest;
import org.apache.storm.daemon.drpc.RequestFactory;
import org.apache.storm.generated.DRPCExceptionType;
import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.generated.DRPCRequest;
import org.apache.storm.utils.WrappedDRPCExecutionException;

public class NotBlockingOutstandingRequest extends OutstandingRequest {

    public static final RequestFactory<NotBlockingOutstandingRequest> FACTORY = NotBlockingOutstandingRequest::new;
    private String result = null;
    private DRPCExecutionException drpcExecutionException = null;

    public NotBlockingOutstandingRequest(String function, DRPCRequest req) {
        super(function, req);
    }

    public String getResult() throws DRPCExecutionException {

        if (result != null) {
            return result;
        }

        if (drpcExecutionException == null) {
            drpcExecutionException = new WrappedDRPCExecutionException("Internal Error: No Result and No Exception");
            drpcExecutionException.set_type(DRPCExceptionType.INTERNAL_ERROR);
        }
        throw drpcExecutionException;
    }

    @Override
    public void returnResult(String result) {
        this.result = result;
    }

    @Override
    public void fail(DRPCExecutionException e) {
        drpcExecutionException = e;
    }
}
