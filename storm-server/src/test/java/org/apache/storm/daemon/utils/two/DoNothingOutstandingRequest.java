package org.apache.storm.daemon.utils.two;

import org.apache.storm.daemon.drpc.OutstandingRequest;
import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.generated.DRPCRequest;

public class DoNothingOutstandingRequest extends OutstandingRequest {

    public DoNothingOutstandingRequest(String function, DRPCRequest req) {
        super(function, req);
    }

    @Override
    public void returnResult(String result) {}

    @Override
    public void fail(DRPCExecutionException e) {}
}
