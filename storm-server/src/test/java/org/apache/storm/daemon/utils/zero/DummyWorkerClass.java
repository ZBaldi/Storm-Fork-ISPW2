package org.apache.storm.daemon.utils.zero;

import org.apache.storm.daemon.drpc.refactored.zero.DRPCThrift;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.generated.DRPCRequest;

public class DummyWorkerClass {

    private final DRPCThrift drpcThrift;

    public DummyWorkerClass(DRPCThrift drpcThrift) {

        this.drpcThrift = drpcThrift;
    }

    public void executeFunctionAndReturnResult(String functionName) throws AuthorizationException {

        DRPCRequest drpcRequest = fetch(functionName);
        drpcThrift.result(drpcRequest.get_request_id(), "done");
    }

    public void executeFunctionAndFailRequest(String functionName) throws AuthorizationException {

        DRPCRequest drpcRequest = fetch(functionName);
        drpcThrift.failRequestV2(drpcRequest.get_request_id(), new DRPCExecutionException("failed"));
    }

    private DRPCRequest fetch(String functionName) throws AuthorizationException {
        return drpcThrift.fetchRequest(functionName);
    }
}
