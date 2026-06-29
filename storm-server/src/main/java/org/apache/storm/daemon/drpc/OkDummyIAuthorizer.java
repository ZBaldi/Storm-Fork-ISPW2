package org.apache.storm.daemon.drpc;

import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.security.auth.ReqContext;

import java.util.Map;

public class OkDummyIAuthorizer implements IAuthorizer {

    public OkDummyIAuthorizer() {
        super();
    }

    @Override
    public void prepare(Map<String, Object> conf) {}

    @Override
    public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
        return true;
    }
}
