package org.apache.storm.daemon.drpc;

import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.security.auth.ReqContext;

import java.util.Map;

public class KoDummyIAuthorizer implements IAuthorizer {

    public KoDummyIAuthorizer() {
        super();
    }

    @Override
    public void prepare(Map<String, Object> conf) {}

    @Override
    public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
        return false;
    }
}
