package org.apache.storm.daemon.utils.three;

import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.security.auth.ReqContext;

import java.util.Map;

public class ThrowExceptionIAuthorizer implements IAuthorizer {

    @Override
    public void prepare(Map<String, Object> conf) {

    }

    @Override
    public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
