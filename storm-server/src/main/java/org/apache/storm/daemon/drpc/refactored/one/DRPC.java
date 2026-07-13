/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.daemon.drpc.refactored.one;

import com.codahale.metrics.Meter;
import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.storm.DaemonConfig;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.daemon.drpc.BlockingOutstandingRequest;
import org.apache.storm.daemon.drpc.OutstandingRequest;
import org.apache.storm.daemon.drpc.RequestFactory;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.DRPCExceptionType;
import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.generated.DRPCRequest;
import org.apache.storm.logging.ThriftAccessLogger;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.security.auth.authorizer.DRPCAuthorizerBase;
import org.apache.storm.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.WrappedAuthorizationException;
import org.apache.storm.utils.WrappedDRPCExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class DRPC implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(DRPC.class);
    private static final DRPCRequest NOTHING_REQUEST = new DRPCRequest("", "");
    private static final DRPCExecutionException TIMED_OUT = new WrappedDRPCExecutionException("Timed Out");
    private static final DRPCExecutionException SHUT_DOWN = new WrappedDRPCExecutionException("Server Shutting Down");
    private static final DRPCExecutionException DEFAULT_FAILED = new WrappedDRPCExecutionException("Request failed");

    private final Meter timedOutMeter;
    private final Meter executeMeter;
    private final Meter resultMeter;
    private final Meter failureMeter;
    private final Meter fetchMeter;
    private final Timer timer = new Timer("DRPC-CLEANUP-TIMER", true);
    private final IAuthorizer auth;
    private final RequestStore requestStore = new RequestStore();

    static {
        TIMED_OUT.set_type(DRPCExceptionType.SERVER_TIMEOUT);
        SHUT_DOWN.set_type(DRPCExceptionType.SERVER_SHUTDOWN);
        DEFAULT_FAILED.set_type(DRPCExceptionType.FAILED_REQUEST);
    }

    public DRPC(final StormMetricsRegistry metricsRegistry, final Map<String, Object> conf) {
        this(metricsRegistry,
             AuthorizationSupport.createHandler((String) conf.get(DaemonConfig.DRPC_AUTHORIZER), conf),
             ObjectReader.getInt(conf.get(DaemonConfig.DRPC_REQUEST_TIMEOUT_SECS), 600) * 1000);
    }

    public DRPC(final StormMetricsRegistry metricsRegistry, final IAuthorizer auth, final long timeoutMs) {
        this.auth = auth;
        this.timedOutMeter = metricsRegistry.registerMeter("drpc:num-server-timedout-requests");
        this.executeMeter = metricsRegistry.registerMeter("drpc:num-execute-calls");
        this.resultMeter = metricsRegistry.registerMeter("drpc:num-result-calls");
        this.failureMeter = metricsRegistry.registerMeter("drpc:num-failRequest-calls");
        this.fetchMeter = metricsRegistry.registerMeter("drpc:num-fetchRequest-calls");
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                requestStore.cleanupAll(timeoutMs, TIMED_OUT, timedOutMeter);
            }
        }, timeoutMs / 2, timeoutMs / 2);
    }

    @VisibleForTesting
    public static void checkAuthorization(final ReqContext reqContext, final IAuthorizer authorizer,
                                                 final String operation, final String function)
        throws AuthorizationException {
        AuthorizationSupport.check(reqContext, authorizer, operation, function, true);
    }

    public void returnResult(final String requestId, final String result) throws AuthorizationException {
        resultMeter.mark();
        LOG.debug("Got a result {} {}", requestId, result);
        final OutstandingRequest request = requestStore.get(requestId);
        if (request != null) {
            AuthorizationSupport.checkCurrent(auth, "result", request.getFunction(), true);
            request.returnResult(result);
        }
    }

    public DRPCRequest fetchRequest(final String functionName) throws AuthorizationException {
        fetchMeter.mark();
        AuthorizationSupport.checkCurrent(auth, "fetchRequest", functionName, false);
        final OutstandingRequest request = requestStore.fetch(functionName);
        final DRPCRequest result;
        if (request != null) {
            AuthorizationSupport.logAccess("fetchRequest", functionName);
            request.fetched();
            result = request.getRequest();
        } else {
            result = NOTHING_REQUEST;
        }
        return result;
    }

    public void failRequest(final String requestId, final DRPCExecutionException failure) throws AuthorizationException {
        failureMeter.mark();
        LOG.debug("Got a fail {}", requestId);
        final OutstandingRequest request = requestStore.get(requestId);
        if (request != null) {
            AuthorizationSupport.checkCurrent(auth, "failRequest", request.getFunction(), true);
            final DRPCExecutionException effectiveFailure = failure == null ? DEFAULT_FAILED : failure;
            request.fail(effectiveFailure);
        }
    }

    public <T extends OutstandingRequest> T execute(final String functionName, final String functionArguments,
                                                     final RequestFactory<T> factory) throws AuthorizationException {
        executeMeter.mark();
        AuthorizationSupport.checkCurrent(auth, "execute", functionName, true);
        final String requestId = requestStore.nextRequestId();
        LOG.debug("Execute {} {}", functionName, functionArguments);
        final T request = factory.mkRequest(functionName, new DRPCRequest(functionArguments, requestId));
        requestStore.add(requestId, functionName, request);
        return request;
    }

    public String executeBlocking(final String functionName, final String functionArguments)
        throws DRPCExecutionException, AuthorizationException {
        final BlockingOutstandingRequest request =
            execute(functionName, functionArguments, BlockingOutstandingRequest.FACTORY);
        try {
            LOG.debug("Waiting for result {} {}", functionName, functionArguments);
            return request.getResult();
        } finally {
            requestStore.cleanup(request.getRequest().get_request_id());
        }
    }

    @Override
    public void close() {
        timer.cancel();
        requestStore.cleanupAll(0, SHUT_DOWN, timedOutMeter);
    }

    private static final class AuthorizationSupport {
        private AuthorizationSupport() {
        }

        private static IAuthorizer createHandler(final String className, final Map<String, Object> conf) {
            try {
                return StormCommon.mkAuthorizationHandler(className, conf);
            } catch (RuntimeException | IllegalAccessException | InstantiationException | ClassNotFoundException exception) {
                throw new AuthorizationHandlerInitializationException((RuntimeException) exception);
            }
        }

        private static void logAccess(final String operation, final String function) {
            logAccess(ReqContext.context(), operation, function);
        }

        private static void logAccess(final ReqContext reqContext, final String operation, final String function) {
            ThriftAccessLogger.logAccessFunction(reqContext.requestID(), reqContext.remoteAddress(), reqContext.principal(),
                                                 operation, function);
        }

        private static void checkCurrent(final IAuthorizer authorizer, final String operation, final String function,
                                         final boolean log) throws AuthorizationException {
            check(ReqContext.context(), authorizer, operation, function, log);
        }

        private static void check(final ReqContext reqContext, final IAuthorizer authorizer, final String operation,
                                  final String function, final boolean log) throws AuthorizationException {
            if (reqContext != null && log) {
                logAccess(reqContext, operation, function);
            }
            if (authorizer != null) {
                final Map<String, Object> authorizationData = new HashMap<>();
                authorizationData.put(DRPCAuthorizerBase.FUNCTION_NAME, function);
                if (!authorizer.permit(reqContext, operation, authorizationData)) {
                    final Principal principal = reqContext.principal();
                    final String user = principal != null ? principal.getName() : "unknown";
                    throw new WrappedAuthorizationException("DRPC request '" + operation + "' for '" + user
                                                                + "' user is not authorized");
                }
            }
        }
    }

    private static final class RequestStore {
        private final ConcurrentMap<String, Queue<OutstandingRequest>> queues = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, OutstandingRequest> requests = new ConcurrentHashMap<>();
        private final AtomicLong counter = new AtomicLong(0);

        private String nextRequestId() {
            return String.valueOf(counter.incrementAndGet());
        }

        private OutstandingRequest get(final String requestId) {
            return requests.get(requestId);
        }

        private Queue<OutstandingRequest> getQueue(final String function) {
            if (function == null) {
                throw new IllegalArgumentException("The function for a request cannot be null");
            }
            return queues.computeIfAbsent(function, ignored -> new ConcurrentLinkedQueue<>());
        }

        private OutstandingRequest fetch(final String function) {
            return getQueue(function).poll();
        }

        private void add(final String requestId, final String function, final OutstandingRequest request) {
            requests.put(requestId, request);
            getQueue(function).add(request);
        }

        private void cleanup(final String requestId) {
            final OutstandingRequest request = requests.remove(requestId);
            if (request != null && !request.wasFetched()) {
                getQueue(request.getFunction()).remove(request);
            }
        }

        private void cleanupAll(final long timeoutMs, final DRPCExecutionException failure, final Meter timeoutMeter) {
            for (final Entry<String, OutstandingRequest> entry : requests.entrySet()) {
                final OutstandingRequest request = entry.getValue();
                if (request.isTimedOut(timeoutMs)) {
                    request.fail(failure);
                    cleanup(entry.getKey());
                    timeoutMeter.mark();
                }
            }
        }
    }

    private static final class AuthorizationHandlerInitializationException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private AuthorizationHandlerInitializationException(final RuntimeException cause) {
            super(cause);
        }
    }
}
