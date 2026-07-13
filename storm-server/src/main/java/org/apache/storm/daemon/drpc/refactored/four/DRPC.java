/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.daemon.drpc.refactored.four;

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
import org.apache.storm.daemon.drpc.BlockingOutstandingRequest;
import org.apache.storm.daemon.drpc.OutstandingRequest;
import org.apache.storm.daemon.drpc.RequestFactory;
import org.apache.storm.generated.AuthorizationException;
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

/**
 * Maintainable, plug-compatible implementation of the DRPC request coordinator.
 * The public contract and the observable request lifecycle intentionally match DRPC.
 */
@SuppressWarnings({"checkstyle:AbbreviationAsWordInName", "PMD.ShortClassName", "PMD.TooManyMethods"})
public class DRPC implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(DRPC.class);
    private static final DRPCRequest NOTHING_REQUEST = new DRPCRequest("", "");
    private static final DRPCExecutionException TIMED_OUT =
        new WrappedDRPCExecutionException("Timed Out");
    private static final DRPCExecutionException SHUT_DOWN =
        new WrappedDRPCExecutionException("Server Shutting Down");
    private static final DRPCExecutionException DEFAULT_FAILED =
        new WrappedDRPCExecutionException("Request failed");

    private final Meter serverTimeoutMeter;
    private final Meter executeCallsMeter;
    private final Meter resultCallsMeter;
    private final Meter failedRequestCallsMeter;
    private final Meter fetchedRequestCallsMeter;
    private final IAuthorizer authorizer;
    private final AtomicLong nextRequestId = new AtomicLong(0L);
    private final ConcurrentMap<String, OutstandingRequest> requests = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Queue<OutstandingRequest>> requestQueues =
        new ConcurrentHashMap<>();
    private final Timer cleanupTimer = new Timer("drpc-request-cleanup", true);

    public DRPC(final StormMetricsRegistry metricsRegistry, final Map<String, Object> configuration) {
        this(metricsRegistry, createAuthorizer(configuration),
            1_000L * ObjectReader.getInt(configuration.get(DaemonConfig.DRPC_REQUEST_TIMEOUT_SECS)));
    }

    @VisibleForTesting
    public DRPC(final StormMetricsRegistry metricsRegistry, final IAuthorizer auth,
                final long timeoutMilliseconds) {
        authorizer = auth;
        serverTimeoutMeter = metricsRegistry.registerMeter("drpc:num-server-timed-out-requests");
        executeCallsMeter = metricsRegistry.registerMeter("drpc:num-execute-calls");
        resultCallsMeter = metricsRegistry.registerMeter("drpc:num-result-calls");
        failedRequestCallsMeter = metricsRegistry.registerMeter("drpc:num-failRequest-calls");
        fetchedRequestCallsMeter = metricsRegistry.registerMeter("drpc:num-fetchRequest-calls");
        scheduleCleanup(timeoutMilliseconds);
    }

    private static IAuthorizer createAuthorizer(final Map<String, Object> configuration) {
        final String className = (String) configuration.get(DaemonConfig.DRPC_AUTHORIZER);
        if (className == null) {
            return null;
        }
        try {
            return (IAuthorizer) Class.forName(className).getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException reflectiveFailure) {
            throw new IllegalStateException("Unable to instantiate DRPC authorizer " + className,
                reflectiveFailure);
        }
    }

    private void scheduleCleanup(final long timeoutMilliseconds) {
        cleanupTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                expireRequests();
            }
        }, timeoutMilliseconds, timeoutMilliseconds);
    }

    private void expireRequests() {
        for (final Entry<String, OutstandingRequest> entry : requests.entrySet()) {
            final OutstandingRequest pendingRequest = entry.getValue();
            if (requests.remove(entry.getKey(), pendingRequest)) {
                removeFromFunctionQueue(pendingRequest);
                pendingRequest.fail(TIMED_OUT);
                serverTimeoutMeter.mark();
            }
        }
    }

    private void removeFromFunctionQueue(final OutstandingRequest request) {
        final Queue<OutstandingRequest> functionQueue = requestQueues.get(request.getFunction());
        if (functionQueue != null) {
            functionQueue.remove(request);
        }
    }

    public <T extends OutstandingRequest> T execute(final String functionName,
                                                     final String functionArguments,
                                                     final RequestFactory<T> factory)
        throws AuthorizationException {
        checkAuthorization(ReqContext.context(), authorizer, "execute", functionName);
        executeCallsMeter.mark();
        final String requestId = Long.toString(nextRequestId.incrementAndGet());
        final DRPCRequest request = new DRPCRequest(functionArguments, requestId);
        final T outstandingRequest = factory.mkRequest(functionName, request);
        requests.put(requestId, outstandingRequest);
        requestQueues.computeIfAbsent(functionName, ignored -> new ConcurrentLinkedQueue<>())
            .add(outstandingRequest);
        return outstandingRequest;
    }

    public String executeBlocking(final String functionName, final String functionArguments)
        throws AuthorizationException, DRPCExecutionException {
        final BlockingOutstandingRequest outstandingRequest =
            execute(functionName, functionArguments, BlockingOutstandingRequest.FACTORY);
        try {
            return outstandingRequest.getResult();
        } finally {
            requests.remove(outstandingRequest.getRequest().get_request_id());
            removeFromFunctionQueue(outstandingRequest);
        }
    }

    public DRPCRequest fetchRequest(final String functionName) throws AuthorizationException {
        checkAuthorization(ReqContext.context(), authorizer, "fetchRequest", functionName);
        fetchedRequestCallsMeter.mark();
        final Queue<OutstandingRequest> functionQueue = requestQueues.get(functionName);
        final OutstandingRequest outstandingRequest = functionQueue == null ? null : functionQueue.poll();
        if (outstandingRequest == null) {
            return NOTHING_REQUEST;
        }
        outstandingRequest.fetched();
        return outstandingRequest.getRequest();
    }

    public void returnResult(final String requestId, final String result)
        throws AuthorizationException {
        final OutstandingRequest outstandingRequest = requests.get(requestId);
        if (outstandingRequest != null) {
            checkAuthorization(ReqContext.context(), authorizer, "returnResult",
                outstandingRequest.getFunction());
            resultCallsMeter.mark();
            if (requests.remove(requestId, outstandingRequest)) {
                outstandingRequest.returnResult(result);
            }
        }
    }

    public void failRequest(final String requestId, final DRPCExecutionException failure)
        throws AuthorizationException {
        final OutstandingRequest outstandingRequest = requests.get(requestId);
        if (outstandingRequest != null) {
            checkAuthorization(ReqContext.context(), authorizer, "failRequest",
                outstandingRequest.getFunction());
            failedRequestCallsMeter.mark();
            if (requests.remove(requestId, outstandingRequest)) {
                outstandingRequest.fail(failure == null ? DEFAULT_FAILED : failure);
            }
        }
    }

    public static void checkAuthorization(final ReqContext requestContext, final IAuthorizer auth,
                                          final String operation, final String function)
        throws AuthorizationException {
        if (auth == null) {
            return;
        }
        final Map<String, Object> authorizationParameters = new HashMap<>();
        authorizationParameters.put(DRPCAuthorizerBase.FUNCTION_NAME, function);
        final Principal principal = requestContext == null ? null : requestContext.principal();
        final boolean permitted = auth.permit(requestContext, operation, authorizationParameters);
        if (requestContext != null) {
            ThriftAccessLogger.logAccessFunction(
                requestContext.requestID(), requestContext.remoteAddress(), principal, operation, function);
        }
        if (!permitted) {
            final String deniedPrincipal = requestContext.principal() == null
                ? "unknown" : requestContext.principal().getName();
            throw new WrappedAuthorizationException(
                "DRPC request denied for principal '" + deniedPrincipal + "', operation '"
                    + operation + "' and function '" + function + "'");
        }
    }

    @Override
    public void close() {
        cleanupTimer.cancel();
        for (final OutstandingRequest pendingRequest : requests.values()) {
            if (requests.remove(pendingRequest.getRequest().get_request_id(), pendingRequest)) {
                pendingRequest.fail(SHUT_DOWN);
            }
        }
        requestQueues.clear();
    }
}
