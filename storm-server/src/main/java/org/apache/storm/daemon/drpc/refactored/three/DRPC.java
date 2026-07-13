/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package org.apache.storm.daemon.drpc.refactored.three;

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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.storm.DaemonConfig;
import org.apache.storm.daemon.drpc.BlockingOutstandingRequest;
import org.apache.storm.daemon.drpc.OutstandingRequest;
import org.apache.storm.daemon.drpc.RequestFactory;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.generated.DRPCRequest;
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
 * Maintainability-oriented alternative to DRPC. Public behavior and integration
 * contracts are intentionally kept equivalent to the original implementation.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class DRPC implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(DRPC.class);
    private static final DRPCRequest NOTHING_REQUEST = new DRPCRequest("", "");
    private static final DRPCExecutionException TIMED_OUT =
        new WrappedDRPCExecutionException("Timed Out");
    private static final DRPCExecutionException SHUT_DOWN =
        new WrappedDRPCExecutionException("Server Shutting Down");
    private static final DRPCExecutionException DEFAULT_FAILED =
        new WrappedDRPCExecutionException("Request failed");

    private final Meter timedOutMeter;
    private final Meter executeMeter;
    private final Meter failMeter;
    private final Meter fetchMeter;
    private final IAuthorizer authorizer;
    private final long timeoutMs;
    private final AtomicLong requestCounter = new AtomicLong(0L);

    /* Kept as ConcurrentHashMap for binary/reflection compatibility with existing tests. */
    private final ConcurrentHashMap<String, OutstandingRequest> requests =
        new ConcurrentHashMap<>();
    private final Map<String, Queue<OutstandingRequest>> requestQueues =
        new ConcurrentHashMap<>();
    private final Timer cleanupTimer = new Timer("drpc-request-cleanup", true);

    public DRPC(final StormMetricsRegistry metricsRegistry, final Map<String, Object> conf) {
        this(metricsRegistry, loadAuthorizer(conf),
            ObjectReader.getLong(conf.get(DaemonConfig.DRPC_REQUEST_TIMEOUT_SECS), 600L) * 1000L);
    }

    @VisibleForTesting
    public DRPC(final StormMetricsRegistry metricsRegistry, final IAuthorizer auth,
                  final long requestTimeoutMs) {
        authorizer = auth;
        timeoutMs = requestTimeoutMs;
        timedOutMeter = metricsRegistry.registerMeter("drpc:num-server-timed-out-requests");
        executeMeter = metricsRegistry.registerMeter("drpc:num-execute-calls");
        failMeter = metricsRegistry.registerMeter("drpc:num-failRequest-calls");
        fetchMeter = metricsRegistry.registerMeter("drpc:num-fetchRequest-calls");
        scheduleCleanup();
    }

    private static IAuthorizer loadAuthorizer(final Map<String, Object> conf) {
        final Object configuredClass = conf.get(DaemonConfig.DRPC_AUTHORIZER);
        if (configuredClass == null) {
            return null;
        }
        final String className = String.valueOf(configuredClass);
        try {
            return (IAuthorizer) Class.forName(className).getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException | ClassCastException exception) {
            throw new IllegalStateException("Unable to instantiate DRPC authorizer " + className,
                exception);
        }
    }

    private void scheduleCleanup() {
        final long checkPeriodMs = Math.max(1L, Math.min(1000L, Math.max(1L, timeoutMs)));
        cleanupTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                clearTimedOutRequests();
            }
        }, checkPeriodMs, checkPeriodMs);
    }

    public static void checkAuthorization(final ReqContext requestContext,
                                          final IAuthorizer auth,
                                          final String operation,
                                          final String function)
        throws AuthorizationException {
        checkAuthorization(requestContext, auth, operation, function, LOG);
    }

    static void checkAuthorization(final ReqContext requestContext,
                                   final IAuthorizer auth,
                                   final String operation,
                                   final String function,
                                   final Logger logger)
        throws AuthorizationException {
        if (auth == null) {
            return;
        }
        final Map<String, Object> authorizationContext = new HashMap<>();
        authorizationContext.put(DRPCAuthorizerBase.FUNCTION_NAME, function);
        if (!auth.permit(requestContext, operation, authorizationContext)) {
            final Principal principal = requestContext.principal();
            final String user = principal == null ? "unknown" : principal.getName();
            logger.warn("DRPC authorization failed for user {} operation {} function {}",
                user, operation, function);
            throw new WrappedAuthorizationException("DRPC request is not authorized");
        }
    }

    private void clearTimedOutRequests() {
        for (final Entry<String, OutstandingRequest> requestEntry : requests.entrySet()) {
            final OutstandingRequest request = requestEntry.getValue();
            if (request.isTimedOut(timeoutMs) && requests.remove(requestEntry.getKey(), request)) {
                removeFromQueue(request);
                request.fail(TIMED_OUT);
                timedOutMeter.mark();
            }
        }
    }

    private void removeFromQueue(final OutstandingRequest request) {
        final Queue<OutstandingRequest> queue = requestQueues.get(request.getFunction());
        if (queue != null) {
            queue.remove(request);
        }
    }

    public DRPCRequest fetchRequest(final String functionName) throws AuthorizationException {
        fetchMeter.mark();
        checkAuthorization(ReqContext.context(), authorizer, "fetchRequest", functionName);
        if (functionName == null) {
            throw new IllegalArgumentException("functionName cannot be null");
        }
        final Queue<OutstandingRequest> queue = requestQueues.computeIfAbsent(
            functionName, ignored -> new ConcurrentLinkedQueue<>());
        final OutstandingRequest request = queue.poll();
        if (request == null) {
            return NOTHING_REQUEST;
        }
        request.fetched();
        return request.getRequest();
    }

    public void returnResult(final String requestId, final String result)
        throws AuthorizationException {
        final OutstandingRequest request = requests.get(requestId);
        if (request != null) {
            checkAuthorization(ReqContext.context(), authorizer, "result", request.getFunction());
            if (requests.remove(requestId, request)) {
                removeFromQueue(request);
                request.returnResult(result);
            }
        }
    }

    public void failRequest(final String requestId, final DRPCExecutionException exception)
        throws AuthorizationException {
        failMeter.mark();
        final OutstandingRequest request = requests.get(requestId);
        if (request != null) {
            checkAuthorization(ReqContext.context(), authorizer, "failRequest", request.getFunction());
            if (requests.remove(requestId, request)) {
                removeFromQueue(request);
                request.fail(exception == null ? DEFAULT_FAILED : exception);
            }
        }
    }

    public <T extends OutstandingRequest> T execute(final String functionName,
                                                     final String functionArguments,
                                                     final RequestFactory<T> factory)
        throws AuthorizationException {
        executeMeter.mark();
        checkAuthorization(ReqContext.context(), authorizer, "execute", functionName);
        final String requestId = Long.toString(requestCounter.incrementAndGet());
        final DRPCRequest drpcRequest = new DRPCRequest(functionArguments, requestId);
        final T outstandingRequest = factory.mkRequest(functionName, drpcRequest);
        requests.put(requestId, outstandingRequest);
        requestQueues.computeIfAbsent(functionName, ignored -> new ConcurrentLinkedQueue<>())
            .add(outstandingRequest);
        return outstandingRequest;
    }

    public String executeBlocking(final String functionName, final String functionArguments)
        throws AuthorizationException, DRPCExecutionException {
        return execute(functionName, functionArguments, BlockingOutstandingRequest.FACTORY)
            .getResult();
    }

    @Override
    public void close() {
        cleanupTimer.cancel();
        for (final Entry<String, OutstandingRequest> requestEntry : requests.entrySet()) {
            final OutstandingRequest request = requestEntry.getValue();
            if (requests.remove(requestEntry.getKey(), request)) {
                removeFromQueue(request);
                request.fail(SHUT_DOWN);
            }
        }
        requestQueues.clear();
    }
}
