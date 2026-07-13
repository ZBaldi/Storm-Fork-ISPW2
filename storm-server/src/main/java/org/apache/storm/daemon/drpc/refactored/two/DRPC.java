/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package org.apache.storm.daemon.drpc.refactored.two;

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

    private final Meter timeoutMeter;
    private final Meter executeMeter;
    private final Meter resultMeter;
    private final Meter failureMeter;
    private final Meter fetchMeter;

    static {
        TIMED_OUT.set_type(DRPCExceptionType.SERVER_TIMEOUT);
        SHUT_DOWN.set_type(DRPCExceptionType.SERVER_SHUTDOWN);
        DEFAULT_FAILED.set_type(DRPCExceptionType.FAILED_REQUEST);
    }

    // Waiting to be fetched. Concrete concurrent implementations are retained at runtime.
    private final ConcurrentMap<String, Queue<OutstandingRequest>> queues = new ConcurrentHashMap<>();
    // Waiting to be returned. The concrete map is retained for behavioral compatibility.
    private final ConcurrentMap<String, OutstandingRequest> requests = new ConcurrentHashMap<>();
    private final Timer timer = new Timer("DRPC-CLEANUP-TIMER", true);
    private final AtomicLong counter = new AtomicLong(0);
    private final IAuthorizer auth;

    public DRPC(final StormMetricsRegistry metricsRegistry, final Map<String, Object> conf) {
        this(metricsRegistry,
             AuthorizationSupport.makeHandler((String) conf.get(DaemonConfig.DRPC_AUTHORIZER), conf),
             ObjectReader.getInt(conf.get(DaemonConfig.DRPC_REQUEST_TIMEOUT_SECS), 600) * 1000);
    }

    public DRPC(final StormMetricsRegistry metricsRegistry, final IAuthorizer auth, final long timeoutMs) {
        this.auth = auth;
        timeoutMeter = metricsRegistry.registerMeter("drpc:num-server-timedout-requests");
        executeMeter = metricsRegistry.registerMeter("drpc:num-execute-calls");
        resultMeter = metricsRegistry.registerMeter("drpc:num-result-calls");
        failureMeter = metricsRegistry.registerMeter("drpc:num-failRequest-calls");
        fetchMeter = metricsRegistry.registerMeter("drpc:num-fetchRequest-calls");
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                RequestSupport.cleanupAll(requests, queues, timeoutMs, TIMED_OUT, timeoutMeter);
            }
        }, timeoutMs / 2, timeoutMs / 2);
    }

    @VisibleForTesting
    /* default */ public static void checkAuthorization(final ReqContext requestContext, final IAuthorizer authorizer,
                                                        final String operation, final String function)
        throws AuthorizationException {
        AuthorizationSupport.check(requestContext, authorizer, operation, function, true);
    }

    public void returnResult(final String requestId, final String result) throws AuthorizationException {
        resultMeter.mark();
        LOG.debug("Got a result {} {}", requestId, result);
        final OutstandingRequest request = requests.get(requestId);
        if (request != null) {
            AuthorizationSupport.check(ReqContext.context(), auth, "result", request.getFunction(), true);
            request.returnResult(result);
        }
    }

    public DRPCRequest fetchRequest(final String functionName) throws AuthorizationException {
        fetchMeter.mark();
        AuthorizationSupport.check(ReqContext.context(), auth, "fetchRequest", functionName, false);
        final Queue<OutstandingRequest> queue = RequestSupport.getQueue(queues, functionName);
        final OutstandingRequest request = queue.poll();
        DRPCRequest result = NOTHING_REQUEST;
        if (request != null) {
            AuthorizationSupport.logAccess(ReqContext.context(), "fetchRequest", functionName);
            request.fetched();
            result = request.getRequest();
        }
        return result;
    }

    public void failRequest(final String requestId, final DRPCExecutionException exception)
        throws AuthorizationException {
        failureMeter.mark();
        LOG.debug("Got a fail {}", requestId);
        final OutstandingRequest request = requests.get(requestId);
        if (request != null) {
            AuthorizationSupport.check(ReqContext.context(), auth, "failRequest", request.getFunction(), true);
            final DRPCExecutionException failure = exception == null ? DEFAULT_FAILED : exception;
            request.fail(failure);
        }
    }

    public <T extends OutstandingRequest> T execute(final String functionName, final String functionArguments,
                                                     final RequestFactory<T> factory)
        throws AuthorizationException {
        executeMeter.mark();
        AuthorizationSupport.check(ReqContext.context(), auth, "execute", functionName, true);
        final String requestId = String.valueOf(counter.incrementAndGet());
        LOG.debug("Execute {} {}", functionName, functionArguments);
        final T request = factory.mkRequest(functionName, new DRPCRequest(functionArguments, requestId));
        requests.put(requestId, request);
        final Queue<OutstandingRequest> queue = RequestSupport.getQueue(queues, functionName);
        queue.add(request);
        return request;
    }

    public String executeBlocking(final String functionName, final String functionArguments)
        throws DRPCExecutionException, AuthorizationException {
        final BlockingOutstandingRequest request = execute(functionName, functionArguments,
                                                            BlockingOutstandingRequest.FACTORY);
        try {
            LOG.debug("Waiting for result {} {}", functionName, functionArguments);
            return request.getResult();
        } finally {
            RequestSupport.cleanup(requests, queues, request.getRequest().get_request_id());
        }
    }

    @Override
    public void close() {
        timer.cancel();
        RequestSupport.cleanupAll(requests, queues, 0, SHUT_DOWN, timeoutMeter);
    }

    private static final class AuthorizationSupport {
        private AuthorizationSupport() {
        }

        private static IAuthorizer makeHandler(final String className, final Map<String, Object> conf) {
            try {
                return StormCommon.mkAuthorizationHandler(className, conf);
            } catch (ReflectiveOperationException exception) {
                throw new IllegalStateException(exception);
            }
        }

        private static void logAccess(final ReqContext requestContext, final String operation, final String function) {
            ThriftAccessLogger.logAccessFunction(requestContext.requestID(), requestContext.remoteAddress(),
                                                 requestContext.principal(), operation, function);
        }

        private static void check(final ReqContext requestContext, final IAuthorizer authorizer,
                                  final String operation, final String function, final boolean log)
            throws AuthorizationException {
            if (requestContext != null && log) {
                logAccess(requestContext, operation, function);
            }
            if (authorizer != null) {
                final Map<String, Object> parameters = new HashMap<>();
                parameters.put(DRPCAuthorizerBase.FUNCTION_NAME, function);
                if (!authorizer.permit(requestContext, operation, parameters)) {
                    final Principal principal = requestContext.principal();
                    final String user = principal != null ? principal.getName() : "unknown";
                    throw new WrappedAuthorizationException("DRPC request '" + operation + "' for '"
                                                                + user + "' user is not authorized");
                }
            }
        }
    }

    private static final class RequestSupport {
        private RequestSupport() {
        }

        private static Queue<OutstandingRequest> getQueue(
            final ConcurrentMap<String, Queue<OutstandingRequest>> queueMap, final String function) {
            if (function == null) {
                throw new IllegalArgumentException("The function for a request cannot be null");
            }
            queueMap.putIfAbsent(function, new ConcurrentLinkedQueue<>());
            return queueMap.get(function);
        }

        private static void cleanup(final ConcurrentMap<String, OutstandingRequest> requestMap,
                                    final ConcurrentMap<String, Queue<OutstandingRequest>> queueMap,
                                    final String requestId) {
            final OutstandingRequest request = requestMap.remove(requestId);
            if (request != null && !request.wasFetched()) {
                queueMap.get(request.getFunction()).remove(request);
            }
        }

        private static void cleanupAll(final ConcurrentMap<String, OutstandingRequest> requestMap,
                                       final ConcurrentMap<String, Queue<OutstandingRequest>> queueMap,
                                       final long timeoutMs, final DRPCExecutionException exception,
                                       final Meter timeoutMeter) {
            for (final Entry<String, OutstandingRequest> entry : requestMap.entrySet()) {
                final OutstandingRequest request = entry.getValue();
                if (request.isTimedOut(timeoutMs)) {
                    request.fail(exception);
                    cleanup(requestMap, queueMap, entry.getKey());
                    timeoutMeter.mark();
                }
            }
        }
    }
}
