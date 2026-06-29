package org.apache.storm.daemon.drpc;

import com.codahale.metrics.Meter;
import org.apache.storm.metric.StormMetricsRegistry;

public class DummyStormMetricRegistry extends StormMetricsRegistry {

    public DummyStormMetricRegistry() {
        super();
    }

    @Override
    public Meter registerMeter(String name) {
        return new Meter();
    }
}
