package com.hazelcast.jet.contrib.influxdb.publisher;

import com.hazelcast.internal.metrics.MetricTarget;
import com.hazelcast.internal.metrics.MetricsPublisher;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.MetricsUtil;
import com.hazelcast.internal.metrics.impl.MetricsService;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.servicemanager.ServiceDescriptor;
import com.hazelcast.spi.impl.servicemanager.ServiceDescriptorProvider;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.Preconditions.checkHasText;
import static java.util.stream.Collectors.toMap;
import static org.influxdb.BatchOptions.DEFAULTS;

/**
 * A service which publishes the contents of the {@link MetricsRegistry} to an InfluxDB database.
 */
public class InfluxDbMetricsPublisherService implements MetricsPublisher {

    public static final String SERVICE_NAME = "hz:impl:influxDbMetricsPublisherService";

    public static final String INFLUDB_URL_PROPERTY = "influxdb.url";
    public static final String INFLUDB_DB_PROPERTY = "influxdb.db";
    public static final String INFLUDB_DB_USERNAME_PROPERTY = "influxdb.db.username";
    public static final String INFLUDB_DB_PASSWORD_PROPERTY = "influxdb.db.password";

    private InfluxDB influxDB;

    public InfluxDbMetricsPublisherService(NodeEngine nodeEngine) {
        Properties configProperties = nodeEngine.getConfig().getProperties();
        String url = checkHasText(configProperties.getProperty(INFLUDB_URL_PROPERTY), INFLUDB_URL_PROPERTY + " cannot be null");
        String db = checkHasText(configProperties.getProperty(INFLUDB_DB_PROPERTY), INFLUDB_DB_PROPERTY + "cannot be null");
        String username = checkHasText(configProperties.getProperty(INFLUDB_DB_USERNAME_PROPERTY), INFLUDB_DB_USERNAME_PROPERTY + "cannot be null");
        String password = checkHasText(configProperties.getProperty(INFLUDB_DB_PASSWORD_PROPERTY), INFLUDB_DB_PASSWORD_PROPERTY + "cannot be null");
        this.influxDB = InfluxDBFactory.connect(url, username, password)
                                       .setDatabase(db)
                                       .enableBatch(
                                               DEFAULTS.exceptionHandler((points, throwable) -> ExceptionUtil.rethrow(throwable))
                                       );
        MetricsService metricsService = nodeEngine.getService(MetricsService.SERVICE_NAME);
        metricsService.registerPublisher(e -> this);
    }

    @Override
    public void publishLong(String name, long value, Set<MetricTarget> excludedTargets) {
        publish(name, value);
    }

    @Override
    public void publishDouble(String name, double value, Set<MetricTarget> excludedTargets) {
        publish(name, value);
    }

    private void publish(String name, Number value) {
        Map<String, String> tags = MetricsUtil.parseMetricName(name)
                                              .stream()
                                              .collect(toMap(Entry::getKey, Entry::getValue));
        String metric = tags.get("metric");
        Point point = Point.measurement(metric)
                           .addField("value", value)
                           .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                           .tag(tags)
                           .build();
        influxDB.write(point);

    }

    @Override
    public void shutdown() {
        influxDB.close();
    }

    @Override
    public String name() {
        return "InfluxDB Metrics Publisher";
    }

    public static class InfluxDbMetricsPublisherServiceDescriptorProvider implements ServiceDescriptorProvider {

        @Override
        public ServiceDescriptor[] createServiceDescriptors() {
            return new ServiceDescriptor[] {
                    new ServiceDescriptor() {
                        @Override
                        public String getServiceName() {
                            return SERVICE_NAME;
                        }

                        @Override
                        public Object getService(NodeEngine nodeEngine) {
                            return new InfluxDbMetricsPublisherService(nodeEngine);
                        }
                    }
            };
        }
    }

}
