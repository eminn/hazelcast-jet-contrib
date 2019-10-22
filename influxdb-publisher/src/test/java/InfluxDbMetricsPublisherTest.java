import com.hazelcast.config.Config;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.contrib.influxdb.publisher.InfluxDbMetricsPublisherService;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.test.HazelcastTestSupport;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult.Series;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.InfluxDBContainer;
import org.testcontainers.containers.Network;

import java.util.List;

public class InfluxDbMetricsPublisherTest extends JetTestSupport {
    private static final String DATABASE_NAME = "test";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "root";

    @Rule
    public InfluxDBContainer influxdbContainer = new InfluxDBContainer<>()
            .withAuthEnabled(true)
            .withDatabase(DATABASE_NAME)
            .withUsername(USERNAME)
            .withPassword(PASSWORD)
            .withNetwork(Network.newNetwork());

    private JetInstance jet;
    private InfluxDB influxDB;

    @Before
    public void setup() {
        influxDB = influxdbContainer.getNewInfluxDB();
        JetConfig jetConfig = new JetConfig();
        Config config = jetConfig.getHazelcastConfig();
        config.getMetricsConfig().setEnabled(true);
        config.getMetricsConfig().setCollectionIntervalSeconds(1);
        config.setProperty(InfluxDbMetricsPublisherService.INFLUDB_URL_PROPERTY, influxdbContainer.getUrl());
        config.setProperty(InfluxDbMetricsPublisherService.INFLUDB_DB_USERNAME_PROPERTY, USERNAME);
        config.setProperty(InfluxDbMetricsPublisherService.INFLUDB_DB_PASSWORD_PROPERTY, PASSWORD);
        config.setProperty(InfluxDbMetricsPublisherService.INFLUDB_DB_PROPERTY, DATABASE_NAME);
        jet = createJetMember(jetConfig);
    }


    @Test
    public void testMetrics() {
        HazelcastTestSupport.sleepAtLeastSeconds(5);

        List<Series> series = influxDB.query(new Query("SELECT * FROM test.autogen.\"taskletCount\"")).getResults().get(0).getSeries();
        for (Series serie : series) {
            Assert.assertEquals("taskletCount", serie.getName());
            String metric = (String) serie.getValues().get(0).get(2);
            String module = (String) serie.getValues().get(0).get(3);
            String unit = (String) serie.getValues().get(0).get(4);
            Assert.assertEquals("taskletCount", metric);
            Assert.assertEquals("jet", module);
            Assert.assertEquals("count", unit);
        }
    }
}
