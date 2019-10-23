package com.hazelcast.jet.contrib;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import org.junit.Test;

import java.util.Properties;

/**
 * date: 2019-10-23
 * author: emindemirci
 */
public class OracleDbSourcesTest {

    @Test
    public void drawFromOracleViaConnect() throws Exception {

        System.setProperty("hazelcast.logging.type", "log4j");

        Pipeline pipeline = Pipeline.create();

        Properties oracleProperties = new Properties();
        oracleProperties.setProperty(OracleDbSources.DB_NAME_ALIAS_PROPERTY, "ORCLCDB");
        oracleProperties.setProperty(OracleDbSources.DB_NAME_PROPERTY, "ORCLCDB");
        oracleProperties.setProperty(OracleDbSources.DB_HOSTNAME_PROPERTY, "10.212.1.116");
        oracleProperties.setProperty(OracleDbSources.DB_PORT_PROPERTY, "1521");
        oracleProperties.setProperty(OracleDbSources.DB_USER_PROPERTY, "sys");
        oracleProperties.setProperty(OracleDbSources.DB_PASSWORD_PROPERTY, "top_secret");
        oracleProperties.setProperty(OracleDbSources.START_SCN, "4572074");
        oracleProperties.setProperty(OracleDbSources.DB_FETCH_SIZE, "1");
        oracleProperties.setProperty(OracleDbSources.RESET_OFFSET, "false");
        oracleProperties.setProperty(OracleDbSources.TABLE_WHITELIST, "SYS.CUSTOMERS");
//        oracleProperties.setProperty("parse.dml.data", "true");
//        oracleProperties.setProperty("multitenant", "false");


        pipeline.drawFrom(OracleDbSources.stream(oracleProperties))
                .withoutTimestamps()
                .drainTo(Sinks.logger());

        JobConfig jobConfig = new JobConfig();
        JetConfig config = new JetConfig();
        Job job = Jet.newJetInstance(config).newJob(pipeline, jobConfig);


        job.join();


    }

}
