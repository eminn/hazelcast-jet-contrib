package com.hazelcast.jet.contrib;

import com.hazelcast.jet.contrib.model.CDCRecord;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder.SourceBuffer;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Properties;

import static com.hazelcast.jet.contrib.util.SQLUtil.COMMIT_SCN_FIELD;
import static com.hazelcast.jet.contrib.util.SQLUtil.COMMIT_SCN_POSITION_FIELD;
import static com.hazelcast.jet.contrib.util.SQLUtil.CSF_FIELD;
import static com.hazelcast.jet.contrib.util.SQLUtil.CURRENT_DB_SCN_SQL;
import static com.hazelcast.jet.contrib.util.SQLUtil.LASTSCN_STARTPOS;
import static com.hazelcast.jet.contrib.util.SQLUtil.LOGMINER_SELECT_WITH_SCHEMA_SQL;
import static com.hazelcast.jet.contrib.util.SQLUtil.OPERATION_FIELD;
import static com.hazelcast.jet.contrib.util.SQLUtil.ROW_ID_FIELD;
import static com.hazelcast.jet.contrib.util.SQLUtil.ROW_ID_POSITION_FIELD;
import static com.hazelcast.jet.contrib.util.SQLUtil.SCN_FIELD;
import static com.hazelcast.jet.contrib.util.SQLUtil.SCN_POSITION_FIELD;
import static com.hazelcast.jet.contrib.util.SQLUtil.SEG_OWNER_FIELD;
import static com.hazelcast.jet.contrib.util.SQLUtil.SQL_REDO_FIELD;
import static com.hazelcast.jet.contrib.util.SQLUtil.START_LOGMINER_SQL;
import static com.hazelcast.jet.contrib.util.SQLUtil.STOP_LOGMINER_SQL;
import static com.hazelcast.jet.contrib.util.SQLUtil.TABLE_NAME_FIELD;
import static com.hazelcast.jet.contrib.util.SQLUtil.TEMPORARY_TABLE;
import static com.hazelcast.jet.contrib.util.SQLUtil.TIMESTAMP_FIELD;
import static com.hazelcast.jet.contrib.util.SQLUtil.parseSqlCreateRecord;
import static com.hazelcast.util.StringUtil.isNullOrEmptyAfterTrim;

/**
 * date: 2019-10-23
 * author: emindemirci
 */
public final class OracleDbSources {

    public static final String DB_HOSTNAME_PROPERTY = "db.hostname";
    public static final String DB_PORT_PROPERTY = "db.port";
    public static final String DB_NAME_PROPERTY = "db.name";
    public static final String DB_NAME_ALIAS_PROPERTY = "db.name.alias";
    public static final String DB_USER_PROPERTY = "db.user";
    public static final String DB_PASSWORD_PROPERTY = "db.password";
    public static final String DB_FETCH_SIZE = "db.fetch.size";
    public static final String PARSE_DML_DATA_PROPERTY = "parse.dml.data";
    public static final String TABLE_WHITELIST = "table.whitelist";
    public static final String RESET_OFFSET = "reset.offset";
    public static final String START_SCN = "start.scn";
    public static final String MULTITENANT = "multitenant";


    private OracleDbSources() {
    }

    public static StreamSource<CDCRecord> stream(Properties databaseProps) {

        return SourceBuilder.stream("oracle-cdc", context -> new OracleDBContext(databaseProps))
                            .createSnapshotFn(OracleDBContext::getOffsetMap)
                            .restoreSnapshotFn((oracleDBContext, maps) -> oracleDBContext.setOffsetMap(maps.get(0)))
                            .fillBufferFn(OracleDBContext::fillBufferWithRecords)
                            .destroyFn(OracleDBContext::stop)
                            .build();

    }


    private static class OracleDBContext {
        private static final ILogger LOGGER = Logger.getLogger(OracleDBContext.class);
        private Connection connection;
        private Map<String, String> offsetMap;
        private final String hostname;
        private final String port;
        private final String name;
        private final String alias;
        private final int fetchSize;
        private final String user;
        private final String pass;
        private final String startScn;
        private final boolean resetOffset;
        private volatile boolean logMinerSessionStarted = false;

        private String rowId;
        private long offsetScn;
        private long commitScn;
        private ResultSet logMinerSelectResultSet;
        private CallableStatement logMinerStartStatement;
        private CallableStatement logMinerSelectStatement;

        public OracleDBContext(Properties properties) throws Exception {
            hostname = properties.getProperty(DB_HOSTNAME_PROPERTY);
            port = properties.getProperty(DB_PORT_PROPERTY);
            name = properties.getProperty(DB_NAME_PROPERTY);
            alias = properties.getProperty(DB_NAME_ALIAS_PROPERTY);
            fetchSize = Integer.parseInt(properties.getProperty(DB_FETCH_SIZE, "100"));
            user = properties.getProperty(DB_USER_PROPERTY);
            pass = properties.getProperty(DB_PASSWORD_PROPERTY);
            startScn = properties.getProperty(START_SCN);
            resetOffset = Boolean.parseBoolean(properties.getProperty(RESET_OFFSET));
            LOGGER.info("Starting the Oracle DB connection to " + alias + "database.");
            connection = getConnection();
        }

        private void startLogMinerSession() throws SQLException {
            LOGGER.info("Starting LogMiner Session");
            logMinerStartStatement = connection.prepareCall(START_LOGMINER_SQL);

            // restored from snapshot
            if (!offsetMap.isEmpty()) {
                offsetScn = Long.parseLong(offsetMap.get(SCN_POSITION_FIELD));
                commitScn = Long.parseLong(offsetMap.get(COMMIT_SCN_POSITION_FIELD));
                rowId = offsetMap.get(ROW_ID_POSITION_FIELD);
                CallableStatement lastScnFirstPos = connection.prepareCall(LASTSCN_STARTPOS);
                lastScnFirstPos.setLong(1, offsetScn);
                lastScnFirstPos.setLong(2, offsetScn);
                ResultSet resultSet = lastScnFirstPos.executeQuery();
                while (resultSet.next()) {
                    offsetScn = lastScnFirstPos.getLong("FIRST_CHANGE#");
                }
                resultSet.close();
                lastScnFirstPos.close();
                LOGGER.info("Restored last SCN has first position: " + offsetScn);
            }

            if (!isNullOrEmptyAfterTrim(startScn)) {
                LOGGER.info("Starting from the specified start SCN: " + startScn);
                offsetScn = Long.parseLong(startScn);
            }

            if (resetOffset) {
                LOGGER.info("Resetting offset");
                offsetScn = 0;
                commitScn = 0;
                rowId = "";
            }

            if (offsetScn == 0) {
                // get offset from db;
                CallableStatement currentSCNStatement = connection.prepareCall(CURRENT_DB_SCN_SQL);
                ResultSet resultSet = currentSCNStatement.executeQuery();
                while (resultSet.next()) {
                    offsetScn = resultSet.getLong("CURRENT_SCN");
                }
                resultSet.close();
                currentSCNStatement.close();
                LOGGER.info("Got current SCN from database: " + offsetScn);
            }

            logMinerStartStatement.setLong(1, offsetScn);
            logMinerStartStatement.executeQuery();
            LOGGER.info("LogMiner Session started, executing the SELECT statement");
            logMinerSelectStatement = connection.prepareCall(LOGMINER_SELECT_WITH_SCHEMA_SQL);
            logMinerSelectStatement.setLong(1, commitScn);
            logMinerSelectStatement.setFetchSize(fetchSize);
            logMinerSelectResultSet = logMinerSelectStatement.executeQuery();
        }

        void stop() throws SQLException {
            logMinerSelectStatement.cancel();
            logMinerSelectStatement.close();
            logMinerStartStatement.cancel();
            logMinerStartStatement.close();
            connection.prepareCall(STOP_LOGMINER_SQL).executeQuery();
            connection.close();
        }


        void fillBufferWithRecords(SourceBuffer<CDCRecord> buffer) throws Exception {
            if (!logMinerSessionStarted) {
                startLogMinerSession();
                logMinerSessionStarted = true;
            }
            int count = 0;
            while (logMinerSelectResultSet.next() && count < fetchSize) {
                long scn = logMinerSelectResultSet.getLong(SCN_FIELD);
                long commitScn = logMinerSelectResultSet.getLong(COMMIT_SCN_FIELD);
                String rowId = logMinerSelectResultSet.getString(ROW_ID_FIELD);
                boolean continuationSQL = logMinerSelectResultSet.getBoolean(CSF_FIELD);


                String segOwner = logMinerSelectResultSet.getString(SEG_OWNER_FIELD);
                String segName = logMinerSelectResultSet.getString(TABLE_NAME_FIELD);
                StringBuilder sqlRedo = new StringBuilder(logMinerSelectResultSet.getString(SQL_REDO_FIELD));
                if (sqlRedo.toString().contains(TEMPORARY_TABLE)) {
                    continue;
                }
                while (continuationSQL) {
                    logMinerSelectResultSet.next();
                    sqlRedo.append(logMinerSelectResultSet.getString(SQL_REDO_FIELD));
                    continuationSQL = logMinerSelectResultSet.getBoolean(CSF_FIELD);
                }
                Timestamp timestamp = logMinerSelectResultSet.getTimestamp(TIMESTAMP_FIELD);
                String operation = logMinerSelectResultSet.getString(OPERATION_FIELD);
                count++;
                CDCRecord record = parseSqlCreateRecord(sqlRedo.toString());
                buffer.add(record);
            }
        }

        Map<String, String> getOffsetMap() {
            offsetMap.put(SCN_POSITION_FIELD, Long.toString(offsetScn));
            offsetMap.put(COMMIT_SCN_POSITION_FIELD, Long.toString(commitScn));
            offsetMap.put(ROW_ID_POSITION_FIELD, rowId);
            return offsetMap;
        }

        void setOffsetMap(Map<String, String> offsetMap) {
            this.offsetMap = offsetMap;
        }

        private Connection getConnection() throws SQLException {
            return DriverManager.getConnection("jdbc:oracle:thin:@" + hostname + ":" + port + "/" + name, user, pass);
        }

    }

}
