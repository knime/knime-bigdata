package org.knime.bigdata.spark.local.database;

import org.knime.base.node.io.database.connection.util.ParameterizedDatabaseConnectionSettings;
import org.knime.bigdata.hive.utility.HiveDriverDetector;
import org.knime.core.node.port.database.DatabaseConnectionSettings;

/**
 * A {@link DatabaseConnectionSettings} class for local Hive (Spark Thriftserver).
 * 
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class LocalHiveConnectionSettings extends ParameterizedDatabaseConnectionSettings {

    /**
     * Default constructor.
     * 
     * @param hiveserverPort The TCP that local Hive (i.e. Spark thriftserver) is listening on.
     */
    public LocalHiveConnectionSettings(int hiveserverPort) {

        setDatabaseIdentifier(LocalHiveUtility.DATABASE_IDENTIFIER);
        setDriver(HiveDriverDetector.getDriverName());
        setPort(hiveserverPort);
        setRowIdsStartWithZero(true);
        setRetrieveMetadataInConfigure(false);
        setHost("localhost");
        setParameter("");
        setDatabaseName("");
        setJDBCUrl(String.format("jdbc:hive2://localhost:%d/", hiveserverPort));
    }
}
