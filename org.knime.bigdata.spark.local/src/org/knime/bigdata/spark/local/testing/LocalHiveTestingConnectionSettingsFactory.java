/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on Jul 20, 2018 by bjoern
 */
package org.knime.bigdata.spark.local.testing;

import java.util.Map;

import org.knime.bigdata.commons.testing.TestflowVariable;
import org.knime.bigdata.spark.local.database.LocalHiveConnectionSettings;
import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.workflow.FlowVariable;

/**
 * Provides factory methods to create {@link DatabaseConnectionSettings} objects that connect to Local Hive (Spark
 * Thriftserver) for testing purposes.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 * @noreference This is testing code and its API is subject to change without notice.
 */
public class LocalHiveTestingConnectionSettingsFactory {

    /**
     * Creates a {@link DatabaseConnectionSettings} for Local Hive (Spark Thriftserver) from the given map of flow
     * variables.
     *
     * @param flowVars A map of flow variables that provide the connection settings.
     * @return a {@link DatabaseConnectionSettings}.
     */
    public static DatabaseConnectionSettings create(final Map<String, FlowVariable> flowVars) {
        return new LocalHiveConnectionSettings(
            TestflowVariable.getInt(TestflowVariable.SPARK_LOCAL_THRIFTSERVERPORT, flowVars));
    }
}
