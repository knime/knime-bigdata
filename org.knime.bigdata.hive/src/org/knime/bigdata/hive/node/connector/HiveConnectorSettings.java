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
 *   Created on 06.05.2014 by thor
 */
package org.knime.bigdata.hive.node.connector;

import org.knime.base.node.io.database.connection.util.ParameterizedDatabaseConnectionSettings;
import org.knime.bigdata.hive.utility.HiveUtility;

/**
 * Settings for the Hive connector node.
 *
 * @author Thorsten Meinl, KNIME AG, Zurich, Switzerland
 */
public class HiveConnectorSettings extends ParameterizedDatabaseConnectionSettings {

    /**
     * Default constructor.
     */
    public HiveConnectorSettings() {
        setPort(10000);
        setRowIdsStartWithZero(true);
        setRetrieveMetadataInConfigure(false);
        setDatabaseIdentifier(HiveUtility.DATABASE_IDENTIFIER);
    }
}
