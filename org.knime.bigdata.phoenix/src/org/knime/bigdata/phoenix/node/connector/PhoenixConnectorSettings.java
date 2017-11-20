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
package com.knime.bigdata.phoenix.node.connector;

import org.knime.base.node.io.database.connection.util.DefaultDatabaseConnectionSettings;

import com.knime.bigdata.phoenix.utility.PhoenixUtility;

/**
 * Settings for the Impala connector node.
 *
 * @author Tobias Koetter, KNIME AG, Zurich, Switzerland
 */
class PhoenixConnectorSettings extends DefaultDatabaseConnectionSettings {


    PhoenixConnectorSettings() {
        setPort(8765);
        setRowIdsStartWithZero(true);
        setRetrieveMetadataInConfigure(false);
        setDatabaseName("dbadmin");
        setDatabaseIdentifier(PhoenixUtility.DATABASE_IDENTIFIER);
    }
}
