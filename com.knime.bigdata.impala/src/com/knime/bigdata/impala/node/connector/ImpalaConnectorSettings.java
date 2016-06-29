/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
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
package com.knime.bigdata.impala.node.connector;

import org.knime.base.node.io.database.connection.util.ParameterizedDatabaseConnectionSettings;

import com.knime.bigdata.impala.utility.ImpalaUtility;

/**
 * Settings for the Impala connector node.
 *
 * @author Tobias Koetter, KNIME.com, Zurich, Switzerland
 */
class ImpalaConnectorSettings extends ParameterizedDatabaseConnectionSettings {

    ImpalaConnectorSettings() {
        setPort(21050);
        setRowIdsStartWithZero(true);
        setRetrieveMetadataInConfigure(false);
        setDatabaseName("default");
        setDatabaseIdentifier(ImpalaUtility.DATABASE_IDENTIFIER);
    }
}
