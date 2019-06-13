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
 *   Created on Jun 4, 2019 by mareike
 */
package org.knime.bigdata.spark.local.db;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.database.VariableContext;
import org.knime.database.attribute.AttributeValueRepository;
import org.knime.database.connection.UrlDBConnectionController;

/**
 * Database connection management controller, based on user authentication information defined by the authentication
 * type.
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class LocalHiveConnectionController extends UrlDBConnectionController {

    /**
     * Constructs a {@link LocalHiveConnectionController} object.
     *
     * @param internalSettings the internal settings to load from.
     * @throws InvalidSettingsException if the settings are not valid.
     */
    public LocalHiveConnectionController(final NodeSettingsRO internalSettings)
        throws InvalidSettingsException {
        super(internalSettings);
    }

    /**
     * Constructs a {@link LocalHiveConnectionController} object.
     *
     * @param jdbcUrl the database connection URL as a {@link String}.
     * @throws NullPointerException if {@code jdbcUrl} or {@code authenticationType} is {@code null}.
     */
    public LocalHiveConnectionController(final String jdbcUrl) {
        super(jdbcUrl);
    }

    @Override
    protected Connection createConnection(final AttributeValueRepository attributeValues, final Driver driver,
        final VariableContext variableContext, final ExecutionMonitor monitor)
                throws CanceledExecutionException, SQLException {

        return new LocalHiveWrappedConnection(super.createConnection(attributeValues, driver, variableContext, monitor));

    }

}
