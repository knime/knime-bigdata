/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   16.04.2019 (Mareike Hoeger, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.database.hive;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.database.VariableContext;
import org.knime.database.attribute.AttributeValueRepository;
import org.knime.database.connection.UserDBConnectionController;

/**
 * Database connection management controller, based on user authentication information defined by the authentication
 * type.
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class HiveConnectionController extends UserDBConnectionController {

    /**
     * Constructs a {@link HiveConnectionController} object.
     *
     * @param internalSettings the internal settings to load from.
     * @param credentialsProvider the {@link CredentialsProvider} object for accessing the credential variables of the
     *            workflow.
     * @throws InvalidSettingsException if the settings are not valid.
     */
    public HiveConnectionController(final NodeSettingsRO internalSettings, final CredentialsProvider credentialsProvider)
        throws InvalidSettingsException {
        super(internalSettings, credentialsProvider);
    }

    /**
     * Constructs a {@link HiveConnectionController} object.
     *
     * @param jdbcUrl the database connection URL as a {@link String}.
     * @param authenticationType the {@linkplain AuthenticationType authentication type}.
     * @param user the optional name of the database user.
     * @param password the optional password of the database user.
     * @param credential the optional identifier of the user credential variables.
     * @param credentialsProvider the {@link CredentialsProvider} object for accessing the credential variables of the
     *            workflow.
     * @throws NullPointerException if {@code jdbcUrl} or {@code authenticationType} is {@code null}.
     */
    public HiveConnectionController(final String jdbcUrl, final AuthenticationType authenticationType,
        final String user, final String password, final String credential,
        final CredentialsProvider credentialsProvider) {
        super(jdbcUrl, authenticationType, user, password, credential, credentialsProvider);
    }

    @Override
    protected Connection createConnection(final AttributeValueRepository attributeValues, final Driver driver,
        final VariableContext variableContext, final ExecutionMonitor monitor)
                throws CanceledExecutionException, SQLException {

        return new HiveWrappedConnection(super.createConnection(attributeValues, driver, variableContext, monitor));

    }

}
