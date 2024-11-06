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
 *   2024-11-05 (Sascha Wolke, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.bigdata.databricks.sqlwarehouse;

import static org.apache.commons.lang3.StringUtils.stripToEmpty;
import static org.knime.database.driver.URLTemplates.VARIABLE_NAME_HOST;
import static org.knime.database.driver.URLTemplates.VARIABLE_NAME_PORT;
import static org.knime.database.driver.URLTemplates.resolveDriverUrl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.knime.bigdata.database.databricks.DatabricksUserDBConnectionController;
import org.knime.bigdata.database.databricks.DatabricksOAuth2DBConnectionController;
import org.knime.bigdata.databricks.credential.DatabricksAccessTokenCredential;
import org.knime.bigdata.databricks.credential.DatabricksAccessTokenType;
import org.knime.bigdata.databricks.rest.sql.SQLWarehouseInfo;
import org.knime.bigdata.spark.core.databricks.context.DatabricksClusterStatusProvider;
import org.knime.core.node.InvalidSettingsException;
import org.knime.database.connection.DBConnectionController;
import org.knime.database.driver.DBDriverWrapper;
import org.knime.database.util.StringTokenException;

/**
 * Factory to create a SQL Warehouse DB connection controller.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
final class SQLWarehouseDBControllerFactory {

    private SQLWarehouseDBControllerFactory() {
    }

    static DBConnectionController createController(final DBDriverWrapper driver, final SQLWarehouseInfo info,
        final DatabricksAccessTokenCredential credential, final DatabricksClusterStatusProvider clusterStatus)
        throws InvalidSettingsException {

        final String jdbcUrl = getJdbcUrl(driver, info.odbcHostname, info.odbcPort);
        final String httpPath = info.odbcPath;
        final String description = String.format("SQL Warehouse=\"%s\" (%s)", info.id, info.name);

        if (credential.getDatabricksTokenType() == DatabricksAccessTokenType.PERSONAL_ACCESS_TOKEN) {
            final String user = "token";
            final String password = getPersonalAccessToken(credential);

            return new DatabricksUserDBConnectionController(jdbcUrl, httpPath, clusterStatus, description, user,
                password);
        }

        return new DatabricksOAuth2DBConnectionController(jdbcUrl, httpPath, clusterStatus, description, credential);
    }

    private static String getPersonalAccessToken(final DatabricksAccessTokenCredential credential)
        throws InvalidSettingsException {
        try {
            return credential.getAccessToken();
        } catch (final IOException e) {
            throw new InvalidSettingsException(
                "Unable to load personal access token from input connection. Restart predecessor nodes.", e);
        }
    }

    private static String getJdbcUrl(final DBDriverWrapper driver, final String host, final int port)
        throws InvalidSettingsException {

        try {
            final Map<String, String> variableValues = new HashMap<>();
            variableValues.put(VARIABLE_NAME_HOST, stripToEmpty(host));
            variableValues.put(VARIABLE_NAME_PORT, String.valueOf(port));
            return resolveDriverUrl(driver.getURLTemplate(), variableValues, variableValues);
        } catch (final StringTokenException e) {
            throw new InvalidSettingsException("Unable to resolve JDBC URL from driver template.", e);
        }
    }

}
