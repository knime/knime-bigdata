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
 */
package org.knime.bigdata.spark.local.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Functional interface with a helper method to wrap a query result and close the statement/result set on
 * {@link SQLException}s.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@FunctionalInterface
public interface LocalHiveSafeWrappedQuery {

    /**
     * Wrapper method that might throw an {@link SQLException}.
     *
     * @param statement the statement used to execute the query
     * @param resultSet the result set returned by query
     * @return the wrapped result set
     * @throws SQLException
     */
    ResultSet apply(final Statement statement, final ResultSet resultSet) throws SQLException;

    /**
     * Run a given query and close statement + result set before rethrowing exceptions.
     *
     * @param connection connection to use
     * @param query query to run
     * @param wrapper a wrapper function that might throw an exception
     * @return result after wrapping it
     * @throws SQLException
     */
    public static ResultSet runQuery(final Connection connection, final String query, final LocalHiveSafeWrappedQuery wrapper) throws SQLException {
        final var statement = connection.createStatement();
        try {
            final var resultSet = statement.executeQuery(query);
            try { // NOSONAR
                return wrapper.apply(statement, resultSet);
            } catch (final SQLException throwable) {
                try (final var resultSetToClose = resultSet) {
                    throw throwable;
                }
            }
        } catch (final SQLException throwable) {
            try (var statementToClose = statement) {
                throw throwable;
            }
        }
    }
}
