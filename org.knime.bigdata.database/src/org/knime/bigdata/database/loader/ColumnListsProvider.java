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
 *   31.07.2019 (Mareike Hoeger, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.database.loader;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.database.dialect.DBColumn;
import org.knime.database.model.DBTable;
import org.knime.database.session.DBSession;

/**
 * Class that builds and provides the column lists from the database table for the {@link BigDataLoaderNode}
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class ColumnListsProvider {

    private final List<DBColumn> m_normalColumns = new ArrayList<>();

    private final List<DBColumn> m_partitionColumns = new ArrayList<>();

    /**
     * Constructs a new ColumnListProvider by fetching the column information from the database and building the
     * building the columns lists accordingly
     *
     * @param exec the ExecutionMonitor for the connection
     * @param table the table to fetch information from
     * @param session the database session
     * @throws Exception if the table information can not be retrieved from the database
     */
    public ColumnListsProvider(final ExecutionMonitor exec, final DBTable table, final DBSession session)
        throws Exception {
        fillColumnLists(exec, table, session);
    }

    /**
     * Method that uses the describe formatted table command to get the normal and partition column information.
     *
     * In Hive, the first line starts with {@code # col_name}, followed by normal column names in the next rows. On
     * Databricks, this first line already contains the first normal column.
     *
     * @param exec {@link ExecutionMonitor}
     * @param table the hive/impala table
     * @param session the {@link DBSession} to use
     * @throws CanceledExecutionException if the execution has been cancelled.
     * @throws SQLException if a database access error occurs
     */
    private void fillColumnLists(final ExecutionMonitor exec, final DBTable table, final DBSession session)
        throws SQLException, CanceledExecutionException {
        final String getCreateTable = "DESCRIBE FORMATTED " + session.getDialect().createFullName(table);

        try (Connection connection = session.getConnectionProvider().getConnection(exec)) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet result = statement.executeQuery(getCreateTable)) {
                    String prevRow = "";
                    String row = "";
                    while (result.next()) {
                        prevRow = row;
                        row = result.getString(1);

                        // Normal columns in Hive or partition columns
                        if (row.startsWith("# col_name")) {
                            if (prevRow.startsWith("# Partition Information")) {
                                row = fillListWithColumnNamesFromResultSet(m_partitionColumns, result);
                            } else {
                                row = fillListWithColumnNamesFromResultSet(m_normalColumns, result);
                            }

                        // First normal column on Databricks
                        } else if (!row.isEmpty() && m_normalColumns.isEmpty()) {
                            m_normalColumns.add(new DBColumn(row, result.getString(2), false));
                            row = fillListWithColumnNamesFromResultSet(m_normalColumns, result);
                        }
                    }
                }
            }
        }
    }

    private static String fillListWithColumnNamesFromResultSet(final List<DBColumn> columns, final ResultSet result)
        throws SQLException {
        String row = "";
        while (result.next()) {
            row = result.getString(1);
            if (row.startsWith("#")) {
                break;
            } else {
                if (!row.isEmpty()) {
                    final String type = result.getString(2);
                    final DBColumn column = new DBColumn(row, type, false);
                    columns.add(column);
                }
            }
        }
        return row;
    }

    /**
     * @return the normalColumns
     */
    public List<DBColumn> getNormalColumns() {
        return m_normalColumns;
    }

    /**
     * @return the partitionColumns
     */
    public List<DBColumn> getPartitionColumns() {
        return m_partitionColumns;
    }

}
