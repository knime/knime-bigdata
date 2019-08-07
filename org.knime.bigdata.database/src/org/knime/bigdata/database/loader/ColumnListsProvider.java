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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.database.dialect.DBColumn;
import org.knime.database.model.DBTable;
import org.knime.database.session.DBSession;

/**
 * Class that builds and provides the column lists for the {@link BigDataLoaderNode}
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class ColumnListsProvider {

    private final List<DBColumn> m_normalColumns = new ArrayList<>();

    private final List<DBColumn> m_partitionColumns = new ArrayList<>();

    private final List<String> m_selectOrderColumnNames = new ArrayList<>();

    private final DBColumn[] m_tempTableColumns;

    /**
     * Constructs a new ColumnListProvider by fetching the column information from the database and building the
     * building the columns lists accordingly
     *
     * @param exec the ExecutionMonitor for the connection
     * @param table the table to fetch information from
     * @param session the database session
     * @param tableSpec the KNIME table specification
     * @throws Exception if the table information can not be retrieved from the database
     */
    public ColumnListsProvider(final ExecutionMonitor exec, final DBTable table, final DBSession session,
        final DataTableSpec tableSpec) throws Exception {
        fillColumnLists(exec, table, session);
        m_tempTableColumns = new DBColumn[m_normalColumns.size() + m_partitionColumns.size()];
        fillTempTableColumnsLists(tableSpec);
    }

    /**
     * Method that uses the describe formatted table command to get the normal and partition column information.
     *
     * @param exec {@link ExecutionMonitor}
     * @param table the hive/impala table
     * @param m_normalColumns list to fill with the normal column names in the order they appear
     * @param m_partitionColumns list to fill with the partition column names in the order they appear
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
                        if (row.startsWith("# col_name")) {
                            if (prevRow.startsWith("# Partition Information")) {
                                row = fillListWithColumnNamesFromResultSet(m_partitionColumns, result);
                            } else {
                                row = fillListWithColumnNamesFromResultSet(m_normalColumns, result);
                            }
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
     * Fills the list with input columns for the creation of temporary table and a list with column names in the correct
     * order for the select part of the insert into statement.
     *
     * @param m_normalColumns the normal column list
     * @param m_partitionColumns the partition columns
     * @param m_tempTableColumns the temporary table Column list to fill
     * @param m_selectOrderColumnNames the column name list for the select statement to fill
     * @param tableSpec the tablespec of the input table
     */
    private void fillTempTableColumnsLists(final DataTableSpec tableSpec) {
        final Map<String, Integer> colIndexMap = createColumnNameToIndexMap(tableSpec);

        Stream.concat(m_normalColumns.stream(), m_partitionColumns.stream()).forEachOrdered(d -> {
            final int index = colIndexMap.get(d.getName().toLowerCase());
            final String tempName = String.format("column%d", index);
            //replace characters that can not be handled by Parquet
            m_tempTableColumns[index] = new DBColumn(tempName, d.getType(), d.isNotNull());
            m_selectOrderColumnNames.add(tempName);
        });
    }

    private static Map<String, Integer> createColumnNameToIndexMap(final DataTableSpec tableSpec) {
        final Map<String, Integer> colIndexMap = new HashMap<>();
        for (int i = 0; i < tableSpec.getNumColumns(); i++) {
            colIndexMap.put(tableSpec.getColumnSpec(i).getName().toLowerCase(), i);
        }
        return colIndexMap;
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

    /**
     * @return the tempTableColumns
     */
    public DBColumn[] getTempTableColumns() {
        return m_tempTableColumns;
    }

    /**
     * @return the selectOrderColumnNames
     */
    public List<String> getSelectOrderColumnNames() {
        return m_selectOrderColumnNames;
    }

}
