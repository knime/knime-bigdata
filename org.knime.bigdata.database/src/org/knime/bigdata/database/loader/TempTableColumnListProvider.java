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
 *   13.08.2019 (Mareike Hoeger, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.database.loader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.knime.core.data.DataTableSpec;
import org.knime.database.dialect.DBColumn;

/**
 * Class that builds and provides the temporary table column lists for the {@link BigDataLoaderNode}
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class TempTableColumnListProvider {

    /**
     *
     * Constructs a {@link TempTableColumnListProvider} that creates a list of DBColumns for the temporary table and a
     * select name list
     *
     * @param normalColumns list of normal columns from the database
     * @param partitionColumns list of partitioning columns from the database
     * @param tableSpec the specification of the input table
     */
    public TempTableColumnListProvider(final List<DBColumn> normalColumns, final List<DBColumn> partitionColumns,
        final DataTableSpec tableSpec) {
        m_tempTableColumns = new DBColumn[normalColumns.size() + partitionColumns.size()];
        fillTempTableColumnsLists(normalColumns, partitionColumns, tableSpec);
    }

    private final List<String> m_selectOrderColumnNames = new ArrayList<>();

    private final DBColumn[] m_tempTableColumns;

    /**
     * Fills the list with input columns for the creation of temporary table and a list with column names in the correct
     * order for the select part of the insert into statement.
     *
     * @param partitionColumns
     * @param normalColumns
     *
     * @param tableSpec the tablespec of the input table
     */
    private void fillTempTableColumnsLists(final List<DBColumn> normalColumns, final List<DBColumn> partitionColumns,
        final DataTableSpec tableSpec) {
        final Map<String, Integer> colIndexMap = createColumnNameToIndexMap(tableSpec);

        Stream.concat(normalColumns.stream(), partitionColumns.stream()).forEachOrdered(d -> {
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
