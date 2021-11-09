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
 *   Nov 7, 2021 (Sascha Wolke, KNIME GmbH): created
 */
package org.knime.bigdata.fileformats.filehandling.reader.parquet;

import java.util.Collections;
import java.util.List;

import org.knime.bigdata.fileformats.filehandling.reader.cell.BigDataCell;
import org.knime.bigdata.fileformats.filehandling.reader.parquet.cell.ParquetConverterProvider;
import org.knime.bigdata.fileformats.filehandling.reader.type.KnimeType;
import org.knime.filehandling.core.node.table.reader.spec.TypedReaderColumnSpec;
import org.knime.filehandling.core.node.table.reader.spec.TypedReaderTableSpec.TypedReaderTableSpecBuilder;

/**
 * Represents a Parquet column with one or more KNIME columns (one-to-n relation).
 *
 * @author Sascha Wolke, KNIME GmbH
 */
class GroupParquetColumn extends ParquetColumn {

    private final ParquetConverterProvider m_converterProvider;

    private final List<TypedReaderColumnSpec<KnimeType>> m_columnSpecs;

    private final BigDataCell[] m_cells;

    private final String m_error;

    GroupParquetColumn(final ParquetConverterProvider converterProvider,
        final List<TypedReaderColumnSpec<KnimeType>> columnSpecs, final BigDataCell[] cells) {
        m_converterProvider = converterProvider;
        m_columnSpecs = columnSpecs;
        m_cells = cells;
        m_error = null;
    }

    GroupParquetColumn(final String error) {
        m_converterProvider = null;
        m_columnSpecs = null;
        m_cells = null;
        m_error = error;
    }

    /**
     * @return {@code true} if the column should be skipped
     */
    @Override
    public boolean skipColumn() {
        return m_error != null;
    }

    @Override
    public String getErrorMessage() {
        return m_error;
    }

    @Override
    public void addConverterProviders(final List<ParquetConverterProvider> converters) {
        if (!skipColumn()) {
            converters.add(m_converterProvider);
        }
    }

    @Override
    public void addColumnSpecs(final TypedReaderTableSpecBuilder<KnimeType> specBuilder) {
        if (!skipColumn()) {
            for (final var spec : m_columnSpecs) {
                specBuilder.addColumn(spec);
            }
        }
    }

    @Override
    public void addColumnCells(final List<BigDataCell> cells) {
        if (!skipColumn()) {
            Collections.addAll(cells, m_cells);
        }
    }

    @Override
    public GroupParquetColumn asGroupColumn() {
        return this;
    }

}
