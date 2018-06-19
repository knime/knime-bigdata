/*
 * ------------------------------------------------------------------------
 *
 * Copyright by KNIME AG, Zurich, Switzerland Website: http://www.knime.com;
 * Email: contact@knime.com
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, Version 3, as published by the
 * Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, see <http://www.gnu.org/licenses>.
 *
 * Additional permission under GNU GPL version 3 section 7:
 *
 * KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs. Hence,
 * KNIME and ECLIPSE are both independent programs and are not derived from each
 * other. Should, however, the interpretation of the GNU GPL Version 3
 * ("License") under any applicable laws result in KNIME and ECLIPSE being a
 * combined program, KNIME AG herewith grants you the additional permission to
 * use and propagate KNIME together with ECLIPSE with only the license terms in
 * place for ECLIPSE applying to ECLIPSE and the GNU GPL Version 3 applying for
 * KNIME, provided the license terms of ECLIPSE themselves allow for the
 * respective use and propagation of ECLIPSE together with KNIME.
 *
 * Additional permission relating to nodes for KNIME that extend the Node
 * Extension (and in particular that are based on subclasses of NodeModel,
 * NodeDialog, and NodeView) and that only interoperate with KNIME through
 * standard APIs ("Nodes"): Nodes are deemed to be separate and independent
 * programs and to not be covered works. Notwithstanding anything to the
 * contrary in the License, the License does not apply to Nodes, you are not
 * required to license Nodes under the License, and you are granted a license to
 * prepare and propagate Nodes, in each case even if such Nodes are propagated
 * with or for interoperation with KNIME. The owner of a Node may freely choose
 * the license terms applicable to such Node, including when such Node is
 * propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History 2 May 2018 (Marc Bux, KNIME AG, Zurich, Switzerland): created
 */
package org.knime.bigdata.fileformats.parquet.reader;

import java.util.Arrays;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.knime.bigdata.fileformats.parquet.type.ParquetType;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.RowKey;
import org.knime.core.data.container.BlobSupportDataRow;

/**
 * A class that converts Parquet records to {@link DataRow} instances.
 *
 * @author Mareike Hoeger, KNIME AG, Zurich, Switzerland
 * @author Marc Bux, KNIME AG, Zurich, Switzerland
 */
final class DataRowConverter extends GroupConverter {

    private final ParquetType[] m_columnTypes;

    private DataRow m_dataRow;

    private long m_rowCount = 0l;

    DataRowConverter(final ParquetType[] columnTypes) {
        m_columnTypes = columnTypes.clone();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() {
        // nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Converter getConverter(final int fieldIndex) {
        return m_columnTypes[fieldIndex].getConverter();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void end() {
        final DataCell[] dataCells = Arrays.stream(m_columnTypes).map(ParquetType::readValue).toArray(DataCell[]::new);
        final RowKey rowKey = RowKey.createRowKey(m_rowCount);
        m_rowCount++;
        m_dataRow = new BlobSupportDataRow(rowKey, dataCells);
    }

    DataRow getRow() {
        return m_dataRow;
    }

}
