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

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.knime.bigdata.fileformats.parquet.type.ParquetType;
import org.knime.core.data.DataRow;

/**
 * A class that specifies how instances of {@link DataRow} are materialized from
 * a Parquet file by means of a {@link RecordMaterializer}.
 *
 * @author Mareike Hoeger, KNIME AG, Zurich, Switzerland
 * @author Marc Bux, KNIME AG, Zurich, Switzerland
 */
public final class DataRowReadSupport extends ReadSupport<DataRow> {
    private final RecordMaterializer<DataRow> m_materializer;

    /**
     * Read support for KNIME data rows
     *
     * @param columnTypes the columns types
     */
    public DataRowReadSupport(final ParquetType[] columnTypes) {
        m_materializer = new DataRowMaterializer(columnTypes);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordMaterializer<DataRow> prepareForRead(final Configuration configuration,
            final Map<String, String> keyValueMetaData, final MessageType fileSchema, final ReadContext readContext) {
        return m_materializer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ReadContext init(final InitContext context) {
        return new ReadContext(context.getFileSchema());
    }

    /**
     * A class that specifies how to materialize instances of {@link DataRow}
     * from a Parquet file by means of a {@link DataRowConverter}.
     */
    static final class DataRowMaterializer extends RecordMaterializer<DataRow> {
        private final DataRowConverter m_rowConverter;

        DataRowMaterializer(final ParquetType[] columnTypes) {
            m_rowConverter = new DataRowConverter(columnTypes);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public DataRow getCurrentRecord() {
            return m_rowConverter.getRow();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public GroupConverter getRootConverter() {
            return m_rowConverter;
        }
    }
}
