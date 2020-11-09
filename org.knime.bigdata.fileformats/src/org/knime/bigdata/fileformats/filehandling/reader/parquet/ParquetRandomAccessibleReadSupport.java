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
 *   Sep 24, 2020 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.fileformats.filehandling.reader.parquet;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

/**
 * {@link ReadSupport} for reading records from Parquet into a {@link ParquetRandomAccessible}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
final class ParquetRandomAccessibleReadSupport extends ReadSupport<ParquetRandomAccessible> {

    @Override
    public ReadContext init(final InitContext context) {
        return new ReadContext(context.getFileSchema());
    }

    @Override
    public RecordMaterializer<ParquetRandomAccessible> prepareForRead(final Configuration configuration,
        final Map<String, String> keyValueMetaData, final MessageType fileSchema, final ReadContext readContext) {
        return new ParquetRandomAccessibleMaterializer(fileSchema.getFields().toArray(new Type[0]));
    }

    /**
     * {@link RecordMaterializer} for materializing a record from Parquet into a {@link ParquetRandomAccessible}.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    private static class ParquetRandomAccessibleMaterializer extends RecordMaterializer<ParquetRandomAccessible> {

        private final ParquetRandomAccessibleConverter m_converter;

        ParquetRandomAccessibleMaterializer(final Type[] types) {
            m_converter = new ParquetRandomAccessibleConverter(types);
        }

        @Override
        public ParquetRandomAccessible getCurrentRecord() {
            return m_converter.getRow();
        }

        @Override
        public GroupConverter getRootConverter() {
            return m_converter;
        }

    }

    /**
     * Root converter for reading records from a Parquet file.</br>
     * Note that it always returns the same {@link ParquetRandomAccessible} which is overwritten with the new record.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    private static class ParquetRandomAccessibleConverter extends GroupConverter {

        private ParquetRandomAccessible m_row;

        ParquetRandomAccessibleConverter(final Type[] types) {
            m_row = new ParquetRandomAccessible(types);
        }

        @Override
        public Converter getConverter(final int fieldIndex) {
            return m_row.get(fieldIndex).asConverter();
        }

        ParquetRandomAccessible getRow() {
            return m_row;
        }

        @Override
        public void start() {
            m_row.reset();
        }

        @Override
        public void end() {
            // nothing to do
        }

    }
}
