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

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetCellValueProducerFactory;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetOriginalTypeSource;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetSource;
import org.knime.bigdata.fileformats.utility.BigDataFileFormatException;
import org.knime.core.data.DataRow;
import org.knime.core.data.RowKey;
import org.knime.core.data.convert.map.ExternalToKnimeMapper;
import org.knime.core.data.convert.map.MappingFramework;
import org.knime.core.data.convert.map.ProductionPath;
import org.knime.core.data.convert.map.Source.ProducerParameters;
import org.knime.core.data.filestore.FileStoreFactory;

/**
 * A class that converts Parquet records to {@link DataRow} instances.
 *
 * @author Mareike Hoeger, KNIME AG, Zurich, Switzerland
 * @author Marc Bux, KNIME AG, Zurich, Switzerland
 */
final class DataRowConverter extends GroupConverter {

    private final ProductionPath[] m_paths;

    private DataRow m_dataRow;

    private long m_rowCount = 0l;

    private final ParquetSource m_source;

    private final ProducerParameters<ParquetSource>[] m_parameters;

    private final ExternalToKnimeMapper<ParquetSource, ProducerParameters<ParquetSource>> m_externalToKnimeMapper;

    DataRowConverter(final FileStoreFactory fileStoreFactory, final ProductionPath[] paths,
        final ProducerParameters<ParquetSource>[] params) {
        m_paths = paths;
        m_externalToKnimeMapper = MappingFramework.createMapper(i -> fileStoreFactory, paths);
        m_parameters = params.clone();
        m_source = new ParquetOriginalTypeSource();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void end() {
        m_rowCount++;
        try {
            m_dataRow = m_externalToKnimeMapper.map(RowKey.createRowKey(m_rowCount), m_source, m_parameters);
        } catch (final Exception e) {
            throw new BigDataFileFormatException(e);
        }
        for(final ProductionPath path: m_paths) {
            ((ParquetCellValueProducerFactory<?>) path.getProducerFactory()).resetConverters();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Converter getConverter(final int fieldIndex) {
        final ParquetCellValueProducerFactory<?> factory =
                (ParquetCellValueProducerFactory<?>) m_paths[fieldIndex].getProducerFactory();
        return factory.getConverter(fieldIndex);
    }

    DataRow getRow() {
        return m_dataRow;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() {
        // nothing to do
    }

}
