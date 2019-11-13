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
package org.knime.bigdata.fileformats.parquet.writer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetDestination;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetParameter;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetType;
import org.knime.bigdata.fileformats.utility.BigDataFileFormatException;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.convert.map.CellValueConsumerFactory;
import org.knime.core.data.convert.map.ConsumptionPath;
import org.knime.core.data.convert.map.MappingFramework;

/**
 * A class that specifies how instances of {@link DataRow} are converted and
 * consumed by Parquet's {@link RecordConsumer}.
 *
 * @author Mareike Hoeger, KNIME AG, Zurich, Switzerland
 * @author Marc Bux, KNIME AG, Zurich, Switzerland
 */
public final class DataRowWriteSupport extends WriteSupport<DataRow> {
    private final String m_name;

    private RecordConsumer m_recordConsumer;

    private final ConsumptionPath[] m_paths;

    private final ParquetParameter[] m_params;

    private ParquetDestination m_destination;

    private final DataTableSpec m_spec;

    /**
     * Write Support for KNIME DataRow
     *
     * @param name the name of table. All special characters will be replaced with _!
     * @param spec the table spec
     * @param consumptionPaths the type mapping consumption paths
     * @param params the parameters for writing
     */
    public DataRowWriteSupport(final String name, final DataTableSpec spec,
            final ConsumptionPath[] consumptionPaths, final ParquetParameter[] params) {
        if (name != null) {
            m_name = name.replaceAll("\\W", "_");
        } else {
            m_name = "KNIMETable";
        }
        m_paths = consumptionPaths.clone();
        m_spec = spec;
        m_params = params.clone();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public WriteContext init(final Configuration configuration) {
        @SuppressWarnings("unchecked")
        final List<ParquetType> types = (List<ParquetType>) Arrays.stream(m_paths)
        .map(ConsumptionPath::getConsumerFactory)
        .map(CellValueConsumerFactory::getDestinationType)
        .collect(Collectors.toList());


        final List<Type> fields = new ArrayList<>();
        for (int i = 0; i < m_spec.getNumColumns() ; i++) {
            final Type type = types.get(i).constructParquetType(m_spec.getColumnSpec(i).getName());
            fields.add(type);
        }
        return new WriteContext(new MessageType(m_name, fields), new HashMap<>());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void prepareForWrite(final RecordConsumer recordConsumer) {
        m_destination = new ParquetDestination(recordConsumer);
        m_recordConsumer = recordConsumer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(final DataRow record) {
        m_recordConsumer.startMessage();

        // write column values
        try {
            MappingFramework.map(record, m_destination, m_paths, m_params);
        } catch (final Exception e) {
            throw new BigDataFileFormatException(e);
        }
        m_recordConsumer.endMessage();
    }
}
