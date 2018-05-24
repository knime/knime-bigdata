/*
 * ------------------------------------------------------------------------
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
 * -------------------------------------------------------------------
 *
 */

package org.knime.bigdata.orc.node.reader;

import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.core.data.DataTable;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.RowIterator;
import org.knime.core.node.ExecutionContext;
import org.knime.orc.OrcKNIMEReader;
import org.knime.orc.OrcReadException;

/**
 * Data table for the ORC file reader.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class OrcTable implements DataTable {

    private final RemoteFile<Connection> m_file;

    private final DataTableSpec m_tSpec;

    private final int m_batchSize;

    private final OrcKNIMEReader m_reader;

    /**
     * Creates a ORC Table, reading the content of the given file or files in
     * the given directory.
     *
     * @param file the file to read
     * @param batchSize the batch size for reading
     * @param readRowKey boolean indicating whether the row key should be read.
     * @param exec the execution context
     * @throws Exception if file can not be read
     */
    public OrcTable(RemoteFile<Connection> file, boolean readRowKey, int batchSize, ExecutionContext exec)
            throws Exception {
        super();
        m_file = file;
        m_batchSize = batchSize;
        m_reader = new OrcKNIMEReader(m_file, readRowKey, m_batchSize, exec);
        m_tSpec = m_reader.getTableSpec();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataTableSpec getDataTableSpec() {
        return m_tSpec;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RowIterator iterator() {
        try {
            return m_reader.iterator();
        } catch (final Exception e) {
            throw new OrcReadException(e);
        }
    }
}
