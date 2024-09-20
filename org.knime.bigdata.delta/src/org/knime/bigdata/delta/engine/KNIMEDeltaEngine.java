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
 *   2024-09-17 (Tobias): created
 */
package org.knime.bigdata.delta.engine;

import org.apache.hadoop.conf.Configuration;
import org.knime.bigdata.delta.engine.json.KNIMEJsonHandler;
import org.knime.bigdata.delta.engine.parquet.KNIMEParquetHandler;
import org.knime.bigdata.hadoop.filesystem.NioFileSystemUtil;
import org.knime.filehandling.core.connections.FSPath;

import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.FileSystemClient;
import io.delta.kernel.engine.JsonHandler;
import io.delta.kernel.engine.ParquetHandler;

/**
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class KNIMEDeltaEngine extends DefaultEngine {

    private final KNIMEFileSystemClient m_fileSystemClient;

    private final static Configuration CONF = NioFileSystemUtil.getConfiguration();

    private final FSPath m_rootPath;

    public KNIMEDeltaEngine(final FSPath rootPath) {
        super(CONF);
        m_rootPath = rootPath;
        m_fileSystemClient = new KNIMEFileSystemClient(m_rootPath);
    }

    @Override
    public FileSystemClient getFileSystemClient() {
        return m_fileSystemClient;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JsonHandler getJsonHandler() {
        return new KNIMEJsonHandler(CONF, m_rootPath);
    }

    @Override
    public ParquetHandler getParquetHandler() {
        return new KNIMEParquetHandler(CONF, m_rootPath);
    }
}
