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
 * History 04.06.2018 (Mareike Hoeger): created
 */
package org.knime.bigdata.fileformats.orc;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.orc.CompressionKind;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.bigdata.commons.hadoop.ConfigurationFactory;
import org.knime.bigdata.commons.hadoop.UserGroupUtil;
import org.knime.bigdata.fileformats.node.reader.AbstractFileFormatReader;
import org.knime.bigdata.fileformats.node.writer.AbstractFileFormatWriter;
import org.knime.bigdata.fileformats.orc.reader.OrcKNIMEReader;
import org.knime.bigdata.fileformats.orc.writer.OrcKNIMEWriter;
import org.knime.bigdata.fileformats.utility.BigDataFileFormatException;
import org.knime.bigdata.fileformats.utility.FileFormatFactory;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;

/**
 * Factory for ORC format
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class OrcFormatFactory implements FileFormatFactory {

    private static final String SUFFIX = ".orc";

    private static final String NAME = "ORC";

    @Override
    public AbstractFileFormatReader getReader(RemoteFile<Connection> file, boolean isReadRowKey, int batchSize,
            ExecutionContext exec) {
        try {
            AbstractFileFormatReader reader;
            if (file.getConnectionInformation() != null && file.getConnectionInformation().useKerberos()) {
                final Configuration conf = ConfigurationFactory.createBaseConfigurationWithKerberosAuth();
                final UserGroupInformation user = UserGroupUtil.getKerberosUser(conf);
                reader = user.doAs(new PrivilegedExceptionAction<AbstractFileFormatReader>() {
                    @Override
                    public AbstractFileFormatReader run() throws Exception {
                        return new OrcKNIMEReader(file, isReadRowKey, batchSize, exec);
                    }
                });
            } else {
                reader = new OrcKNIMEReader(file, isReadRowKey, batchSize, exec);
            }
            return reader;
        } catch (final Exception e) {
            throw new BigDataFileFormatException(e);
        }
    }

    @Override
    public AbstractFileFormatWriter getWriter(RemoteFile<Connection> file, DataTableSpec spec, boolean isWriteRowKey,
            int chunkSize, String compression) throws IOException {
        return new OrcKNIMEWriter(file, spec, isWriteRowKey, chunkSize, compression);
    }

    @Override
    public String[] getCompressionList() {
        return Stream.of(CompressionKind.values()).map(Enum::name).toArray(String[]::new);
    }

    @Override
    public String[] getUnsupportedTypes(DataTableSpec spec) {
        return OrcTableStoreFormat.getUnsupportedTypes(spec);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getFilenameSuffix() {
        return SUFFIX;
    }

}
