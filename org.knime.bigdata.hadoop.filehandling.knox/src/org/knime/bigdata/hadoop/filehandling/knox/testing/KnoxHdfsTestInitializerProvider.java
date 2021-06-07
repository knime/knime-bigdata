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
 */
package org.knime.bigdata.hadoop.filehandling.knox.testing;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.hadoop.filehandling.knox.fs.KnoxHdfsFSConnection;
import org.knime.bigdata.hadoop.filehandling.knox.fs.KnoxHdfsFSConnectionConfig;
import org.knime.bigdata.hadoop.filehandling.knox.fs.KnoxHdfsFSDescriptorProvider;
import org.knime.bigdata.hadoop.filehandling.knox.fs.KnoxHdfsFileSystem;
import org.knime.filehandling.core.connections.FSLocationSpec;
import org.knime.filehandling.core.connections.meta.FSType;
import org.knime.filehandling.core.testing.DefaultFSTestInitializerProvider;

/**
 * WebHDFS via KNOX test initializer provider.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class KnoxHdfsTestInitializerProvider extends DefaultFSTestInitializerProvider {

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(10);

    @SuppressWarnings("resource")
    @Override
    public KnoxHdfsTestInitializer setup(final Map<String, String> configuration) throws IOException {
        return new KnoxHdfsTestInitializer(new KnoxHdfsFSConnection(toFSConnectionConfig(configuration)));
    }

    private KnoxHdfsFSConnectionConfig toFSConnectionConfig(final Map<String, String> configuration) {
        checkArgumentNotBlank(configuration.get("url"), "url must be specified.");
        try {
            final URI fsURI = new URI(getParameter(configuration, "url"));
            checkArgumentNotBlank(fsURI.getScheme(), "Invalid or missing scheme in URI.");
            checkArgumentNotBlank(fsURI.getHost(), "Invalid or missing host in URI.");
            checkArgumentNotBlank(fsURI.getPath(), "Invalid or missing path in URI.");
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid KNOX endpoint URI.", e);
        }

        try {
            if (configuration.containsKey("timeout") && Integer.parseInt(configuration.get("timeout")) < 0) {
                throw new IllegalArgumentException("Timeout must be a positive integer.");
            }
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException("Invalid timeout format.", e);
        }

        final String workingDir = generateRandomizedWorkingDir(getParameter(configuration, "workingDirPrefix"),
            KnoxHdfsFileSystem.PATH_SEPARATOR);
        return KnoxHdfsFSConnectionConfig.builder() //
            .withEndpointUrl(configuration.get("url")) //
            .withWorkingDirectory(workingDir) //
            .withUserAndPassword(getParameter(configuration, "user"), getParameter(configuration, "pass")) //
            .withConnectionTimeout(getTimeout(configuration)).withReceiveTimeout(getTimeout(configuration)) //
            .build();
    }

    private static Duration getTimeout(final Map<String, String> configuration) {
        if (configuration.containsKey("timeout")) {
            return Duration.ofSeconds(Integer.parseInt(configuration.get("timeout")));
        } else {
            return DEFAULT_TIMEOUT;
        }
    }

    private static void checkArgumentNotBlank(final String arg, final String message) {
        if (StringUtils.isBlank(arg)) {
            throw new IllegalArgumentException(message);
        }
    }

    @Override
    public FSType getFSType() {
        return KnoxHdfsFSDescriptorProvider.FS_TYPE;
    }

    @Override
    public FSLocationSpec createFSLocationSpec(final Map<String, String> configuration) {
        return toFSConnectionConfig(configuration).createFSLocationSpec();
    }
}
