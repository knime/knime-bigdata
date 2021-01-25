/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on Jan 25, 2021 by Bjoern Lohrmann, KNIME GmbH
 */
package org.knime.bigdata.spark.core.port;

import java.io.IOException;
import java.util.Optional;

import org.knime.core.node.InvalidSettingsException;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.connections.uriexport.URIExporter;
import org.knime.filehandling.core.connections.uriexport.URIExporterIDs;
import org.knime.filehandling.core.port.FileSystemPortObject;
import org.knime.filehandling.core.port.FileSystemPortObjectSpec;

/**
 * Provides utility method(s) for Spark nodes that use a {@link FileSystemPortObject}.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class FileSystemPortChecker {

    /**
     * Checks whether the given {@link FileSystemPortObjectSpec} is Hadoop-compatible, i.e. whether it provides a
     * Hadoop-{@link URIExporter}.
     *
     * @param spec The {@link FileSystemPortObjectSpec} to check.
     * @throws InvalidSettingsException if the given spec is not Hadoop-compatible.
     */
    public static void checkFileSystemPortHadoopCompatibility(final FileSystemPortObjectSpec spec)
        throws InvalidSettingsException {
        final Optional<FSConnection> optionalFSConn = spec.getFileSystemConnection();
        if (optionalFSConn.isPresent()) {
            try (final FSConnection conn = optionalFSConn.get()) {
                if (!conn.getURIExporters().containsKey(URIExporterIDs.DEFAULT_HADOOP)) {
                    throw new InvalidSettingsException(
                        String.format("Connected file system '%s' is not Hadoop-compatible", spec.getFileSystemType()));
                }
            } catch (IOException e) { // NOSONAR
            }
        }
    }
}
