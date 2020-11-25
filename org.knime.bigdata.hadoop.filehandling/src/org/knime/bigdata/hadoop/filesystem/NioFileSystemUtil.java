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
 *   Created on Aug 11, 2020 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.hadoop.filesystem;

import static org.knime.bigdata.hadoop.filesystem.NioFileSystem.SCHEME;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.knime.filehandling.core.connections.FSFileSystem;
import org.knime.filehandling.core.connections.FSPath;

/**
 * Utility methods to use a {@link NioFileSystem} as wrapper around a {@link FSFileSystem}.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public final class NioFileSystemUtil {

    private NioFileSystemUtil() {}

    /**
     * Creates a {@link NioFileSystem} compatible Hadoop configuration.
     *
     * @return Hadoop configuration usable with a {@link NioFileSystem}
     */
    public static Configuration getConfiguration() {
        final Configuration config = new Configuration();
        config.set("fs." + SCHEME + ".impl", NioFileSystem.class.getName());
        config.setClassLoader(NioFileSystemUtil.class.getClassLoader());
        return config;
    }

    /**
     * @return unique Hadoop file system instance identifier (used to cache the file system in Hadoop)
     */
    private static String getFileSystemUUID() {
        return UUID.randomUUID().toString();
    }

    /**
     * Create a Hadoop path using the given {@link FSPath} and open the Hadoop file system.
     *
     * @param fsPath desired path
     * @param config file system configuration to use
     * @return Hadoop path using the given file system identifier.
     * @throws IOException
     */
    @SuppressWarnings("resource")
    public static Path getHadoopPath(final FSPath fsPath, final Configuration config) throws IOException {
        try {
            final String key = getFileSystemUUID();
            NioFileSystem.NEXT_FILE_SYSTEM.set(fsPath.getFileSystem());
            final Path hadoopPath = new Path(new URI(SCHEME, key, fsPath.getURICompatiblePath(), null));
            hadoopPath.getFileSystem(config); // open the file system
            return hadoopPath;
        } catch (final URISyntaxException e) {
            throw new IOException(e);
        } finally {
            NioFileSystem.NEXT_FILE_SYSTEM.remove();
        }
    }
}
