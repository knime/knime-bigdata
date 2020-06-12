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
 */
package org.knime.bigdata.spark.local.hadoop;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;

/**
 * Wrapper around the local Hadoop file system to return a valid hive scratch directory permission on Windows.
 *
 * The local hive scratch directory requires a 0733 permission on startup. Hadoop uses a tool called winutils.exe on
 * Windows to read and validate this permission. In case of a failures running this tool (e.g. AD DC not available), the
 * error gets catched, ignored and a (wrong) default permission of 666 will be returned. The hive startup fails in this
 * case and becomes very difficult to debug.
 *
 * Note: The {@link RawLocalFileSystem} always uses the DeprecatedRawLocalFileStatus on Windows. This deprecated file
 * status calls {@code new File(...)} instead of {@link RawLocalFileSystem#pathToFile(Path)} and fails if the path
 * contains a scheme that is not {@code file}. The {@link LocalFileSystemHiveTempWrapper} replaces therefore all file
 * status objects to avoid this.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class LocalFileSystemHiveTempWrapper extends LocalFileSystem {
    /**
     * Scheme of this file system.
     */
    public static final String SCHEME = "spark-local-hive-tmp";

    /**
     * The {@link URI} of this file system.
     */
    public static final URI FS_URI = URI.create(SCHEME + ":///");

    /**
     * Default constructor that uses {@link RawLocalFileSystem} with a custom scheme.
     *
     * Note: The file system gets closed via {@link FilterFileSystem#close()} and the resource warning can be ignored
     * here.
     */
    @SuppressWarnings("resource")
    public LocalFileSystemHiveTempWrapper() {
        super(new PermissionIndependentRawLocalFileSystem());
    }

    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
        // copyFromLocalFile in LocalFileSystem assumes that both files are on the same file system,
        // remove the "file" scheme from the source path to handle it like files from spark-local-hive-tmp
        super.copyFromLocalFile(delSrc, Path.getPathWithoutSchemeAndAuthority(src), dst);
    }

    @Override
    public void copyToLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
        // copyToLocalFile in LocalFileSystem assumes that both files are on the same file system,
        // remove the "file" scheme from the destination path to handle it like files from spark-local-hive-tmp
        super.copyToLocalFile(delSrc, src, Path.getPathWithoutSchemeAndAuthority(dst));
    }
}
