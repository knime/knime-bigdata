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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * Wrapper around the local Hadoop file system to return a valid hive scratch directory permission on Windows.
 *
 * The local hive scratch directory requires a 0733 permission on startup. Hadoop uses a tool called winutils.exe on
 * Windows to read and validate this permission. In case of a failures running this tool (e.g. AD DC not available), the
 * error gets catched, ignored and a (wrong) default permission of 666 will be returned. The hive startup fails in this
 * case and becomes very difficult to debug.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class LocalFileSystemHiveTempWrapper extends LocalFileSystem {
    private static final FsPermission SCRATCHDIR_PERM = new FsPermission((short)00733);
    private static final String SCRATCHDIR_OWNER = "hive-user";
    private static final String SCRATCHDIR_GROUP = "hive-group";

    private Path m_hiveScratchDir = null;

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
        setHiveScratchDir(conf);
    }

    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
        setHiveScratchDir(conf);
    }

    /**
     * Read the hive scratch directory from the given configuration if available.
     */
    private void setHiveScratchDir(final Configuration conf) {
        m_hiveScratchDir = null;

        if (conf != null) {
            final String path = conf.get("hive.exec.scratchdir");
            if (!StringUtils.isEmpty(path)) {
                m_hiveScratchDir = makeQualified(new Path(path));
            }
        }
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        return fakePermission(super.getFileStatus(f));
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        final FileStatus[] oriStatus = super.listStatus(f);
        final FileStatus[] newStatus = new FileStatus[oriStatus.length];
        for (int i = 0; i < oriStatus.length; i++) {
            newStatus[i] = fakePermission(oriStatus[i]);
        }
        return newStatus;
    }

    /**
     * Replace the permission, owner and group of the given file status object if it matches the hive scratch directory.
     */
    private FileStatus fakePermission(final FileStatus s) throws IOException {
        if (m_hiveScratchDir != null && s.getPath().equals(m_hiveScratchDir)) {
            return new FileStatus(s.getLen(), s.isDirectory(), s.getReplication(), s.getBlockSize(),
                s.getModificationTime(), s.getAccessTime(), SCRATCHDIR_PERM, SCRATCHDIR_OWNER, SCRATCHDIR_GROUP,
                (s.isSymlink() ? s.getSymlink() : null), s.getPath());
        } else {
            return s;
        }
    }
}
