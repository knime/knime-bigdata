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
 *  2021-09-06 (Alexander Bondaletov): created
 */
package org.knime.bigdata.hadoop.filehandling.fs;

import java.io.IOException;
import java.nio.file.LinkOption;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.knime.filehandling.core.connections.base.attributes.BasePosixFileAttributeView;

/**
 * Implementation of the {@link PosixFileAttributeView} interface for HDFS.
 *
 * @author Alexander Bondaletov
 */
class HdfsFileAttributeView extends BasePosixFileAttributeView<HdfsPath, HdfsFileSystem> {

    /**
     * Constructor.
     *
     * @param path the path this attributes view belongs to.
     * @param linkOptions whether to following symbolic links or not.
     */
    HdfsFileAttributeView(final HdfsPath path, final LinkOption[] linkOptions) {
        super(path, linkOptions);
    }

    @SuppressWarnings("resource")
    private FileSystem getHadoopFS() {
        return getFileSystem().getHadoopFileSystem();
    }

    @SuppressWarnings("resource")
    @Override
    protected void setTimesInternal(final FileTime lastModifiedTime, final FileTime lastAccessTime,
        final FileTime createTime) throws IOException {
        getHadoopFS().setTimes(getPath().toHadoopPath(), lastModifiedTime.toMillis(), lastAccessTime.toMillis());
        clearAttributeCache();
    }

    @SuppressWarnings("resource")
    @Override
    protected void setOwnerInternal(final UserPrincipal owner) throws IOException {
        getHadoopFS().setOwner(getPath().toHadoopPath(), owner.getName(), null);
        clearAttributeCache();
    }

    @SuppressWarnings("resource")
    @Override
    protected void setGroupInternal(final GroupPrincipal group) throws IOException {
        getHadoopFS().setOwner(getPath().toHadoopPath(), null, group.getName());
        clearAttributeCache();
    }

    @SuppressWarnings("resource")
    @Override
    protected void setPermissionsInternal(final Set<PosixFilePermission> perms) throws IOException {
        var user = toFsAction(perms, PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE,
            PosixFilePermission.OWNER_EXECUTE);
        var group = toFsAction(perms, PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE,
            PosixFilePermission.GROUP_EXECUTE);
        var others = toFsAction(perms, PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_WRITE,
            PosixFilePermission.OTHERS_EXECUTE);

        var permission = new FsPermission(user, group, others);
        getHadoopFS().setPermission(getPath().toHadoopPath(), permission);
        clearAttributeCache();
    }

    private static FsAction toFsAction(final Set<PosixFilePermission> perms, final PosixFilePermission read,
        final PosixFilePermission write, final PosixFilePermission exec) {
        FsAction res = FsAction.NONE;

        if (perms.contains(read)) {
            res = res.or(FsAction.READ);
        }

        if (perms.contains(write)) {
            res = res.or(FsAction.WRITE);
        }

        if (perms.contains(exec)) {
            res = res.or(FsAction.EXECUTE);
        }

        return res;
    }
}
