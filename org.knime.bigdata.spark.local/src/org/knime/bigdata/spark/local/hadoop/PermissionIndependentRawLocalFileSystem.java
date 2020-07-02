package org.knime.bigdata.spark.local.hadoop;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * Wrapper around the local Hadoop file system to return a valid hive scratch directory permission on Windows.
 *
 * The local hive scratch directory requires a 0733 permission on startup. Hadoop uses a tool called winutils.exe on
 * Windows to read and validate this permission. In case of a failures running this tool (e.g. AD DC not available), the
 * error gets catched, ignored and a (wrong) default permission of 666 will be returned. The hive startup fails in this
 * case and becomes very difficult to debug.
 *
 * This file system replaces permission, owner and group in file status objects returned from the underlying
 * {@link RawLocalFileSystem} to avoid calls to winutils.exe and to return some more useful default values instead.
 *
 * Note: The {@link RawLocalFileSystem} always uses the DeprecatedRawLocalFileStatus on Windows. This deprecated file
 * status calls {@code new File(...)} instead of {@link RawLocalFileSystem#pathToFile(Path)} and fails if the path
 * contains a scheme that is not {@code file}. The {@link LocalFileSystemHiveTempWrapper} replaces therefore all file
 * status objects to avoid this.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class PermissionIndependentRawLocalFileSystem extends RawLocalFileSystem {
    private static final FsPermission DEFAULT_PERM = new FsPermission((short)00777);
    private static final String DEFAULT_OWNER = "hive-user";
    private static final String DEAULT_GROUP = "hive-group";

    @Override
    public URI getUri() {
        return LocalFileSystemHiveTempWrapper.FS_URI;
    }

    @Override
    public String getScheme() {
        return LocalFileSystemHiveTempWrapper.SCHEME;
    }

    @Override
    public void setPermission(Path p, FsPermission permission) throws IOException {
        // ignore any permission change
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        return replacePermission(super.getFileStatus(f));
    }

    @Override
    public FileStatus getFileLinkStatus(final Path f) throws IOException {
        return replacePermission(super.getFileLinkStatus(f));
    }

    /**
     * {@inheritDoc}
     *
     * Note: {@link RawLocalFileSystem#getLinkTarget(Path)} calls deprecatedGetFileLinkStatusInternal that supports only
     * a path without scheme or with a {@code file} scheme and fails on other schemes. This method removes the virtual
     * scheme before it calls the super implementation.
     */
    @Override
    public Path getLinkTarget(Path f) throws IOException {
        return super.getLinkTarget(Path.getPathWithoutSchemeAndAuthority(f));
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        // ignore permissions
        return super.mkdirs(f, null);
    }

    /**
     * Replace the permission, owner and group of the given file status object.
     */
    private static FileStatus replacePermission(final FileStatus s) throws IOException {
        return new FileStatus(s.getLen(), s.isDirectory(), s.getReplication(), s.getBlockSize(),
            s.getModificationTime(), s.getAccessTime(), DEFAULT_PERM, DEFAULT_OWNER, DEAULT_GROUP,
            (s.isSymlink() ? s.getSymlink() : null), s.getPath());
    }
}
