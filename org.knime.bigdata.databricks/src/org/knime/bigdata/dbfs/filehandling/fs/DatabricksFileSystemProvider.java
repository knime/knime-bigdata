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
 *   2020-10-14 (Alexander Bondaletov): created
 */
package org.knime.bigdata.dbfs.filehandling.fs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.Iterator;
import java.util.Set;

import org.knime.filehandling.core.connections.base.BaseFileSystemProvider;
import org.knime.filehandling.core.connections.base.attributes.BaseFileAttributes;

/**
 * File system provider for the {@link DatabricksFileSystem}.
 *
 * @author Alexander Bondaletov
 */
public class DatabricksFileSystemProvider extends BaseFileSystemProvider<DatabricksPath, DatabricksFileSystem> {
    /**
     * Databricks DBFS URI scheme.
     */
    public static final String FS_TYPE = "dbfs";

    @Override
    protected SeekableByteChannel newByteChannelInternal(final DatabricksPath path, final Set<? extends OpenOption> options,
            final FileAttribute<?>... attrs) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected void moveInternal(final DatabricksPath source, final DatabricksPath target, final CopyOption... options)
            throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    protected void copyInternal(final DatabricksPath source, final DatabricksPath target, final CopyOption... options)
            throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    protected InputStream newInputStreamInternal(final DatabricksPath path, final OpenOption... options) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected OutputStream newOutputStreamInternal(final DatabricksPath path, final OpenOption... options) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected Iterator<DatabricksPath> createPathIterator(final DatabricksPath dir, final Filter<? super Path> filter)
            throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected void createDirectoryInternal(final DatabricksPath dir, final FileAttribute<?>... attrs) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    protected BaseFileAttributes fetchAttributesInternal(final DatabricksPath path, final Class<?> type) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected void checkAccessInternal(final DatabricksPath path, final AccessMode... modes) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    protected void deleteInternal(final DatabricksPath path) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public String getScheme() {
        return FS_TYPE;
    }

}
