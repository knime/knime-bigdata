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
package org.knime.bigdata.databricks.rest.files;

import static org.knime.filehandling.core.connections.FSPath.URI_SEPARATOR;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import org.knime.bigdata.databricks.rest.APIWrapper;
import org.knime.filehandling.core.connections.base.UnixStylePathUtil;

import jakarta.ws.rs.core.Response;

/**
 * Wrapper class for {@link FilesAPI} that suppresses authentication popups and encodes parameters.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
public class FilesAPIWrapper extends APIWrapper<FilesAPI> implements FilesAPI {

    /**
     * Default constructor.
     *
     * @param api the API to wrap
     */
    public FilesAPIWrapper(final FilesAPI api) {
        super(api, "files");
    }

    /**
     * Encode the parts of the path, but not the separator.
     */
    private static String encodePathParts(final String path) {
        final String[] parts = UnixStylePathUtil.getPathSplits(URI_SEPARATOR, path).stream() //
            .map(part -> URLEncoder.encode(part, StandardCharsets.UTF_8)) //
            .toArray(String[]::new);
        return URI_SEPARATOR + String.join(URI_SEPARATOR, parts);
    }

    @Override
    public void getDirectoryMetadata(final String path) throws IOException {
        invoke(() -> {
            m_api.getDirectoryMetadata(encodePathParts(path));
            return null;
        });
    }

    @Override
    public Response getFileMetadata(final String path) throws IOException {
        return invoke(() -> m_api.getFileMetadata(encodePathParts(path)));
    }

    @Override
    public FileInfoList list(final String path) throws IOException {
        return invoke(() -> m_api.list(encodePathParts(path)));
    }

    @Override
    public FileInfoList list(final String path, final String pageToken) throws IOException {
        return invoke(() -> m_api.list(encodePathParts(path), pageToken));
    }

    @Override
    public void mkdirs(final String path) throws IOException {
        invoke(() -> {
            m_api.mkdirs(encodePathParts(path));
            return null;
        });
    }

    @Override
    public void deleteDirectory(final String path) throws IOException {
        invoke(() -> {
            m_api.deleteDirectory(encodePathParts(path));
            return null;
        });
    }

    @Override
    public void deleteFile(final String path) throws IOException {
        invoke(() -> {
            m_api.deleteFile(encodePathParts(path));
            return null;
        });
    }

    @Override
    public InputStream download(final String path) throws IOException {
        return invoke(() -> m_api.download(encodePathParts(path)));
    }

    @Override
    public void upload(final String path, final boolean overwrie, final InputStream content) throws IOException {
        invoke(() -> {
            m_api.upload(encodePathParts(path), overwrie, content);
            return null;
        });
    }

}
