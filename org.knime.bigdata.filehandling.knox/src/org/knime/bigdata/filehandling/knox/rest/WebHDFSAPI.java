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
 *   Nov 12, 2019 (Sascha Wolke, KNIME GmbH): created
 */
package org.knime.bigdata.filehandling.knox.rest;

import java.io.IOException;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import org.apache.hadoop.hdfs.web.resources.DeleteOpParam;
import org.apache.hadoop.hdfs.web.resources.GetOpParam;
import org.apache.hadoop.hdfs.web.resources.PutOpParam;

/**
 * Parts of the <a href="https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html">Apache Hadoop WebHDFS API</a>.
 *
 * @author Sascha Wolke, KNIME GmbH
 * @see <a href="https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html">Apache Hadoop WebHDFS API</a>
 */
@SuppressWarnings("javadoc")
public interface WebHDFSAPI {

    @GET
    @Path("{path}")
    FileStatus getFileStatus(@PathParam("path") String path, @QueryParam("op") GetOpParam.Op op) throws IOException;

    @GET
    @Path("{path}")
    FileStatuses listStatus(@PathParam("path") String path, @QueryParam("op") GetOpParam.Op op) throws IOException;

    @GET
    org.apache.hadoop.fs.Path getHomeDirectory(@QueryParam("op") GetOpParam.Op op) throws IOException;

    @PUT
    @Path("{path}")
    boolean mkdirs(@PathParam("path") String path, @QueryParam("op") PutOpParam.Op op) throws IOException;

    @PUT
    @Path("{path}")
    boolean rename(@PathParam("path") String path, @QueryParam("op") PutOpParam.Op op,
        @QueryParam("destination") String destination) throws IOException;

    @PUT
    @Path("{path}")
    void setPermission(@PathParam("path") String path, @QueryParam("op") PutOpParam.Op op,
        @QueryParam("permission") short permission) throws IOException;

    @DELETE
    @Path("{path}")
    boolean delete(@PathParam("path") String path, @QueryParam("op") DeleteOpParam.Op op,
        @QueryParam("recursive") boolean recursive) throws IOException;

    @GET
    @Path("{path}")
    Response open(@PathParam("path") String path, @QueryParam("op") GetOpParam.Op op,
        @QueryParam("buffersize") long buffersize);

    @PUT
    @Path("{path}")
    Response create(@PathParam("path") String path, @QueryParam("op") PutOpParam.Op op,
        @QueryParam("buffersize") long buffersize, @QueryParam("overwrite") boolean overwrite);
}
