package org.knime.bigdata.dbfs.filehandler;

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

import java.io.IOException;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.bigdata.databricks.rest.DatabricksRESTClient;
import org.knime.bigdata.databricks.rest.dbfs.AddBlockRequest;
import org.knime.bigdata.databricks.rest.dbfs.CloseRequest;
import org.knime.bigdata.databricks.rest.dbfs.CreateRequest;
import org.knime.bigdata.databricks.rest.dbfs.DBFSAPI;
import org.knime.bigdata.databricks.rest.dbfs.DeleteRequest;
import org.knime.bigdata.databricks.rest.dbfs.FileBlockResponse;
import org.knime.bigdata.databricks.rest.dbfs.FileInfoListResponse;
import org.knime.bigdata.databricks.rest.dbfs.FileInfoReponse;
import org.knime.bigdata.databricks.rest.dbfs.MkdirRequest;
import org.knime.bigdata.databricks.rest.dbfs.MoveRequest;
import org.knime.core.util.KnimeEncryption;

/**
 * DBFS connection implementation.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class DBFSConnection extends Connection {

    private DBFSAPI m_dbfsApi;

    private final ConnectionInformation m_connectionInformation;

    /**
     * @param connectionInformation the {@link ConnectionInformation} to use
     */
    public DBFSConnection(final ConnectionInformation connectionInformation) {
        m_connectionInformation = connectionInformation;
    }

    synchronized boolean delete(final String path, final boolean recursive) throws IOException {
        final DeleteRequest delete = new DeleteRequest();
        delete.path = path;
        delete.recursive = recursive;
        m_dbfsApi.delete(delete);
        return true;
    }

    synchronized void move(final String source, final String destination) throws IOException {
        final MoveRequest move = new MoveRequest();
        move.source_path = source;
        move.destination_path = destination;
        m_dbfsApi.move(move);
    }

    synchronized boolean mkDir(final String path) throws IOException {
        final MkdirRequest mkdir = new MkdirRequest();
        mkdir.path = path;
        m_dbfsApi.mkdirs(mkdir);
        return true;
    }

    synchronized FileBlockResponse readBlock(final String path, final long offset, final long length) throws IOException {
        return m_dbfsApi.read(path, offset, length);
    }

    synchronized long createHandle(final String path, final boolean overwrite) throws IOException {
        final CreateRequest createReq = new CreateRequest();
        createReq.path = path;
        createReq.overwrite = overwrite;
        return m_dbfsApi.create(createReq).handle;
    }

    synchronized void addBlock(final long handle, final String data) throws IOException {
        final AddBlockRequest addBlockReq = new AddBlockRequest();
        addBlockReq.handle = handle;
        addBlockReq.data = data;
        m_dbfsApi.addBlock(addBlockReq);
    }

    synchronized void closeHandle(final long handle) throws IOException {
        final CloseRequest closeReq = new CloseRequest();
        closeReq.handle = handle;
        m_dbfsApi.close(closeReq);
    }

    synchronized FileInfoReponse getFileInfo(final String path) throws IOException {
        return m_dbfsApi.getStatus(path);
    }

    synchronized FileInfoReponse[] listFiles(final String path) throws IOException {
        final FileInfoListResponse resp = m_dbfsApi.list(path);
        final FileInfoReponse[] files = resp.files;
        return files != null ? files : new FileInfoReponse[0];
    }

    @Override
    public synchronized void open() throws Exception {
        // token based authentication
        if (m_dbfsApi == null && m_connectionInformation.useToken()) {
            m_dbfsApi = DatabricksRESTClient.create(
                "https://" + m_connectionInformation.getHost() + ":" + m_connectionInformation.getPort(),
                DBFSAPI.class,
                KnimeEncryption.decrypt(m_connectionInformation.getToken()),
                m_connectionInformation.getTimeout(),
                m_connectionInformation.getTimeout());

        // user+password based authentication
        } else if (m_dbfsApi == null) {
            m_dbfsApi = DatabricksRESTClient.create(
                "https://" + m_connectionInformation.getHost(),
                DBFSAPI.class,
                m_connectionInformation.getUser(),
                KnimeEncryption.decrypt(m_connectionInformation.getPassword()),
                m_connectionInformation.getTimeout(),
                m_connectionInformation.getTimeout());
        }
    }

    @Override
    public synchronized boolean isOpen() {
        return m_dbfsApi != null;
    }

    @Override
    public synchronized void close() throws Exception {
        DatabricksRESTClient.close(m_dbfsApi);
        m_dbfsApi = null;
    }
}
