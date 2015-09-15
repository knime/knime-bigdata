/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
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
 *   Created on 27.08.2015 by dwk
 */
package com.knime.bigdata.spark.jobserver.client;

import java.io.File;
import java.io.Serializable;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;

/**
 * simple utility that uploads some file to the server and can clean up afterwards
 *
 *
 * @author dwk
 */
public class UploadUtil {

    private final KNIMESparkContext m_context;

    private final String m_typePrefix;

    private final File m_tmpFile;

    private String m_serverFileName;

    /**
     * @param aContext
     * @param aData
     * @param aTypePrefix
     * @throws GenericKnimeSparkException
     */
    public UploadUtil(final KNIMESparkContext aContext, final Serializable aData, final String aTypePrefix)
        throws GenericKnimeSparkException {
        m_context = aContext;
        m_typePrefix = aTypePrefix;
        m_tmpFile = JobConfig.encodeToBase64AndStoreAsFile(aData);
    }

    /**
     * performs the actual upload
     *
     * @throws GenericKnimeSparkException
     *
     */
    public void upload() throws GenericKnimeSparkException {
        m_serverFileName = DataUploader.uploadDataFile(m_context, m_tmpFile.getAbsolutePath(), m_typePrefix);
    }

    /**
     * @return name of file on server
     */
    public String getServerFileName() {
        return m_serverFileName;
    }

    /**
     * @return name of temp file on client
     */
    public String getClientFileName() {
        return m_tmpFile.getAbsolutePath();
    }

    /**
     * delete local and remote temp files
     *
     * @throws GenericKnimeSparkException
     *
     */
    public void cleanup() throws GenericKnimeSparkException {
        //delete file on server again:
        if (m_context != null) {
            DataUploader.deleteFile(m_context, m_serverFileName);
        }
        //delete temp file:
        m_tmpFile.delete();
    }

}
