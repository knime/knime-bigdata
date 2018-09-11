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
 *   Created on Sep 9, 2018 by bjoern
 */
package org.knime.bigdata.mapr.fs.wrapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public interface MaprFsRemoteFileWrapper {

    URI getURI();

    boolean exists() throws Exception;

    boolean isDirectory() throws Exception;

    InputStream openInputStream() throws Exception;

    OutputStream openOutputStream() throws Exception;

    long getSize() throws Exception;

    long lastModified() throws Exception;

    boolean delete() throws Exception;

    boolean mkDir() throws Exception;

    MaprFsRemoteFileWrapper[] listFiles() throws Exception;

    void setPermission(final String unixSymbolicPermission) throws IOException;
}
