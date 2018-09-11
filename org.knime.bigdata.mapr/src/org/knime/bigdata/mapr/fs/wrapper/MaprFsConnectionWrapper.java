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

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public interface MaprFsConnectionWrapper {
    /**
     * Open this connection.
     *
     * @throws Exception If opening failed
     */
    public abstract void open() throws IOException;

    /**
     * Checks if the connection is open.
     *
     * @return true if the connection is open, false otherwise
     */
    public abstract boolean isOpen();

    /**
     * Close this connection.
     *
     * @throws Exception If closing failed
     */
    public abstract void close() throws IOException;
}
