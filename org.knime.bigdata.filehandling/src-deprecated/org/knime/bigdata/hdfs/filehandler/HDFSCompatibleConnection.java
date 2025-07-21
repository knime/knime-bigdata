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
 *   Created on Nov 14, 2019 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.hdfs.filehandler;

import java.io.IOException;
import java.net.URI;

import org.knime.base.filehandling.remote.files.Connection;

/**
 * A {@link Connection} with (some) HDFS compatible methods.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@Deprecated
public interface HDFSCompatibleConnection {

    /**
     * Return the current user's home directory.
     *
     * @return user's home directory.
     * @throws IOException
     * @deprecated
     */
    @Deprecated
    public URI getHomeDirectory() throws IOException;

    /**
     * Validate of given path exists.
     *
     * @param uri URI of path to check
     * @return {@code true} if path exists
     * @throws IOException
     * @deprecated
     */
    @Deprecated
    public boolean exists(final URI uri) throws IOException;

    /**
     * Set permission of a path.
     *
     * @param uri URI of path to set permission on
     * @param unixSymbolicPermission Symbolic permission (e.g. {@code -rwxr-x---})
     * @throws IOException
     * @deprecated
     */
    @Deprecated
    public void setPermission(final URI uri, final String unixSymbolicPermission) throws IOException;
}
