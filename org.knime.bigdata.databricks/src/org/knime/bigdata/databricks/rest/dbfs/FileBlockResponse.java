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
 *   Created on Jul 18, 2019 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.databricks.rest.dbfs;

/**
 * File block returned from a read request.
 *
 * @see <a href="https://docs.databricks.com/api/latest/dbfs.html#read">DBFS API</a>
 * @author Sascha Wolke, KNIME GmbH
 */
public class FileBlockResponse {

    /**
     * The number of bytes read (could be less than length if we hit end of file). This refers to number of bytes read
     * in unencoded version (response data is base64-encoded).
     */
    public long bytes_read;

    /**
     * The base64-encoded contents of the file read.
     */
    public String data;
}
