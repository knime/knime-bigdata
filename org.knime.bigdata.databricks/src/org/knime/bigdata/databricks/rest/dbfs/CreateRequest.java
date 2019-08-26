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
 *   Created on Jul 22, 2019 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.databricks.rest.dbfs;

/**
 * Create file request container.
 *
 * @see <a href="https://docs.databricks.com/api/latest/dbfs.html#create">DBFS API</a>
 * @author Sascha Wolke, KNIME GmbH
 */
public class CreateRequest {

    /**
     * The path of the new file. The path should be the absolute DBFS path (e.g. /mnt/foo.txt).
     */
    public String path;

    /**
     * The flag that specifies whether to overwrite existing file/files.
     */
    public boolean overwrite;
}
