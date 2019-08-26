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
 * FileInfo container.
 *
 * @see <a href="https://docs.databricks.com/api/latest/dbfs.html#fileinfo">DBFS API</a>
 * @author Sascha Wolke, KNIME GmbH
 */
public class FileInfoReponse {
    /**
     * The path of the file or directory.
     */
    public String path;

    /**
     * True if the path is a directory.
     */
    public boolean is_dir;

    /**
     * The length of the file in bytes or zero if the path is a directory.
     */
    public long file_size;
}
