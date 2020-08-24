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
package org.knime.bigdata.spark.core.util;

import java.net.URI;

import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Interface of a spark side distributed temporary directory provider.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public interface SparkDistributedTempProvider {

    /** Optional: Temporary distributed directory */
    public static final String DISTRIBUTED_TMP_DIR_KEY = "spark.knime.dfs.tmp.dir";

    /**
     * @return Hadoop compatible URI of a distributed temporary directory
     */
    public URI getDistributedTempDirURI();
}
