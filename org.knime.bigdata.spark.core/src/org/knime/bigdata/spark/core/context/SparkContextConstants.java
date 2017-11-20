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
 *   Created on Apr 27, 2016 by bjoern
 */
package org.knime.bigdata.spark.core.context;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class SparkContextConstants {

    /**
     * ID of the "prepare context" job.
     */
    public static final String PREPARE_CONTEXT_JOB_ID = "PrepareContextJob";

    /**
     * ID of the "named objects" job, used to list/delete remote named objects (RDDs, DataFrames, ...)
     */
    public final static String NAMED_OBJECTS_JOB_ID = "NamedObjectsJob";

    /**
     * ID of the "fetch rows" job, used to fetch the rows of a remote named object (RDD, DataFrame, etc).
     */
    public static final String FETCH_ROWS_JOB_ID = "FetchRowsJob";

}
