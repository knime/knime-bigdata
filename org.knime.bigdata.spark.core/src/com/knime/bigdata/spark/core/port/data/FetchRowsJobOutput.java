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
 *   Created on 08.02.2016 by koetter
 */
package com.knime.bigdata.spark.core.port.data;

import java.io.Serializable;
import java.util.List;

import com.knime.bigdata.spark.core.job.JobOutput;
import com.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class FetchRowsJobOutput extends JobOutput {

    private static final String KEY_ROWS = "rows";

    /**
     * Returns the fetched rows. The values in the rows are from the intermediate type domain and thus are
     * {@link Serializable} objects.
     *
     * @return a list of rows (represented as lists)
     */
    public List<List<Serializable>> getRows() {
        return get(KEY_ROWS);
    }

    /**
     * Factory method.
     *
     * @param rows The rows to return. Must contain values from the intermediate type domain.
     * @return a new instance.
     */
    public static FetchRowsJobOutput create(final List<List<Serializable>> rows) {
        FetchRowsJobOutput ret = new FetchRowsJobOutput();
        ret.set(KEY_ROWS, rows);
        return ret;
    }
}
