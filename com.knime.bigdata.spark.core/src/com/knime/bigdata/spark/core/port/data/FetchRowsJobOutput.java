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

    public List<List<Serializable>> getRows() {
        return get(KEY_ROWS);
    }

    public static FetchRowsJobOutput create(final List<List<Serializable>> rows) {
        FetchRowsJobOutput ret = new FetchRowsJobOutput();
        ret.set(KEY_ROWS, rows);
        return ret;
    }
}
