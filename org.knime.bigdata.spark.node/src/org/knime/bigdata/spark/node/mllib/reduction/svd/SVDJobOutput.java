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
 *   Created on 09.05.2016 by koetter
 */
package org.knime.bigdata.spark.node.mllib.reduction.svd;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class SVDJobOutput extends JobOutput {

    private static final String SINGULAR_VALUES = "singularValues";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public SVDJobOutput() {}

    /**
     * @param singularValues the singular values
     */
    public SVDJobOutput(final double[] singularValues) {
        set(SINGULAR_VALUES, singularValues);
    }

    /**
     * @return the singular values
     */
    public double[] getSingularValues() {
        return get(SINGULAR_VALUES);
    }
}
