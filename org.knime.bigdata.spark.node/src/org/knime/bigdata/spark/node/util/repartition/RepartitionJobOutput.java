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
 *   Created on May 7, 2019 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark.node.util.repartition;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Spark repartition job output.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class RepartitionJobOutput extends JobOutput {

    private static final String DYN_ALLOCATION = "dynamicAllocation";
    private static final String DYN_ALLOCATION_EXECUTORS = "dynamicAllocationExecutors";

    /**
     * @param executors number of executors used in calculation
     * @return Job output for spark dynamic allocation mode
     */
    public static RepartitionJobOutput dynamicAllocation(final int executors) {
        final RepartitionJobOutput output = new RepartitionJobOutput();
        output.set(DYN_ALLOCATION, true);
        output.set(DYN_ALLOCATION_EXECUTORS, executors);
        return output;
    }

    /**
     * @return Job result for static executor allocation
     */
    public static RepartitionJobOutput success() {
        final RepartitionJobOutput output = new RepartitionJobOutput();
        output.set(DYN_ALLOCATION, false);
        return output;
    }

    /**
     * @return <code>true</code> if spark uses dynamic resource allocation
     */
    public boolean isDynamicAllocation() {
        return get(DYN_ALLOCATION);
    }

    /**
     * @return number of executors used in calculation, might be <code>null</code>
     */
    public int dynAllocationExecutors() {
        return getInteger(DYN_ALLOCATION_EXECUTORS);
    }
}
