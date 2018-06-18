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
 *   Created on Jan 29, 2018 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark.node.mllib.freqitemset;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Frequent item sets job input container.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class FrequentItemSetJobInput extends JobInput {
    private static final String ITEM_COLUMN = "itemColumn";
    private static final String MIN_SUPPORT = "minSupport";
    private static final String NUM_PARTITIONS = "numPartitions";

    /** Deserialization constructor */
    public FrequentItemSetJobInput() {}

    FrequentItemSetJobInput(final String inputObject, final String freqItemsOutputObject, final String itemColumn,
        final double minSupport) {

        addNamedInputObject(inputObject);
        addNamedOutputObject(freqItemsOutputObject);
        set(ITEM_COLUMN, itemColumn);
        set(MIN_SUPPORT, minSupport);
    }

    /** @return item column name */
    public String getItemColumn() {
        return get(ITEM_COLUMN);
    }

    /** @return minimum support */
    public double getMinSupport() {
        return getDouble(MIN_SUPPORT);
    }

    /** @param numPartitions number of partitions to use */
    public void setNumPartitions(final int numPartitions) {
        set(NUM_PARTITIONS, numPartitions);
    }

    /** @return <code>true</code> if number of partitions should manual defined */
    public boolean hasNumPartitions() {
        return has(NUM_PARTITIONS);
    }

    /** @return number of partitions or null (see {@link #hasNumPartitions()}) */
    public int getNumPartitions() {
        return getInteger(NUM_PARTITIONS);
    }

    /** @return object id of items input */
    public String getItemsInputObject() {
        return getFirstNamedInputObject();
    }

    /** @return object id of output */
    public String getFreqItemsOutputObject() {
        return getFirstNamedOutputObject();
    }
}
