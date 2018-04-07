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
 *   Created on Feb 11, 2018 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark2_1.jobs.mllib.freqitemset;

import java.io.Serializable;

import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.mllib.freqitemset.FrequentItemSetJobInput;

/**
 * Frequent item sets model.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class FrequentItemSetModel implements Serializable  {
    private static final long serialVersionUID = 1L;

    private final String m_frequentItemsObjectName;
    private final double m_minSupport;
    private final int m_numPartitions;

    FrequentItemSetModel(final String frequentItemsObjectName, final double minSupport, final int numPartitions) {
        m_frequentItemsObjectName = frequentItemsObjectName;
        m_minSupport = minSupport;
        m_numPartitions = numPartitions;
    }

    static FrequentItemSetModel fromJobInput(final FrequentItemSetJobInput input) {
        if (input.hasNumPartitions()) {
            return new FrequentItemSetModel(input.getFreqItemsOutputObject(), input.getMinSupport(), input.getNumPartitions());
        } else {
            return new FrequentItemSetModel(input.getFreqItemsOutputObject(), input.getMinSupport(), -1);
        }
    }

    /** @return frequent items object name */
    public String getFrequentItemsObjectName() {
        return m_frequentItemsObjectName;
    }

    /** @return the minimal support */
    public double getMinSupport() {
        return m_minSupport;
    }

    /** @return <code>true</code> if number of partitions has been set */
    public boolean hasNumPartitions() {
        return m_numPartitions > 0;
    }

    /** @return the number of partitions or -1 if not set */
    public int getNumPartitions() {
        return m_numPartitions;
    }

    /** @return human readable summary of this model */
    public String[] getSummary() {
        if (hasNumPartitions()) {
            return new String[] { "Min support: " + m_minSupport, "Number of partitions: " + m_numPartitions };
        } else {
            return new String[] { "Min support: " + m_minSupport };
        }
    }
}
