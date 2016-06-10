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
 *   Created on 16.05.2016 by koetter
 */
package com.knime.bigdata.spark.node.util.rdd.persist;

import com.knime.bigdata.spark.core.job.JobInput;
import com.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class PersistJobInput extends JobInput {

    private static final String DISK = "disk";
    private static final String MEMORY = "memory";
    private static final String OFF_HEAP = "offHeap";
    private static final String DESERIALIZED = "deserialized";
    private static final String REPLICATION = "replication";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public PersistJobInput() {}

    /**
     * @param namedInputObject
     * @param useDisk
     * @param useMemory
     * @param useOffHeap
     * @param deserialized
     * @param replication
     */
    public PersistJobInput(final String namedInputObject, final boolean useDisk, final boolean useMemory,
        final boolean useOffHeap, final boolean deserialized, final int replication) {
        addNamedInputObject(namedInputObject);
        set(DISK, useDisk);
        set(MEMORY, useMemory);
        set(OFF_HEAP,useOffHeap);
        set(DESERIALIZED, deserialized);
        set(REPLICATION, replication);
    }

    /**
     * @return <code>true</code> if disk should be used
     */
    public boolean useDisk() {
        return get(DISK);
    }

    /**
     * @return <code>true</code> if memory should be used
     */
    public boolean useMemory() {
        return get(MEMORY);
    }

    /**
     * @return <code>true</code> if off heap should be used
     */
    public boolean useOffHeap() {
        return get(OFF_HEAP);
    }

    /**
     * @return <code>true</code> if objects should be deserialized
     */
    public boolean deserialized() {
        return get(DESERIALIZED);
    }

    /**
     * @return the replication factor
     */
    public int getReplication() {
        return getInteger(REPLICATION);
    }
}
