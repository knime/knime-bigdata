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
 *   Created on 13.09.2015 by koetter
 */
package com.knime.bigdata.spark.node.util.persist;

import org.apache.spark.storage.StorageLevel;
import org.knime.core.node.util.ButtonGroupEnumInterface;

/**
 * The different storage levels supported by the Spark persist node.
 *
 * @author Tobias Koetter, KNIME.com
 */
enum PersistenceOption implements ButtonGroupEnumInterface {

    MEMORY_ONLY("Memory only", "Store RDD as deserialized Java objects in the JVM. If the RDD does not fit in memory, "
        + "some partitions will not be cached and will be recomputed on the fly each time they're needed.",
        StorageLevel.MEMORY_ONLY(), true),
    MEMORY_AND_DISK("Memory and disk", "Store RDD as deserialized Java objects in the JVM. If the RDD does not fit in "
        + "memory, store the partitions that don't fit on disk, and read them from there when they're needed.",
        StorageLevel.MEMORY_AND_DISK(), false),
    MEMORY_ONLY_SER("Memory only serialized", "Store RDD as serialized Java objects (one byte array per partition). "
        + "This is generally more space-efficient than deserialized objects, especially when using a fast serializer, "
            + "but more CPU-intensive to read.", StorageLevel.MEMORY_ONLY_SER(), false),
    MEMORY_AND_DISK_SER("Memory and disk serialized", "Similar to MEMORY_ONLY_SER, but spill partitions that don't fit "
        + "in memory to disk instead of recomputing them on the fly each time they're needed.",
        StorageLevel.MEMORY_AND_DISK_SER(), false),
    DISK_ONLY("Disk only", "", StorageLevel.DISK_ONLY(), false),
    OFF_HEAP("Off heap (experimental)",
        "Store RDD in serialized format in Tachyon. Compared to MEMORY_ONLY_SER, OFF_HEAP reduces garbage collection "
        + "overhead and allows executors to be smaller and to share a pool of memory, making it attractive in "
        + "environments with large heaps or multiple concurrent applications. Furthermore, as the RDDs reside in "
        + "Tachyon, the crash of an executor does not lead to losing the in-memory cache. In this mode, the memory in "
        + "Tachyon is discardable. Thus, Tachyon does not attempt to reconstruct a block that it evicts from memory. ",
        StorageLevel.OFF_HEAP(), false),
    CUSTOM("Custom", "Define your own storage level.", StorageLevel.NONE(), false);

    private String m_label;
    private String m_desc;
    private boolean m_isDefault;
    private boolean m_useDisk;
    private boolean m_useMemory;
    private boolean m_useOffHeap;
    private boolean m_deserialized;
    private int m_replication;

    private PersistenceOption(final String label, final String desc, final StorageLevel level, final boolean isDefault) {
        this(label, desc, isDefault, level.useDisk(), level.useMemory(), level.useOffHeap(), level.deserialized(),
            level.replication());
    }

    private PersistenceOption(final String label, final String desc, final boolean isDefault, final boolean useDisk,
        final boolean useMemory, final boolean useOffHeap, final boolean deserialized, final int replication) {
        m_label = label;
        m_desc = desc;
        m_isDefault = isDefault;
        m_useDisk = useDisk;
        m_useMemory = useMemory;
        m_useOffHeap = useOffHeap;
        m_deserialized = deserialized;
        m_replication = replication;
    }

    /**
     * @return the useDisk
     */
    public boolean useDisk() {
        return m_useDisk;
    }

    /**
     * @return the useMemory
     */
    public boolean useMemory() {
        return m_useMemory;
    }

    /**
     * @return the useOffHeap
     */
    public boolean useOffHeap() {
        return m_useOffHeap;
    }

    /**
     * @return the deserialized
     */
    public boolean isDeserialized() {
        return m_deserialized;
    }

    /**
     * @return the replication
     */
    public int getReplication() {
        return m_replication;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getText() {
        return m_label;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getActionCommand() {
        return name();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getToolTip() {
        return m_desc;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isDefault() {
        return m_isDefault;
    }

    /**
     * @param actionCommand the action command to get the {@link PersistenceOption} for
     */
    static PersistenceOption getOption(final String actionCommand) {
        return valueOf(actionCommand);
    }

    /**
     * @return the default {@link PersistenceOption}
     */
    static PersistenceOption getDefault() {
        PersistenceOption[] options = values();
        for (PersistenceOption persistenceOption : options) {
            if (persistenceOption.isDefault()) {
                return persistenceOption;
            }
        }
        return MEMORY_ONLY;
    }
}
