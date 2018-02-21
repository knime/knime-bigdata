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
 *   Created on Feb 21, 2018 by bjoern
 */
package org.knime.bigdata.spark.core.node;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import org.knime.bigdata.spark.core.context.SparkContext;
import org.knime.bigdata.spark.core.context.SparkContext.SparkContextStatus;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextManager;
import org.knime.bigdata.spark.core.preferences.KNIMEConfigContainer;
import org.knime.core.node.NodeLogger;

/**
 * Background task to delete Spark named objects.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class DeleteNamedObjectsTask implements Runnable {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(DeleteNamedObjectsTask.class);

    private final Map<SparkContextID, String[]> m_toDelete;

    /**
     * @param toDelete
     */
    public DeleteNamedObjectsTask(final Map<SparkContextID, String[]> toDelete) {
        m_toDelete = toDelete;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public void run() {
        final long startTime = System.currentTimeMillis();

        if (KNIMEConfigContainer.verboseLogging()) {
            LOGGER.debug("Deleting Spark data objects: " + m_toDelete);
        }

        for (final Entry<SparkContextID, String[]> e : m_toDelete.entrySet()) {
            final SparkContextID contextID = e.getKey();
            try {
                final SparkContext context = SparkContextManager.getOrCreateSparkContext(contextID);
                if (SparkContextStatus.OPEN.equals(context.getStatus())) {
                    context.deleteNamedObjects(Collections.singleton(e.getValue()));
                }
            } catch (final Throwable ex) {
                // this does not log the full exception on purpose. In large workflows
                // where the deletion fails for some reason, logging the exception results
                // in a lot of not-so-useful logspam.
                LOGGER.debug("Exception while deleting named Spark data objects for context: " + contextID
                    + " Exception: " + ex.getMessage());
            }
        }

        if (KNIMEConfigContainer.verboseLogging()) {
            final long endTime = System.currentTimeMillis();
            final long durationTime = endTime - startTime;
            LOGGER.debug("Time deleting " + m_toDelete.size() + " Spark data object(s): " + durationTime + " ms");
        }
    }
}
