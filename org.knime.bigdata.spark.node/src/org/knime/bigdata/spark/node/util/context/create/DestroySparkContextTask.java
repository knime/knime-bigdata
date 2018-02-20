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
package org.knime.bigdata.spark.node.util.context.create;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextManager;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.core.node.NodeLogger;

/**
 * Background task to be used when destroying a Spark context during a node's onDispose().
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class DestroySparkContextTask implements Runnable {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(DestroySparkContextTask.class);

    private final SparkContextID m_sparkContextId;

    /**
     * Creates a new instance.
     *
     * @param sparkContextId ID of the Spark context to destroy.
     */
    public DestroySparkContextTask(final SparkContextID sparkContextId) {
        m_sparkContextId = sparkContextId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        try {
            LOGGER.debug("Destroying Spark context: " + m_sparkContextId);
            SparkContextManager.ensureSparkContextDestroyed(m_sparkContextId);
        } catch (KNIMESparkException e) {
            LOGGER.debug(e);
        }
    }
}
