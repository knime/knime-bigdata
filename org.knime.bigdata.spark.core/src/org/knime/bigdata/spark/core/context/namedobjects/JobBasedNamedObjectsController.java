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
 *   Created on Apr 11, 2016 by bjoern
 */
package org.knime.bigdata.spark.core.context.namedobjects;

import java.util.Collections;
import java.util.HashMap;
import java.util.Set;

import org.knime.bigdata.spark.core.context.SparkContext;
import org.knime.bigdata.spark.core.context.SparkContextConstants;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextManager;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.JobRunFactoryRegistry;
import org.knime.core.node.CanceledExecutionException;

/**
 * Implements a {@link NamedObjectsController} that executes Spark jobs to list/delete named objects.
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class JobBasedNamedObjectsController implements NamedObjectsController {

    private final SparkContextID m_sparkContextID;

    private final HashMap<String, NamedObjectStatistics> m_statistics;

    /**
     * @param sparkContextId
     */
    public JobBasedNamedObjectsController(final SparkContextID sparkContextId) {
        m_sparkContextID = sparkContextId;
        m_statistics = new HashMap<>();
    }

    /**
     * {@inheritDoc}
     *
     */
    @Override
    public Set<String> getNamedObjects() throws KNIMESparkException {
        try {
            final SparkContext context = SparkContextManager.getOrCreateSparkContext(m_sparkContextID);
            return JobRunFactoryRegistry.<NamedObjectsJobInput, NamedObjectsJobOutput>getFactory(SparkContextConstants.NAMED_OBJECTS_JOB_ID, context.getSparkVersion())
                    .createRun(NamedObjectsJobInput.createListOperation())
                    .run(m_sparkContextID, null)
                    .getListOfNamedObjects();
        } catch (CanceledExecutionException e) {
            // impossible with null execution context
            return Collections.emptySet();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteNamedObjects(final Set<String> namedObjects) throws KNIMESparkException {
        try {
            final SparkContext context = SparkContextManager.getOrCreateSparkContext(m_sparkContextID);
            JobRunFactoryRegistry.<NamedObjectsJobInput, NamedObjectsJobOutput>getFactory(SparkContextConstants.NAMED_OBJECTS_JOB_ID, context.getSparkVersion())
                .createRun(NamedObjectsJobInput.createDeleteOperation(namedObjects))
                .run(m_sparkContextID, null);

            synchronized (m_statistics) {
                for (final String namedObject : namedObjects) {
                    m_statistics.remove(namedObject);
                }
            }
        } catch (CanceledExecutionException e) {
            // impossible with null execution context
        }
    }

    /**
     * Add {@link NamedObjectStatistics} of object with given name.
     * @param objectName name of object
     * @param statistic statistic of named object
     */
    public void addNamedObjectStatistics(final String objectName, final NamedObjectStatistics statistic) {
        synchronized (m_statistics) {
            m_statistics.put(objectName, statistic);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends NamedObjectStatistics> T getNamedObjectStatistics(final String objectName) {
        synchronized (m_statistics) {
            return (T) m_statistics.get(objectName);
        }
    }
}
