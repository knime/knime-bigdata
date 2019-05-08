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
 *   Created on Mar 4, 2016 by bjoern
 */
package org.knime.bigdata.spark.core.context.namedobjects;

import java.util.Set;

import org.knime.bigdata.spark.core.exception.KNIMESparkException;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public interface NamedObjectsController {

    /**
     * @return {@link Set} with the unique names of all named objects within the context
     * @throws KNIMESparkException
     */
    public Set<String> getNamedObjects() throws KNIMESparkException;

    /**
     * @param namedObjects the unique names of the named objects to delete
     * @throws KNIMESparkException if the deletion failed
     */
    public void deleteNamedObjects(Set<String> namedObjects) throws KNIMESparkException;

    /**
     * Returns statistics for given named object or <code>null</code>.
     *
     * @param objectName name of the named object
     * @return statistic for given named object or <code>null</code>
     */
    public <T extends NamedObjectStatistics> T getNamedObjectStatistics(final String objectName);
}
