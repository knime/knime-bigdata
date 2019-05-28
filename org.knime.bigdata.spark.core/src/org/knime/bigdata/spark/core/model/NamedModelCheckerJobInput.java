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
 *   Created on May 27, 2019 by bjoern
 */
package org.knime.bigdata.spark.core.model;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Bjoern Lohrmann
 */
@SparkClass
public class NamedModelCheckerJobInput extends JobInput {

    private static final String KEY_NAMED_MODEL_ID = "namedModelId";

    /**
     * Empty constructor for (de)serialization.
     */
    public NamedModelCheckerJobInput() {
    }

    /**
     * Creates a new instance.
     *
     * @param namedModelId Key/ID of the named model.
     */
    public NamedModelCheckerJobInput(final String namedModelId) {
        set(KEY_NAMED_MODEL_ID, namedModelId);
    }

    /**
     * @return The Key/ID of the named model.
     */
    public String getNamedModelId() {
        return get(KEY_NAMED_MODEL_ID);
    }
}
