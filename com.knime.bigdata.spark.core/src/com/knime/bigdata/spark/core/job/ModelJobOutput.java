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
 *   Created on 29.01.2016 by koetter
 */
package com.knime.bigdata.spark.core.job;

import java.io.Serializable;

/**
 *
 * @author Tobias Koetter, KNIME.com
 * @param <M> A (serializable) MLlib model
 */
@SparkClass
public class ModelJobOutput extends JobOutput {

    private final static String KEY_MLLIB_MODEL = "model";

    /**
     * Empty constructor required by deserialization.
     */
    public ModelJobOutput() {
    }

    /**
     * Creates a job output with a MLLib model.
     *
     * @param model the MLlib model
     */
    public ModelJobOutput(final Serializable model) {
        set(KEY_MLLIB_MODEL, model);
    }

    /**
     * @return the generated Spark MLlib model
     */
    public Serializable getModel() {
        return get(KEY_MLLIB_MODEL);
    }
}
