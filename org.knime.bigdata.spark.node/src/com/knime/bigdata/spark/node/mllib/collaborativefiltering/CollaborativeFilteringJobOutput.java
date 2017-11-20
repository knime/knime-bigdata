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
 *   Created on 01.05.2016 by koetter
 */
package com.knime.bigdata.spark.node.mllib.collaborativefiltering;

import java.io.Serializable;

import com.knime.bigdata.spark.core.job.ModelJobOutput;
import com.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class CollaborativeFilteringJobOutput extends ModelJobOutput {

    private final static String KEY_USER_FEATURES = "userFeatures";

    private final static String KEY_PRODUCT_FEATURE = "productFeatures";
    /**
     * Empty constructor required by deserialization.
     */
    public CollaborativeFilteringJobOutput() {}

    /**
     * @param model the model
     * @param userFeaturesObjectName the user feature named object that is stored in Spark
     * @param productFeaturesObjectname the product feature named object that is stored in Spark
     */
    public CollaborativeFilteringJobOutput(final Serializable model, final String userFeaturesObjectName,
        final String productFeaturesObjectname) {
        super(model);
        set(KEY_USER_FEATURES, userFeaturesObjectName);
        set(KEY_PRODUCT_FEATURE, productFeaturesObjectname);
    }

    /**
     * @return the unique identifier of the user features object
     */
    public String getUserFeaturesObjectName() {
        return get(KEY_USER_FEATURES);
    }

    /**
     * @return the unique identifier of the product features object
     */
    public String getProductFeaturesObjectName() {
        return get(KEY_PRODUCT_FEATURE );
    }
}
