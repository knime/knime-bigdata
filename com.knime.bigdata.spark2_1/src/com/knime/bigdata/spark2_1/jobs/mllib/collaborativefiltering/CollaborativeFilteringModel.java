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
 *   Created on 23.09.2015 by dwk
 */
package com.knime.bigdata.spark2_1.jobs.mllib.collaborativefiltering;

import java.io.Serializable;

import com.knime.bigdata.spark.core.job.SparkClass;

/**
 * A wrapper for a MatrixFactorizationModel/ALSModel that contains named object keys und column names for product and
 * user features.
 *
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class CollaborativeFilteringModel implements Serializable {

    private static final long serialVersionUID = 1L;
    private final String m_uid;
    private final int m_rank;
    private final String m_userFeaturesObjectName;
    private final String m_userFeaturesColumnName;
    private final String m_productFeaturesObjectName;
    private final String m_productFeaturesColumnName;

    /**
     * @param uid - unique model id or null
     * @param rank
     * @param userFeaturesObjectName
     * @param productFeaturesObjectName
     */
    CollaborativeFilteringModel(final String uid, final int rank,
            final String userFeaturesObjectName, final String userFeaturesColumnName,
            final String productFeaturesObjectName, final String productFeaturesColumnName) {
        m_uid = uid;
        m_rank = rank;
        m_userFeaturesObjectName = userFeaturesObjectName;
        m_userFeaturesColumnName = userFeaturesColumnName;
        m_productFeaturesObjectName = productFeaturesObjectName;
        m_productFeaturesColumnName = productFeaturesColumnName;
    }

    /**
     * @param rank
     * @param userFeaturesObjectName
     * @param productFeaturesObjectName
     */
    CollaborativeFilteringModel(final int rank,
            final String userFeaturesObjectName, final String userFeaturesColumnName,
            final String productFeaturesObjectName, final String productFeaturesColumnName) {
        this(null, rank, userFeaturesObjectName, userFeaturesColumnName, productFeaturesObjectName, productFeaturesColumnName);
    }

    /**
     * @return unique model id or null
     */
    public String getUid() {
        return m_uid;
    }

    /**
     * @return the rank
     */
    public int getRank() {
        return m_rank;
    }

    /**
     * @return the id of the userFeatures object
     */
    public String getUserFeaturesObjectName() {
        return m_userFeaturesObjectName;
    }

    /**
     * @return the user features column name
     */
    public String getUserFeaturesColumnName() {
        return m_userFeaturesColumnName;
    }

    /**
     * @return the id of the productFeatures object
     */
    public String getProductFeaturesObjectName() {
        return m_productFeaturesObjectName;
    }

    /**
     * @return the product features column name
     */
    public String getProductFeaturesColumnName() {
        return m_productFeaturesColumnName;
    }
}
