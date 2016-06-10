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
package com.knime.bigdata.spark1_2.jobs.mllib.collaborativefiltering;

import java.io.Serializable;

import com.knime.bigdata.spark.core.job.SparkClass;

/**
 * a wrapper for a MatrixFactorizationModel that only contains references to the names of the named RDDs for
 * product and user features
 *
 * @author dwk
 */
@SparkClass
public class CollaborativeFilteringModel implements Serializable {

    private static final long serialVersionUID = 1L;
    private final int m_rank;
    private final String m_userFeatures;
    private final String m_productFeatures;

    /**
     * @param rank
     * @param userFeaturesRDDName
     * @param productFeaturesRDDName
     */
    CollaborativeFilteringModel(final int rank, final String userFeaturesRDDName, final String productFeaturesRDDName) {
        m_rank = rank;
        m_userFeatures = userFeaturesRDDName;
        m_productFeatures = productFeaturesRDDName;
    }



    /**
     * @return the rank
     */
    public int getRank() {
        return rank();
    }



    /**
     * @return the id of the userFeatures RDD
     */
    public String getUserFeaturesRDDID() {
        return m_userFeatures;
    }



    /**
     * @return the id of the productFeatures RDD
     */
    public String getProductFeaturesRDDID() {
        return m_productFeatures;
    }

    /**
     * @return feature rank
     */
    public int rank() {
        return m_rank;
    }
}
