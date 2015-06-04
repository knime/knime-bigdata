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
 *   Created on 04.06.2015 by Dietrich
 */
package com.knime.bigdata.spark.jobserver.server;

import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;

/**
 *
 * @author dwk
 */
public class LabeledDataInfo {

    private final JavaRDD<LabeledPoint> m_labeledPointRDD;
    private final Map<String, Integer> m_classLabelToIntMapping;
	private final JavaRDD<Vector> m_featureRDD;

    /**
     * stores references to given args, no copies are made
     * @param aLabeledPointRDD
     * @param aFeatureData - features of the labeled point rdd
     * @param aClassLabelToIntMapping
     */
    public LabeledDataInfo(final JavaRDD<LabeledPoint> aLabeledPointRDD, final JavaRDD<Vector> aFeatureData, final Map<String, Integer> aClassLabelToIntMapping) {
        m_labeledPointRDD = aLabeledPointRDD;
        m_classLabelToIntMapping = aClassLabelToIntMapping;
        m_featureRDD = aFeatureData;
    }

    /**
     * @return RDD with labeled points
     */
    public JavaRDD<LabeledPoint> getLabeledPointRDD() {
        return m_labeledPointRDD;
    }

    /**
     * @return Vector part of the labeled point RDD
     */
    public JavaRDD<Vector> getVectorRDD() {
        return m_featureRDD;
    }
    /**
     * @return number of classes (aka class labels)
     */
    public int getNumberClasses() {
        return m_classLabelToIntMapping.size();
    }

    /**
     * @return mapping of String class label to integer class
     */
    public Map<String, Integer> getClassLabelToIntMapping() {
        return m_classLabelToIntMapping;
    }
}
