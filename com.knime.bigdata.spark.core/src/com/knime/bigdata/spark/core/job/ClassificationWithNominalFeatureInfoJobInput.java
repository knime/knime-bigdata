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
 *   Created on 08.05.2016 by koetter
 */
package com.knime.bigdata.spark.core.job;

import java.util.Arrays;
import java.util.List;

import com.knime.bigdata.spark.core.job.util.NominalFeatureInfo;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
 @SparkClass
public class ClassificationWithNominalFeatureInfoJobInput extends ClassificationJobInput {

     /** Number of classes. **/
     private static final String NO_OF_CLASSES = "NumberOfClasses";

     /**array with the indices of the nominal columns*/
     private static final String NOMINAL_FEATURE_INFO = "NominalFeatureInfo";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public ClassificationWithNominalFeatureInfoJobInput() {}

    /**
     * @param namedInputObject the input data to classify
     * @param nominalFeatureInfo {@link NominalFeatureInfo} if available
     * @param noOfClasses the number of class values or <code>null</code> if unknown
     * @param classColIdx the class column index starting with 0
     * @param featureColIdxs the feature column indices starting with 0
     */
    public ClassificationWithNominalFeatureInfoJobInput(final String namedInputObject,
        final NominalFeatureInfo nominalFeatureInfo, final Long noOfClasses, final Integer classColIdx, final List<Integer> featureColIdxs) {
        super(namedInputObject, classColIdx, featureColIdxs);
        set(NOMINAL_FEATURE_INFO, nominalFeatureInfo);
        set(NO_OF_CLASSES, noOfClasses);
    }

    /**
     * @param namedInputObject the input data to classify
     * @param nominalFeatureInfo {@link NominalFeatureInfo} if available
     * @param noOfClasses the number of class values or <code>null</code> if unknown
     * @param classColIdx the class column index starting with 0
     * @param featureColIdxs the feature column indices starting with 0
     */
    public ClassificationWithNominalFeatureInfoJobInput(final String namedInputObject,
        final NominalFeatureInfo nominalFeatureInfo, final Long noOfClasses, final Integer classColIdx, final Integer... featureColIdxs) {
        this(namedInputObject, nominalFeatureInfo, noOfClasses, classColIdx, Arrays.asList(featureColIdxs));
    }

    /**
     * @return the number of classes or <code>null</code> if unknown
     */
    public Long getNoOfClasses() {
        return getLong(NO_OF_CLASSES);
    }

    /**
     * @return {@link NominalFeatureInfo} or the empty {@link NominalFeatureInfo#EMPTY} if the information is not
     * available
     */
    public NominalFeatureInfo getNominalFeatureInfo() {
        final NominalFeatureInfo info = get(NOMINAL_FEATURE_INFO);
        if (info != null) {
            return info;
        }
        //we assume that there are no nominal (input) features
        return NominalFeatureInfo.EMPTY;
    }
}
