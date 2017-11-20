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
 *   Created on 23.08.2015 by koetter
 */
package org.knime.bigdata.spark.core.job.util;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.container.ColumnRearranger;

/**
 * Stores MLlib model relevant settings e.g. class column name and feature column names.
 *
 * @author Tobias Koetter, KNIME.com
 */
public class MLlibSettings {

    private final String m_classColName;
    private final int m_classColIdx;
    private final List<String> m_featureColNames;
    private final Integer[] m_featureColIdxs;
    private DataTableSpec m_originalTableSpec;
    private Long m_numberOfClasses;
    private NominalFeatureInfo m_nominalFeatureInfo;

    /**
     * @param tableSpec
     * @param featureColIdxs
     * @param featureColNames
     * @param classColIdx
     * @param classColName
     * @param featureInfo
     * @param numberOfClasses
     *
     */
    public MLlibSettings(final DataTableSpec tableSpec, final String classColName, final int classColIdx,
        final Long numberOfClasses, final List<String> featureColNames, final Integer[] featureColIdxs,
        final NominalFeatureInfo featureInfo) {
        m_originalTableSpec = tableSpec;
        m_classColName = classColName;
        m_classColIdx = classColIdx;
        m_numberOfClasses = numberOfClasses;
        m_featureColNames = featureColNames;
        m_featureColIdxs = featureColIdxs;
        m_nominalFeatureInfo = featureInfo;
    }

    /**
     * @return the classification column name or <code>null</code> if not set
     */
    public String getClassColName() {
        return m_classColName;
    }

    /**
     * @return the index of the classification column or -1 if not set
     */
    public Integer getClassColIdx() {
        return m_classColIdx;
    }

    /**
     * @return the numberOfClasses or <code>null</code> if not available
     */
    public Long getNumberOfClasses() {
        return m_numberOfClasses;
    }

    /**
     * @return the feature column name
     */
    public List<String> getFatureColNames() {
        //TODO: RENAME
        return m_featureColNames;
    }

    /**
     * @return the feature column index
     */
    public Integer[] getFeatueColIdxs() {
        return m_featureColIdxs;
    }

    /**
     * @return the featureInfo
     */
    public NominalFeatureInfo getNominalFeatureInfo() {
        return m_nominalFeatureInfo;
    }

    /**
     * @return the original input {@link DataTableSpec} including all columns e.g. columns that are not used during
     * model learning
     */
    public DataTableSpec getOriginalTableSpec() {
        return m_originalTableSpec;
    }

    /**
     * @return the {@link DataTableSpec} which includes only model relevant columns e.g. feature and class column
     */
    public DataTableSpec getLearningTableSpec() {
        return MLlibSettings.createLearningSpec(m_originalTableSpec, m_classColName, m_featureColNames);
    }

    /**
     * @param origSpec the original {@link DataTableSpec}
     * @param classColName can be <code>null</code>
     * @param featureColNames the names of all feature columns used for model learning
     * @return the {@link DataTableSpec} that includes only the feature and class column names
     */
    public static DataTableSpec createLearningSpec(final DataTableSpec origSpec, final String classColName,
        final List<String> featureColNames) {
        final ColumnRearranger rearranger = new ColumnRearranger(origSpec);
        final List<String> retainedColName = new LinkedList<>(featureColNames);
        if (classColName != null) {
            retainedColName.add(classColName);
        }
        rearranger.keepOnly(retainedColName.toArray(new String[0]));
        final DataTableSpec learnerSpec = rearranger.createSpec();
        return learnerSpec;
    }

    /**
     * @param origSpec the original {@link DataTableSpec}
     * @param classColName can be <code>null</code>
     * @param featureColNames the names of all feature columns used for model learning
     * @return the {@link DataTableSpec} that includes only the feature and class column names
     */
    public static DataTableSpec createLearningSpec(final DataTableSpec origSpec, final String classColName,
        final String[] featureColNames) {
        return createLearningSpec(origSpec, classColName, Arrays.asList(featureColNames));
    }
}