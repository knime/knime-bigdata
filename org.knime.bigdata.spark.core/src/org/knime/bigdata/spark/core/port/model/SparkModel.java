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
 *   Created on May 25, 2019 by bjoern
 */
package org.knime.bigdata.spark.core.port.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.swing.JComponent;

import org.knime.bigdata.spark.core.exception.MissingSparkModelHelperException;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.node.InvalidSettingsException;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public abstract class SparkModel {

    private final Serializable m_metaData;

    /**
     * DataTableSpec of the table used to learn the model including the class column name.
     */
    private final DataTableSpec m_tableSpec;

    /**
     * Name of the target column when learning the model. May be null for unsupervised models
     */
    private final String m_targetColumnName;

    /**
     * The Spark version under which the model was created.
     */
    private final SparkVersion m_sparkVersion;

    /**
     * The name of the model e.g. MLDecisionTree.
     */
    private final String m_modelName;

    /**
     * @param metaData
     * @param tableSpec
     * @param targetColumnName
     * @param sparkVersion
     * @param modelName
     * @throws MissingSparkModelHelperException
     */
    public SparkModel(final SparkVersion sparkVersion, final String modelName, final DataTableSpec tableSpec,
        final String targetColumnName, final Serializable metaData) throws MissingSparkModelHelperException {

        m_metaData = metaData;
        m_tableSpec = tableSpec;
        m_targetColumnName = targetColumnName;
        m_sparkVersion = sparkVersion;
        m_modelName = modelName;
    }

    /**
     * @return the {@link SparkVersion} this model was created with
     */
    public SparkVersion getSparkVersion() {
        return m_sparkVersion;
    }

    /**
     * @return The model interpreter for the model.
     */
    public abstract ModelInterpreter<?> getInterpreter();

    /**
     * @return the summary of this model to use in the port tooltip
     */
    public abstract String getSummary();

    /**
     * @return the model views
     */
    public abstract JComponent[] getViews();

    /**
     * @return the corresponding {@link SparkModelPortObjectSpec}.
     */
    public SparkModelPortObjectSpec getSpec() {
        return new SparkModelPortObjectSpec(getSparkVersion(), getModelName());
    }

    /**
     * @return the unique model name, e.g. MLDecisionTree
     */
    public String getModelName() {
        return m_modelName;
    }

    /**
     * @return additional meta data for the model, or <code>null</code>
     */
    public Serializable getMetaData() {
        return m_metaData;
    }

    /**
     * @return the tableSpec used to learn the mode including the class column if present.
     *
     * @see #getTargetColumnName()
     */
    public DataTableSpec getTableSpec() {
        return m_tableSpec;
    }

    /**
     * @return the name of the optional target column.
     */
    public Optional<String> getTargetColumnName() {
        return Optional.ofNullable(m_targetColumnName);
    }

    /**
     * @return the column spec of the optional target column.
     */
    public Optional<DataColumnSpec> getTargetColumnSpec() {
        if (m_targetColumnName != null) {
            return Optional.of(m_tableSpec.getColumnSpec(m_targetColumnName));
        } else {
            return Optional.empty();
        }
    }

    /**
     * @return the name of all learning columns in the order they have been used during model training.
     */
    public List<String> getLearningColumnNames() {
        return getColNames(getTargetColumnName().orElse(null));
    }

    /**
     * @return the name of all columns (learning and class columns)
     */
    public List<String> getAllColumnNames() {
        return getColNames(null);
    }

    private List<String> getColNames(final String filterColName) {
        final List<String> colNames = new ArrayList<>(m_tableSpec.getNumColumns());
        for (DataColumnSpec dataColumnSpec : m_tableSpec) {
            final String colName = dataColumnSpec.getName();
            if (!colName.equals(filterColName)) {
                colNames.add(colName);
            }
        }
        return colNames;
    }

    /**
     * @param inputSpec the {@link DataTableSpec} to find the learning columns
     * @return the index of each learning column in the order they should be presented to the model
     * @throws InvalidSettingsException if a column is not present or has an invalid data type
     */
    public Integer[] getLearningColumnIndices(final DataTableSpec inputSpec) throws InvalidSettingsException {
        final DataTableSpec learnerTabel = getTableSpec();
        final String targetColumnName = getTargetColumnName().orElse(null);
        final Integer[] colIdxs;
        if (targetColumnName != null) {
            colIdxs = new Integer[learnerTabel.getNumColumns() - 1];
        } else {
            colIdxs = new Integer[learnerTabel.getNumColumns()];
        }
        int idx = 0;
        for (DataColumnSpec col : learnerTabel) {
            final String colName = col.getName();
            if (colName.equals(targetColumnName)) {
                //skip the class column name
                continue;
            }
            final int colIdx = inputSpec.findColumnIndex(colName);
            if (colIdx < 0) {
                throw new InvalidSettingsException("Column with name " + colName + " not found in input data");
            }
            final DataType colType = inputSpec.getColumnSpec(colIdx).getType();
            if (!colType.equals(col.getType())) {
                throw new InvalidSettingsException("Column with name " + colName + " has incompatible type expected "
                    + col.getType() + " was " + colType);
            }
            colIdxs[idx++] = Integer.valueOf(colIdx);
        }
        return colIdxs;
    }
}
