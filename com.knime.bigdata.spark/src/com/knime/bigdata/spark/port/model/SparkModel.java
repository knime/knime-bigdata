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
 *   Created on Feb 12, 2015 by knime
 */
package com.knime.bigdata.spark.port.model;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.ZipEntry;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.container.ColumnRearranger;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.port.PortObjectZipInputStream;
import org.knime.core.node.port.PortObjectZipOutputStream;

/**
 * Spark model that encapsulates a learned Spark MLlib model.
 *
 * @author Tobias Koetter, KNIME.com
 * @param <M> the model
 */
public class SparkModel <M extends Serializable> {

    private static final String MODEL_ENTRY = "Model";
    private final M m_model;
    private final DataTableSpec m_tableSpec;
    private final String m_classColumnName;
    private final SparkModelInterpreter<SparkModel<M>> m_interpreter;


    /**
     * @param model the model
     * @param interperter the {@link SparkModelInterpreter}
     * @param origSpec the {@link DataTableSpec} of the original input table
     * @param classColName the name of the class column if appropriate otherwise <code>null</code>
     * @param featureColNames the names of the feature columns
     */
    public SparkModel(final M model, final SparkModelInterpreter<SparkModel<M>> interperter,
        final DataTableSpec origSpec, final String classColName, final List<String> featureColNames) {
        this(model, interperter, createLearningSpec(origSpec, classColName, featureColNames), classColName);
    }

    /**
     * @param model the model
     * @param interperter the {@link SparkModelInterpreter}
     * @param origSpec the {@link DataTableSpec} of the original input table
     * @param classColName the name of the class column if appropriate otherwise <code>null</code>
     * @param featureColNames the names of the feature columns
     */
    public SparkModel(final M model, final SparkModelInterpreter<SparkModel<M>> interperter,
        final DataTableSpec origSpec, final String classColName, final String... featureColNames) {
        this(model, interperter, createLearningSpec(origSpec, classColName, featureColNames), classColName);
    }

    /**
     * @param model the model
     * @param interperter the {@link SparkModelInterpreter}
     * @param spec the DataTableSpec of the table used to learn the model including the class column name
     * @param classColName the name of the class column if appropriate otherwise <code>null</code>
     */
    public SparkModel(final M model, final SparkModelInterpreter<SparkModel<M>> interperter,
        final DataTableSpec spec, final String classColName) {
        m_model = model;
        m_interpreter = interperter;
        m_tableSpec = spec;
        m_classColumnName = classColName;
    }

    /**
     * @param exec
     * @param in
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public SparkModel(final ExecutionMonitor exec, final PortObjectZipInputStream in)
            throws IOException {
        final ZipEntry type = in.getNextEntry();
        if (!type.getName().equals(MODEL_ENTRY)) {
            throw new IOException("Invalid zip entry");
        }
        try (final ObjectInputStream os = new ObjectInputStream(in);){
            m_classColumnName = (String)os.readObject();
            m_model = (M)os.readObject();
            m_interpreter = (SparkModelInterpreter<SparkModel<M>>)os.readObject();
            NodeSettings config = (NodeSettings)os.readObject();
            m_tableSpec = DataTableSpec.load(config);
        } catch (ClassNotFoundException | InvalidSettingsException e) {
            throw new IOException(e);
        }
    }

    /**
     * @param exec
     * @param out
     * @throws IOException
     */
    public void write(final ExecutionMonitor exec, final PortObjectZipOutputStream out) throws IOException {
        out.putNextEntry(new ZipEntry(MODEL_ENTRY));
        final NodeSettings config = new NodeSettings("bla");
        m_tableSpec.save(config);
        try (final ObjectOutputStream os = new ObjectOutputStream(out)){
            os.writeObject(m_classColumnName);
            os.writeObject(getModel());
            os.writeObject(m_interpreter);
            os.writeObject(config);
        }
    }

    /**
     * @return the interpreter
     */
    public SparkModelInterpreter<SparkModel<M>> getInterpreter() {
        return m_interpreter;
    }

    /**
     * @return the spec
     */
    public SparkModelPortObjectSpec getSpec() {
        return new SparkModelPortObjectSpec(getType());
    }

    /**
     * @return the type
     */
    public String getType() {
        return m_interpreter.getModelName();
    }

    /**
     * @return the model
     */
    public M getModel() {
        return m_model;
    }

    /**
     * @return the tableSpec used to learn the mode including the class column if present
     * @see #getClassColumnName()
     */
    public DataTableSpec getTableSpec() {
        return m_tableSpec;
    }

    /**
     * @return the name of the class column or <code>null</code> if not needed
     */
    public String getClassColumnName() {
        return m_classColumnName;
    }

    /**
     * @return the name of all learning columns in the order they have been used during training
     */
    public List<String> getLearningColumnNames() {
        return getColNames(getClassColumnName());
    }

    /**
     * @return the name of all columns (learning and class columns)
     */
    public List<String> getAllColumnNames() {
        return getColNames(null);
    }

    /**
     * @return
     */
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
    public Integer[] getLearningColumnIndices(final DataTableSpec inputSpec)
            throws InvalidSettingsException {
        final DataTableSpec learnerTabel = getTableSpec();
        final String classColumnName = getClassColumnName();
        final Integer[]colIdxs;
        if (classColumnName != null) {
            colIdxs = new Integer[learnerTabel.getNumColumns() - 1];
        } else {
            colIdxs = new Integer[learnerTabel.getNumColumns()];
        }
        int idx = 0;
        for (DataColumnSpec col : learnerTabel) {
            final String colName = col.getName();
            if (colName.equals(classColumnName)) {
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

    /**
     * @return the summary of this model to use in the port tooltip
     */
    public String getSummary() {
        return m_interpreter.getSummary(this);
    }
}
