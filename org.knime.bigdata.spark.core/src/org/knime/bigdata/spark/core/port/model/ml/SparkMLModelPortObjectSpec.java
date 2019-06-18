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
 *   Created on May 26, 2019 by bjoern
 */
package org.knime.bigdata.spark.core.port.model.ml;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.zip.ZipEntry;

import javax.swing.JComponent;

import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.util.NonClosableInputStream;
import org.knime.core.data.util.NonClosableOutputStream;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContent;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortObjectSpecZipInputStream;
import org.knime.core.node.port.PortObjectSpecZipOutputStream;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class SparkMLModelPortObjectSpec implements PortObjectSpec {

    private static final String KEY_SPARK_VERSION = "version";

    private static final String KEY_TARGET_COLUMN_NAME = "targetColumnName";

    private final SparkVersion m_sparkVersion;

    /**
     * Table spec that contains all columns the model was trained on, including the target column (if applicable).
     */
    private final DataTableSpec m_tableSpec;

    /**
     * The model type.
     */
    private final MLModelType m_modelType;

    private final String m_targetColumnName;

    /**
     * @param sparkVersion
     * @param modelType
     * @param tableSpec Table spec that contains all columns the model was trained on, including the target column (if
     *            applicable).
     * @param targetColumn
     */
    public SparkMLModelPortObjectSpec(final SparkVersion sparkVersion, final MLModelType modelType,
        final DataTableSpec tableSpec, final String targetColumn) {
        m_sparkVersion = sparkVersion;
        m_modelType = modelType;
        m_tableSpec = tableSpec;
        m_targetColumnName = targetColumn;
    }

    /**
     * @return the spark version the model was learned with
     */
    public SparkVersion getSparkVersion() {
        return m_sparkVersion;
    }

    /**
     * @return the type of the model-
     */
    public MLModelType getModelType() {
        return m_modelType;
    }

    /**
     * @return table spec that contains all columns the model was trained on, including the target column (if
     *         applicable).
     */
    public DataTableSpec getTableSpec() {
        return m_tableSpec;
    }

    /**
     * @return the name of the optional target column the model was trained on.
     */
    public Optional<String> getTargetColumnName() {
        return Optional.ofNullable(m_targetColumnName);
    }

    /**
     * @return the column spec of the optional target column the model was trained on.
     */
    public Optional<DataColumnSpec> getTargetColumnSpec() {
        if (m_targetColumnName != null) {
            return Optional.of(m_tableSpec.getColumnSpec(m_targetColumnName));
        } else {
            return Optional.empty();
        }
    }

    /**
     * @return a table spec that only contains the feature columns that the model was trained on (not the target
     *         column).
     */
    public DataTableSpec getLearningColumnSpec() {
        final List<DataColumnSpec> learningColumns = new ArrayList<>();

        for (DataColumnSpec columnSpec : m_tableSpec) {
            if (columnSpec.getName().equals(m_targetColumnName)) {
                continue;
            }

            learningColumns.add(columnSpec);
        }
        return new DataTableSpec(learningColumns.toArray(new DataColumnSpec[learningColumns.size()]));
    }

    /**
     * @return the model type
     */
    public MLModelType getType() {
        return m_modelType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JComponent[] getViews() {
        return new JComponent[]{new SparkMLModelSpecView(this)};
    }

    /**
     * A serializer for {@link SparkMLModelPortObjectSpec}s.
     *
     * @author Bjoern Lohrmann, KNIME GmbH
     */
    public static final class Serializer extends PortObjectSpecSerializer<SparkMLModelPortObjectSpec> {

        private static final String SPARK_ML_MODEL_SPEC = "SparkMLModelSpec";

        private static final String KEY_TABLE_SPEC = "tableSpec";

        private static final String KEY_MODEL_TYPE = "modelType";

        @SuppressWarnings("resource")
        @Override
        public SparkMLModelPortObjectSpec loadPortObjectSpec(final PortObjectSpecZipInputStream in) throws IOException {
            ZipEntry ze = in.getNextEntry();

            if (!ze.getName().equals(SPARK_ML_MODEL_SPEC)) {
                throw new IOException("Key \"" + ze.getName() + "\" does not " + " match expected zip entry name \""
                    + SPARK_ML_MODEL_SPEC + "\".");
            }

            try {
                final ModelContentRO modelContent = ModelContent.loadFromXML(new NonClosableInputStream.Zip(in));
                SparkVersion sparkVersion = SparkVersion.fromString(modelContent.getString(KEY_SPARK_VERSION));
                final String targetColumnName = modelContent.getString(KEY_TARGET_COLUMN_NAME);
                final DataTableSpec tableSpec = DataTableSpec.load(modelContent.getModelContent(KEY_TABLE_SPEC));
                final MLModelType type = MLModelType.readFromModelContent(modelContent.getModelContent(KEY_MODEL_TYPE));

                return new SparkMLModelPortObjectSpec(sparkVersion, type, tableSpec, targetColumnName);
            } catch (InvalidSettingsException e) {
                throw new IOException(e);
            }
        }

        @SuppressWarnings("resource")
        @Override
        public void savePortObjectSpec(final SparkMLModelPortObjectSpec portObjectSpec,
            final PortObjectSpecZipOutputStream out) throws IOException {

            final ModelContent modelContent = new ModelContent(SPARK_ML_MODEL_SPEC);
            modelContent.addString(KEY_SPARK_VERSION, portObjectSpec.getSparkVersion().toString());
            modelContent.addString(KEY_TARGET_COLUMN_NAME, portObjectSpec.getTargetColumnName().orElse(null));
            portObjectSpec.getTableSpec().save(modelContent.addModelContent(KEY_TABLE_SPEC));
            portObjectSpec.getType().saveToModelContent(modelContent.addModelContent(KEY_MODEL_TYPE));

            out.putNextEntry(new ZipEntry(SPARK_ML_MODEL_SPEC));
            modelContent.saveToXML(new NonClosableOutputStream.Zip(out));
        }
    }
}
