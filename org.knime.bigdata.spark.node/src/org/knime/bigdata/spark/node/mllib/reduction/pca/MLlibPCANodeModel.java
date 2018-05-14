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
 *   Created on Feb 12, 2015 by knime
 */
package org.knime.bigdata.spark.node.mllib.reduction.pca;

import static org.knime.bigdata.spark.node.mllib.reduction.pca.MLlibPCASettings.TARGET_DIM_MODE_FIXED;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.util.SparkIDs;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.bigdata.spark.node.mllib.reduction.pca.PCAJobInput.Mode;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 * Spark PCA node model.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class MLlibPCANodeModel extends SparkNodeModel {

    /** The unique job id. */
    public static final String JOB_ID = MLlibPCANodeModel.class.getCanonicalName();

    private static final String PROJ_COL_PREFIX = "PCA dimension ";
    private static final String COMP_COL_PREFIX = "Component ";

    private static final String LEGACY_PROJ_COL_PREFIX = "DIM_";
    private static final String LEGACY_COMP_COL_PREFIX = "Component_";

    private final MLlibPCASettings m_settings;

    private final boolean m_legacyMode;

    /**
     * Default constructor.
     *
     * @param legacyMode <code>false</code> if we run MLlibSVDNodeFactory2
     */
    public MLlibPCANodeModel(final boolean legacyMode) {
        super(new PortType[]{SparkDataPortObject.TYPE},
            new PortType[]{SparkDataPortObject.TYPE, SparkDataPortObject.TYPE});
        m_legacyMode = legacyMode;
        m_settings = new MLlibPCASettings(legacyMode);
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || inSpecs[0] == null) {
            throw new InvalidSettingsException("Spark input data required.");
        }

        final SparkDataPortObjectSpec spec = (SparkDataPortObjectSpec)inSpecs[0];
        final DataTableSpec tableSpec = spec.getTableSpec();
        final SparkVersion sparkVersion = SparkContextUtil.getSparkVersion(spec.getContextID());
        m_settings.validateSettings(tableSpec);

        if (m_legacyMode || m_settings.getTargetDimMode().equals(TARGET_DIM_MODE_FIXED)) {
            final int noOfComponents = m_settings.getNoOfComponents();
            return new PortObjectSpec[]{
                new SparkDataPortObjectSpec(spec.getContextID(), createProjectionResultSpec(tableSpec, noOfComponents)),
                new SparkDataPortObjectSpec(spec.getContextID(), createPCMatrixResultSpec(noOfComponents))};

        } else if (SparkVersion.V_2_0.compareTo(sparkVersion) > 0) {
            throw new InvalidSettingsException(
                "Minimum information fraction to preserve requires at least Spark 2.0."
                + " Set a fixed number of dimensions instead.");

        } else {
            return new PortObjectSpec[]{null, null};
        }
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject data = (SparkDataPortObject)inObjects[0];

        exec.setMessage("Starting Spark PCA job");
        exec.checkCanceled();

        final DataTableSpec inSpec = data.getTableSpec();
        final PCAJobInput jobInput = createJobInput(data);
        final PCAJobOutput jobOutput = SparkContextUtil.<PCAJobInput, PCAJobOutput>getJobRunFactory(data.getContextID(), JOB_ID)
                .createRun(jobInput).run(data.getContextID());
        final int noOfComponents = jobOutput.getNumberOfComponents();

        exec.setMessage("park job done.");
        return new PortObject[]{
            createSparkPortObject(data, createProjectionResultSpec(inSpec, noOfComponents), jobInput.getProjectionOutputName()),
            createSparkPortObject(data, createPCMatrixResultSpec(noOfComponents), jobInput.getPCMatrixOutputName())};
    }

    private PCAJobInput createJobInput(final SparkDataPortObject dataPort) throws InvalidSettingsException {
        final String inputName = dataPort.getData().getID();
        final String pcMatrixName = SparkIDs.createSparkDataObjectID();
        final String projectionName = SparkIDs.createSparkDataObjectID();

        final boolean failOnMissingValues = m_settings.failOnMissingValues();
        final Integer featureColIdxs[] = m_settings.getFeatureColumnIdices(dataPort.getTableSpec());

        final PCAJobInput jobInput = new PCAJobInput(inputName, pcMatrixName, projectionName, featureColIdxs,
            failOnMissingValues, getOutColMode(), getProjColPrefix(), getCompColPrefix());

        if (m_legacyMode || m_settings.getTargetDimMode().equals(TARGET_DIM_MODE_FIXED)) {
            jobInput.setTargetDimensions(m_settings.getNoOfComponents());
        } else {
            jobInput.setMinQuality(m_settings.getMinQuality());
        }

        return jobInput;
    }

    private PCAJobInput.Mode getOutColMode() {
        if (m_legacyMode) {
            return Mode.LEGACY;
        } else if (m_settings.replaceInputColumns()) {
            return Mode.REPLACE_COLUMNS;
        } else {
            return Mode.APPEND_COLUMNS;
        }
    }

    private String getProjColPrefix() {
        return m_legacyMode ? LEGACY_PROJ_COL_PREFIX : PROJ_COL_PREFIX;
    }

    private String getCompColPrefix() {
        return m_legacyMode ? LEGACY_COMP_COL_PREFIX : COMP_COL_PREFIX;
    }

    /** @return projection port output spec */
    private DataTableSpec createProjectionResultSpec(final DataTableSpec inputSpec, final int noOfComponents) {
        final String prefix = getProjColPrefix();
        final List<DataColumnSpec> specs = new LinkedList<>();

        if (!m_legacyMode) {
            final HashSet<String> featureColumns = new HashSet<>(Arrays.asList(m_settings.getFeatureColumns(inputSpec)));
            for (int i = 0; i < inputSpec.getNumColumns(); i++) {
                final DataColumnSpec col = inputSpec.getColumnSpec(i);
                if (!m_settings.replaceInputColumns() || !featureColumns.contains(col.getName())) {
                    specs.add(col);
                }
            }
        }

        final DataColumnSpecCreator specCreator = new DataColumnSpecCreator("output", DoubleCell.TYPE);
        for (int i = 0; i < noOfComponents; i++) {
            specCreator.setName(String.format("%s%d", prefix, i));
            specs.add(specCreator.createSpec());
        }

        return new DataTableSpec(specs.toArray(new DataColumnSpec[0]));
    }

    /** @return principal components output port spec */
    private DataTableSpec createPCMatrixResultSpec(final int noOfComponents) {
        final String prefix = getCompColPrefix();
        final List<DataColumnSpec> specs = new LinkedList<>();
        final DataColumnSpecCreator specCreator = new DataColumnSpecCreator("output", DoubleCell.TYPE);
        for (int i = 0; i < noOfComponents; i++) {
            specCreator.setName(String.format("%s%d", prefix, i));
            specs.add(specCreator.createSpec());
        }
        return new DataTableSpec(specs.toArray(new DataColumnSpec[0]));
    }

    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsTo(settings);
    }

    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadSettingsFrom(settings);
    }
}
