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
 */
package org.knime.bigdata.spark.node.preproc.missingval.compute;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.knime.base.node.preproc.pmml.missingval.MVIndividualSettings;
import org.knime.base.node.preproc.pmml.missingval.MVSettings;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.job.JobRunFactory;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.port.data.SparkDataTable;
import org.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverter;
import org.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverterParameter;
import org.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverterRegistry;
import org.knime.bigdata.spark.core.util.SparkIDs;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.bigdata.spark.node.preproc.missingval.SparkMissingValueHandler;
import org.knime.bigdata.spark.node.preproc.missingval.SparkMissingValueHandlerFactory;
import org.knime.bigdata.spark.node.preproc.missingval.SparkMissingValueSettings;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.pmml.PMMLPortObject;
import org.knime.core.node.port.pmml.PMMLPortObjectSpecCreator;

/**
 * Missing values spark node model.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class SparkMissingValueNodeModel extends SparkNodeModel {

    private MVSettings m_settings = new SparkMissingValueSettings();

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkMissingValueNodeModel.class.getCanonicalName();

    /** Default constructor */
    protected SparkMissingValueNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE}, new PortType[]{SparkDataPortObject.TYPE, PMMLPortObject.TYPE});
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || !(inSpecs[0] instanceof SparkDataPortObjectSpec)) {
            throw new InvalidSettingsException("Please connect the first inport of the node with an RDD outport");
        }

        final SparkDataPortObjectSpec sparkPortSpec = ((SparkDataPortObjectSpec)inSpecs[0]);
        final DataTableSpec inputSpec = sparkPortSpec.getTableSpec();
        m_settings.configure(inputSpec);

        final SparkVersion version = SparkContextUtil.getSparkVersion(sparkPortSpec.getContextID());
        if (SparkVersion.V_2_0.compareTo(version) > 0) {
            throw new InvalidSettingsException("Unsupported Spark Version! This node requires at least Spark 2.0.");
        }

        final DataTableSpec resultSpec = createOutputSpec(inputSpec);
        final PMMLPortObjectSpecCreator pmmlSpecCreator = new PMMLPortObjectSpecCreator(inputSpec);

        return new PortObjectSpec[]{new SparkDataPortObjectSpec(sparkPortSpec.getContextID(), resultSpec),
            pmmlSpecCreator.createSpec()};
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        exec.setMessage("Preparing job input");
        final SparkDataPortObject inputPort = (SparkDataPortObject)inData[0];
        final SparkContextID contextID = inputPort.getContextID();
        final DataTableSpec inputSpec = inputPort.getTableSpec();
        final KNIMEToIntermediateConverter converters[] = new KNIMEToIntermediateConverter[inputSpec.getNumColumns()];
        final KNIMEToIntermediateConverterParameter converterParameter =
            SparkContextUtil.getConverterParameter(contextID);
        final String namedInputObject = inputPort.getData().getID();
        final String namedOutputObject = SparkIDs.createSparkDataObjectID();
        final SparkMissingValueJobInput jobInput = new SparkMissingValueJobInput(namedInputObject, namedOutputObject);
        final SparkMissingValueHandler mvHandler[] = new SparkMissingValueHandler[inputSpec.getNumColumns()];
        boolean validPMML = true;

        final DataTableSpec outputSpec = createOutputSpec(inputSpec);

        // create job input
        for (int i = 0; i < inputSpec.getNumColumns(); i++) {
            final DataColumnSpec inputColSpec = inputSpec.getColumnSpec(i);
            final DataColumnSpec outputColSpec = outputSpec.getColumnSpec(i);

            final SparkMissingValueHandler handler = getSparkMissingValueHandler(inputColSpec);

            converters[i] = KNIMEToIntermediateConverterRegistry.get(outputColSpec.getType());
            final Map<String, Serializable> colConfig =
                handler.getJobInputColumnConfig(converters[i], converterParameter);

            if (!inputColSpec.getType().equals(outputColSpec.getType())) {
                SparkMissingValueJobInput.addCastConfig(colConfig, converters[i].getIntermediateDataType());
            }

            jobInput.addColumnConfig(inputColSpec.getName(), colConfig);
            mvHandler[i] = handler;
            validPMML &= handler.isPMML4_2Compatible();
        }

        if (!validPMML) {
            setWarningMessage(
                "The current settings use missing value handling methods that cannot be represented in PMML 4.2.");
        }

        final SparkDataPortObject sparkOutputPort;
        final Map<String, Object> outputValues = new HashMap<>();

        if (jobInput.isEmtpy()) {
            setWarningMessage("No changes to the input data were made, because the provided missing value replacements did not apply to any of the input columns.");
            setAutomaticSparkDataHandling(false);
            sparkOutputPort = new SparkDataPortObject(inputPort.getData());

        } else {
            exec.setMessage("Running Spark job");
            final JobRunFactory<SparkMissingValueJobInput, SparkMissingValueJobOutput> factory =
                SparkContextUtil.getJobRunFactory(contextID, JOB_ID);
            final SparkMissingValueJobOutput jobOutput = factory.createRun(jobInput).run(contextID, exec);
            setAutomaticSparkDataHandling(true);
            sparkOutputPort =
                new SparkDataPortObject(new SparkDataTable(contextID, namedOutputObject, outputSpec));

            // convert fixed values (including aggregation results)
            final Map<String, Serializable> intermediateOutput = jobOutput.getValues();
            for (Entry<String, Serializable> e : intermediateOutput.entrySet()) {
                KNIMEToIntermediateConverter converter = converters[inputSpec.findColumnIndex(e.getKey())];
                outputValues.put(e.getKey(), converter.convert(e.getValue(), converterParameter));
            }
        }

        exec.setMessage("Generating PMML");
        PMMLMissingValueReplacementTranslator pmmlTranslator =
            new PMMLMissingValueReplacementTranslator(mvHandler, outputValues);
        PMMLPortObject pmmlOutputPort = new PMMLPortObject(new PMMLPortObjectSpecCreator(inputSpec).createSpec());
        pmmlOutputPort.addModelTranslater(pmmlTranslator);

        return new PortObject[]{sparkOutputPort, pmmlOutputPort};
    }

    private DataTableSpec createOutputSpec(final DataTableSpec inputSpec) throws InvalidSettingsException {
        final DataColumnSpec outputColSpec[] = new DataColumnSpec[inputSpec.getNumColumns()];

        for (int i = 0; i < inputSpec.getNumColumns(); i++) {
            final DataColumnSpec colSpec = inputSpec.getColumnSpec(i);
            final SparkMissingValueHandler handler = getSparkMissingValueHandler(colSpec);

            DataColumnSpecCreator specCreator = new DataColumnSpecCreator(colSpec);
            specCreator.setType(handler.getOutputDataType());
            outputColSpec[i] = specCreator.createSpec();
        }

        return new DataTableSpec(outputColSpec);
    }

    private SparkMissingValueHandler getSparkMissingValueHandler(final DataColumnSpec inputColSpec)
        throws InvalidSettingsException {

        final MVIndividualSettings colSetting = m_settings.getSettingsForColumn(inputColSpec);
        final SparkMissingValueHandlerFactory factory = (SparkMissingValueHandlerFactory)colSetting.getFactory();
        final SparkMissingValueHandler handler = factory.createHandler(inputColSpec);
        handler.loadSettingsFrom(colSetting.getSettings());
        return handler;
    }

    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveToSettings(settings);
    }

    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadSettings(settings, false);
    }
}
