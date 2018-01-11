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
package org.knime.bigdata.spark.node.preproc.missingval.apply;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.dmg.pmml.DerivedFieldDocument.DerivedField;
import org.dmg.pmml.ExtensionDocument.Extension;
import org.dmg.pmml.PMMLDocument;
import org.knime.base.node.preproc.pmml.missingval.MissingCellHandler;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.port.data.SparkDataTable;
import org.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverter;
import org.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverterRegistry;
import org.knime.bigdata.spark.core.util.SparkIDs;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.bigdata.spark.node.preproc.missingval.SparkMissingValueHandler;
import org.knime.bigdata.spark.node.preproc.missingval.compute.SparkMissingValueJobInput;
import org.knime.bigdata.spark.node.preproc.missingval.compute.SparkMissingValueNodeModel;
import org.knime.bigdata.spark.node.preproc.missingval.handler.DoNothingMissingValueHandlerFactory;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.pmml.PMMLPortObject;
import org.knime.core.node.port.pmml.preproc.DerivedFieldMapper;

/**
 * Missing values spark apply node model.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class SparkMissingValueApplyNodeModel extends SparkNodeModel {

    private final List<String> m_warningMessage = new ArrayList<>();

    /** Default constructor */
    protected SparkMissingValueApplyNodeModel() {
        super(new PortType[]{PMMLPortObject.TYPE, SparkDataPortObject.TYPE}, new PortType[]{SparkDataPortObject.TYPE});
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 2 || inSpecs[1] == null) {
            throw new InvalidSettingsException("Please connect the second inport of the node with an RDD outport");
        }

        final SparkDataPortObjectSpec sparkPortSpec = ((SparkDataPortObjectSpec)inSpecs[1]);
        final SparkVersion version = SparkContextUtil.getSparkVersion(sparkPortSpec.getContextID());
        if (SparkVersion.V_2_0.compareTo(version) > 0) {
            throw new InvalidSettingsException("Unsupported Spark Version! This node requires at least Spark 2.0.");
        }

        return new PortObjectSpec[]{sparkPortSpec};
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        exec.setMessage("Preparing job input");

        final PMMLPortObject pmmlIn = (PMMLPortObject)inData[0];
        final SparkDataPortObject inputPort = (SparkDataPortObject)inData[1];
        final SparkContextID contextID = inputPort.getContextID();
        final DataTableSpec inputSpec = inputPort.getTableSpec();
        final KNIMEToIntermediateConverter converters[] =
            KNIMEToIntermediateConverterRegistry.getConverters(inputSpec);
        final String namedInputObject = inputPort.getData().getID();
        final String namedOutputObject = SparkIDs.createSparkDataObjectID();
        final SparkMissingValueJobInput jobInput = new SparkMissingValueJobInput(namedInputObject, namedOutputObject);

        final PMMLDocument pmmlDoc = PMMLDocument.Factory.parse(pmmlIn.getPMMLValue().getDocument());
        if (pmmlDoc.getPMML().getTransformationDictionary() == null
                || pmmlDoc.getPMML().getTransformationDictionary().getDerivedFieldList().size() == 0) {
            setWarningMessage("Empty PMML detected, returning input data frame as output data frame.");
            return new PortObject[]{ new SparkDataPortObject(inputPort.getData()) };
        }

        // create job input
        m_warningMessage.clear();
        final DerivedFieldMapper mapper = new DerivedFieldMapper(pmmlDoc);
        for (DerivedField df : pmmlDoc.getPMML().getTransformationDictionary().getDerivedFieldList()) {
            final String colName = mapper.getColumnName(df.getName());
            final int colIndex = inputSpec.findColumnIndex(colName);
            final DataColumnSpec colSpec = inputSpec.getColumnSpec(colIndex);
            final SparkMissingValueHandler handler = createHandlerForColumn(colSpec, df);
            jobInput.addColumnConfig(colSpec.getName(), handler.getJobInputColumnConfig(converters[colIndex]));
        }

        if (m_warningMessage.size() > 0) {
            setWarningMessage(StringUtils.join(m_warningMessage, "\n"));
        }

        exec.setMessage("Running Spark job");
        SparkContextUtil.getJobRunFactory(contextID, SparkMissingValueNodeModel.JOB_ID)
            .createRun(jobInput)
            .run(contextID, exec);

        final SparkDataPortObject sparkOutputPort =
            new SparkDataPortObject(new SparkDataTable(contextID, namedOutputObject, inputSpec));

        return new PortObject[]{ sparkOutputPort };
    }

    private SparkMissingValueHandler createHandlerForColumn(final DataColumnSpec spec, final DerivedField df)
            throws InvalidSettingsException {

        if (df == null) {
            return DoNothingMissingValueHandlerFactory.getInstance().createHandler(spec);
        } else {
            for (Extension ext : df.getExtensionList()) {
                if (ext.getName().equals(MissingCellHandler.CUSTOM_HANDLER_EXTENSION_NAME)) {
                    SparkMissingValueHandler handler;
                    try {
                        handler = SparkMissingValueHandler.fromPMMLExtension(spec, ext);
                    } catch (InvalidSettingsException e) {
                        handler = DoNothingMissingValueHandlerFactory.getInstance().createHandler(spec);
                        m_warningMessage.add(e.getMessage() + " Falling back to \"do nothing\" handler.");
                    }
                    return handler;
                }
            }
            if (df.getApply() != null) {
                return new PMMLApplyMissingValueHandler(spec, df);
            }
            throw new InvalidSettingsException(
                "No valid missing value replacement found in derived field for column " + spec.getName());
        }
    }
}
