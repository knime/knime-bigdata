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
 *   Created on 03.07.2015 by koetter
 */
package com.knime.bigdata.spark.node.util.context.create;

import java.util.ArrayList;
import java.util.List;

import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.core.context.SparkContext;
import com.knime.bigdata.spark.core.context.SparkContext.SparkContextStatus;
import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.context.SparkContextManager;
import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.node.SparkNodeModel;
import com.knime.bigdata.spark.core.port.context.SparkContextPortObject;
import com.knime.bigdata.spark.core.port.context.SparkContextPortObjectSpec;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
class SparkContextCreatorNodeModel extends SparkNodeModel {

    private final ContextSettings m_settings = new ContextSettings();

    /**
     * Constructor.
     */
    SparkContextCreatorNodeModel() {
        super(new PortType[]{}, new PortType[]{SparkContextPortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        configureContextIfPossible(m_settings);
        return new PortObjectSpec[]{new SparkContextPortObjectSpec(m_settings.getSparkContextID())};
    }

    private static void configureContextIfPossible(final ContextSettings settings) {
        final SparkContextID contextID = settings.getSparkContextID();
        final SparkContext sparkContext = SparkContextManager.getOrCreateSparkContext(contextID);

        if (sparkContext.getStatus() == SparkContextStatus.NEW
            || sparkContext.getStatus() == SparkContextStatus.CONFIGURED) {
            sparkContext.configure(settings.createContextConfig());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {


        final SparkContextID contextID = m_settings.getSparkContextID();
        final SparkContext sparkContext = SparkContextManager.getOrCreateSparkContext(contextID);
        final List<String> warnings = new ArrayList<>();

        if (SparkContextManager.getDefaultSparkContext() == sparkContext) {
            warnings.add(
                "Context is the same as in the Spark preferences (see File > Preferences > KNIME > Spark)");
        }

        //try to open the context
        exec.setMessage("Opening Spark context");
        configureContextIfPossible(m_settings);

        switch (sparkContext.getStatus()) {
            case NEW:
                throw new KNIMESparkException(String.format(
                    "Context is in unexpected state %s. Possible reason: Parallel changes are being made to it by another KNIME workflow.",
                    sparkContext.getStatus().toString()));
            case CONFIGURED:
                sparkContext.ensureOpened();
                break;
            case OPEN:
                warnings.add("Spark context exists already. Doing nothing and ignoring settings.");
                break;
        }

        exec.setMessage("Spark context opened");

        if (warnings.size() == 1) {
            setWarningMessage(warnings.get(0));
        } else if (warnings.size() > 1){
            StringBuilder buf = new StringBuilder("Warnings:\n");
            for (String warning : warnings) {
                buf.append("- ");
                buf.append(warning);
                buf.append('\n');
            }
            buf.deleteCharAt(buf.length() - 1);
            setWarningMessage(buf.toString());
        }

        return new PortObject[]{new SparkContextPortObject(contextID)};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadSettingsFrom(settings);
        configureContextIfPossible(m_settings);
    }

}
