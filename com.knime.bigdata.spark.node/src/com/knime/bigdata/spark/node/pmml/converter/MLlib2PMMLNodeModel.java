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
 *   Created on 12.02.2015 by koetter
 */
package com.knime.bigdata.spark.node.pmml.converter;

import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.pmml.PMMLPortObject;

import com.knime.bigdata.spark.core.node.SparkNodeModel;
import com.knime.bigdata.spark.core.port.model.SparkModelPortObject;
import com.knime.bigdata.spark.core.port.model.SparkModelPortObjectSpec;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class MLlib2PMMLNodeModel extends SparkNodeModel {

    /**
     *
     */
    public MLlib2PMMLNodeModel() {
        super(new PortType[]{SparkModelPortObject.TYPE}, new PortType[]{PMMLPortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        final SparkModelPortObjectSpec spec = (SparkModelPortObjectSpec) inSpecs[0];
        final PMMLPortObjectFactory converter = PMMLPortObjectFactoryProviderRegistry.getConverter(spec.getSparkVersion(), spec.getModelName());
        if (converter == null) {
            throw new InvalidSettingsException("Spark model " + spec.getModelName() + " not supported");
        }
        return new PortObjectSpec[] {null};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        //TODO: Add also support for preprocessing info e.g. normalization/category to number that is done in Spark
        PMMLPortObject outPMMLPort = PMMLPortObjectFactoryProviderRegistry.create((SparkModelPortObject)inObjects[0]);
        return new PortObject[] {outPMMLPort};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {}

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) {}

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) {}

}
