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
 *   Created on 12.02.2015 by koetter
 */
package org.knime.bigdata.spark.node.pmml.predictor.compiled;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.util.SparkPMMLUtil;
import org.knime.bigdata.spark.node.pmml.predictor.AbstractSparkPMMLPredictorNodeModel;
import com.knime.pmml.compilation.java.compile.CompiledModelPortObject;
import com.knime.pmml.compilation.java.compile.CompiledModelPortObjectSpec;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkPMMLPredictorNodeModel extends AbstractSparkPMMLPredictorNodeModel {

    /**
     *
     */
    public SparkPMMLPredictorNodeModel() {
        super(new PortType[]{CompiledModelPortObject.TYPE, SparkDataPortObject.TYPE},
            new PortType[]{SparkDataPortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected DataTableSpec getResultSpec(final PortObjectSpec pmmlSpec, final SparkDataPortObjectSpec sparkSpec,
        final String predColName, final boolean outputProbabilities, final String suffix) {
        final CompiledModelPortObjectSpec cms = (CompiledModelPortObjectSpec) pmmlSpec;
        final DataTableSpec resultSpec = SparkPMMLUtil.createPredictionResultSpec(sparkSpec.getTableSpec(), cms,
            predColName, outputProbabilities, suffix);
        return resultSpec;
    }

    @Override
    protected CompiledModelPortObject getCompiledModel(final PortObject inObject) {
        final CompiledModelPortObject pmml = (CompiledModelPortObject)inObject;
        return pmml;
    }
}
