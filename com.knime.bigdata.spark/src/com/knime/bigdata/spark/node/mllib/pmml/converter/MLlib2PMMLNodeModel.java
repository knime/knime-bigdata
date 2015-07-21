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
package com.knime.bigdata.spark.node.mllib.pmml.converter;

import java.io.File;
import java.util.LinkedHashSet;

import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.knime.base.node.mine.cluster.PMMLClusterTranslator;
import org.knime.base.node.mine.cluster.PMMLClusterTranslator.ComparisonMeasure;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.pmml.PMMLPortObject;
import org.knime.core.node.port.pmml.PMMLPortObjectSpecCreator;

import com.knime.bigdata.spark.port.model.SparkModel;
import com.knime.bigdata.spark.port.model.SparkModelPortObject;
import com.knime.bigdata.spark.port.model.SparkModelPortObjectSpec;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class MLlib2PMMLNodeModel extends NodeModel {

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
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        final SparkModelPortObjectSpec spec = (SparkModelPortObjectSpec) inSpecs[0];
//        final PMMLPortObjectSpecCreator specCreator = new PMMLPortObjectSpecCreator(spec.get);
        return new PortObjectSpec[] {null};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final SparkModel<?> model = ((SparkModelPortObject<?>)inObjects[0]).getModel();
        final DataTableSpec learnerSpec = model.getTableSpec();
        final KMeansModel kmeansModel = (KMeansModel) model.getModel();
        PMMLPortObjectSpecCreator creator = new PMMLPortObjectSpecCreator(learnerSpec);
        creator.setLearningCols(learnerSpec);
        final PMMLPortObject outPMMLPort = new PMMLPortObject(creator.createSpec());
        Vector[] clusterCenters = kmeansModel.clusterCenters();
        final double[][] clusters = new double[clusterCenters.length][];
        for (int i = 0; i < clusterCenters.length; i++) {
            clusters[i] = clusterCenters[i].toArray();
        }
        outPMMLPort.addModelTranslater(new PMMLClusterTranslator(ComparisonMeasure.squaredEuclidean,
                clusterCenters.length, clusters, null, new LinkedHashSet<>(model.getLearningColumnNames())));
        return new PortObject[] {outPMMLPort};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {}

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) {}

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) {}

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec) {}

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec) {}

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {}
}
