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
 *   Created on 16 ott 2015 by Stefano
 */
package org.knime.bigdata.spark1_6_cdh5_9.jobs.mllib.clustering.kmeans;

import java.util.LinkedHashSet;

import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.knime.base.node.mine.cluster.PMMLClusterTranslator;
import org.knime.base.node.mine.cluster.PMMLClusterTranslator.ComparisonMeasure;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.pmml.PMMLPortObject;
import org.knime.core.node.port.pmml.PMMLPortObjectSpecCreator;

import org.knime.bigdata.spark.core.port.model.SparkModel;
import org.knime.bigdata.spark.node.mllib.clustering.kmeans.MLlibKMeansNodeModel;
import org.knime.bigdata.spark.node.pmml.converter.PMMLPortObjectFactory;

/**
 *
 * @author Stefano Baghino <stefano.baghino@databiz.it>
 */
public class KMeansModelPMMLPortObjectFactory implements PMMLPortObjectFactory {

    @Override
    public PMMLPortObject convert(final SparkModel knimeModel) throws InvalidSettingsException {
        final DataTableSpec learnerSpec = knimeModel.getTableSpec();
        PMMLPortObjectSpecCreator creator = new PMMLPortObjectSpecCreator(learnerSpec);
        creator.setLearningCols(learnerSpec);
        PMMLPortObject pmmlOutputPort = new PMMLPortObject(creator.createSpec());
        KMeansModel model = (KMeansModel) knimeModel.getModel();
        Vector[] clusterCenters = model.clusterCenters();
        final double[][] clusters = new double[clusterCenters.length][];
        for (int i = 0; i < clusterCenters.length; i++) {
            clusters[i] = clusterCenters[i].toArray();
        }

        pmmlOutputPort.addModelTranslater(new PMMLClusterTranslator(ComparisonMeasure.squaredEuclidean,
                clusterCenters.length, clusters, null, new LinkedHashSet<>(knimeModel.getLearningColumnNames())));

        return pmmlOutputPort;

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getModelName() {
        return MLlibKMeansNodeModel.MODEL_NAME;
    }

}
