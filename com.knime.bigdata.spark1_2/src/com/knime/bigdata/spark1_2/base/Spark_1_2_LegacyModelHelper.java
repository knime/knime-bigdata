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
 *   Created on Apr 14, 2016 by bjoern
 */
package com.knime.bigdata.spark1_2.base;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.URL;
import java.net.URLClassLoader;

import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.Path;
import org.osgi.framework.FrameworkUtil;

import com.knime.bigdata.spark.core.jobserver.CustomClassLoadingObjectInputStream;
import com.knime.bigdata.spark.core.model.LegacyModelHelper;
import com.knime.bigdata.spark.core.port.model.ModelInterpreter;
import com.knime.bigdata.spark.node.mllib.clustering.kmeans.MLlibKMeansNodeModel;
import com.knime.bigdata.spark.node.mllib.collaborativefiltering.MLlibCollaborativeFilteringNodeModel;
import com.knime.bigdata.spark.node.mllib.prediction.bayes.naive.MLlibNaiveBayesNodeModel;
import com.knime.bigdata.spark.node.mllib.prediction.decisiontree.MLlibDecisionTreeNodeModel;
import com.knime.bigdata.spark.node.mllib.prediction.ensemble.gradientboostedtrees.MLlibGradientBoostedTreeNodeModel;
import com.knime.bigdata.spark.node.mllib.prediction.ensemble.randomforest.MLlibRandomForestNodeModel;
import com.knime.bigdata.spark.node.mllib.prediction.linear.logisticregression.MLlibLogisticRegressionNodeFactory;
import com.knime.bigdata.spark.node.mllib.prediction.linear.regression.MLlibLinearRegressionNodeFactory;
import com.knime.bigdata.spark.node.mllib.prediction.linear.svm.MLlibSVMNodeFactory;
import com.knime.bigdata.spark1_2.api.Spark_1_2_ModelHelper;
import com.knime.bigdata.spark1_2.jobserver.server.CollaborativeFilteringModel;

/**
 * Class that helps with loading serialized Spark models from legacy workflows.
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class Spark_1_2_LegacyModelHelper extends Spark_1_2_ModelHelper implements LegacyModelHelper {

    /** Zero parameter constructor */
    public Spark_1_2_LegacyModelHelper() {
        super(LEGACY_MODEL_NAME);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ModelInterpreter getModelInterpreter() {
        throw new UnsupportedOperationException(getClass().getCanonicalName() + " does not provide a SparkModelInterpreter");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String tryToGuessModelName(final Object modelInstance) {
        if (modelInstance instanceof KMeansModel) {
            return MLlibKMeansNodeModel.MODEL_NAME;
        } else if (modelInstance instanceof CollaborativeFilteringModel) {
            return MLlibCollaborativeFilteringNodeModel.MODEL_NAME;
        } else if (modelInstance instanceof NaiveBayesModel) {
            return MLlibNaiveBayesNodeModel.MODEL_NAME;
        } else if (modelInstance instanceof DecisionTreeModel) {
            return MLlibDecisionTreeNodeModel.MODEL_NAME;
        } else if (modelInstance instanceof GradientBoostedTreesModel) {
            return MLlibGradientBoostedTreeNodeModel.MODEL_NAME;
        } else if (modelInstance instanceof RandomForestModel) {
            return MLlibRandomForestNodeModel.MODEL_NAME;
        } else if (modelInstance instanceof LogisticRegressionModel) {
            return MLlibLogisticRegressionNodeFactory.MODEL_NAME;
        } else if (modelInstance instanceof LinearRegressionModel) {
            return MLlibLinearRegressionNodeFactory.MODEL_NAME;
        } else if (modelInstance instanceof SVMModel) {
            return MLlibSVMNodeFactory.MODEL_NAME;
        } else {
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectInputStream getObjectInputStream(final InputStream in) throws IOException {
        final URL pluginURL = FileLocator.resolve(FileLocator.find(FrameworkUtil.getBundle(this.getClass()), new Path(""), null));
        final URL legacySparkUrl = new File(new File(pluginURL.getPath(), "lib"), "legacy-knime-spark-1.2.jar").toURI().toURL();
        System.out.println(legacySparkUrl.toString());

        final URLClassLoader legacyClassLoader = new URLClassLoader(new URL[] {legacySparkUrl}, this.getClass().getClassLoader());
        return new CustomClassLoadingObjectInputStream(in, legacyClassLoader);
    }
}
