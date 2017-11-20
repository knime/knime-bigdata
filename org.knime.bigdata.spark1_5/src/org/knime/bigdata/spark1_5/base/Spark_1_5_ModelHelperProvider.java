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
 *   Created on 27.04.2016 by koetter
 */
package org.knime.bigdata.spark1_5.base;

import org.knime.bigdata.spark.core.model.DefaultModelHelperProvider;
import org.knime.bigdata.spark1_5.api.Spark_1_5_CompatibilityChecker;
import org.knime.bigdata.spark1_5.jobs.mllib.clustering.kmeans.KMeansModelHelper;
import org.knime.bigdata.spark1_5.jobs.mllib.collaborativefiltering.CollaborativeFilteringModelHelper;
import org.knime.bigdata.spark1_5.jobs.mllib.prediction.bayes.naive.NaiveBayesModelHelper;
import org.knime.bigdata.spark1_5.jobs.mllib.prediction.decisiontree.DecisionTreeModelHelper;
import org.knime.bigdata.spark1_5.jobs.mllib.prediction.ensemble.gradientboostedtrees.GradientBoostedTreesModelHelper;
import org.knime.bigdata.spark1_5.jobs.mllib.prediction.ensemble.randomforest.RandomForestModelHelper;
import org.knime.bigdata.spark1_5.jobs.mllib.prediction.linear.logistic.LogisticRegressionModelHelper;
import org.knime.bigdata.spark1_5.jobs.mllib.prediction.linear.regression.LinearRegressionModelHelper;
import org.knime.bigdata.spark1_5.jobs.mllib.prediction.linear.svm.SVMModelHelper;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class Spark_1_5_ModelHelperProvider extends DefaultModelHelperProvider {

    /**
     * Constructor.
     */
    public Spark_1_5_ModelHelperProvider() {
        super(Spark_1_5_CompatibilityChecker.INSTANCE,
            new KMeansModelHelper(),
            new CollaborativeFilteringModelHelper(),
            new LinearRegressionModelHelper(),
            new LogisticRegressionModelHelper(),
            new SVMModelHelper(),
            new NaiveBayesModelHelper(),
            new DecisionTreeModelHelper(),
            new GradientBoostedTreesModelHelper(),
            new RandomForestModelHelper());
    }

}
