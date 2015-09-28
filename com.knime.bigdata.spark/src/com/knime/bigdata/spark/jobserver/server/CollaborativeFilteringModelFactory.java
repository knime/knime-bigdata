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
 *   Created on 23.09.2015 by dwk
 */
package com.knime.bigdata.spark.jobserver.server;

import org.apache.spark.SparkContext;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;

/**
 *
 * @author dwk
 */
public class CollaborativeFilteringModelFactory {

    /**
     * convert the 'true' model into a model that only contains references so that it can be serialized to the client
     * @param aContext
     * @param aJob - the job that created the model
     * @param serverModel - the source model
     * @return container with rank and references to the names of the named RDDs for user and product features
     */
    public static CollaborativeFilteringModel fromMatrixFactorizationModel(final SparkContext aContext, final KnimeSparkJob aJob, final MatrixFactorizationModel serverModel) {
        final String userFeaturesRDDName = "userFeatures_"+System.currentTimeMillis();
        final String productFeaturesRDDName = "productFeatures_"+System.currentTimeMillis();
        final CollaborativeFilteringModel model = new CollaborativeFilteringModel(serverModel.rank(), userFeaturesRDDName, productFeaturesRDDName);
        aJob.addToNamedRdds(userFeaturesRDDName, serverModel.userFeatures());
        aJob.addToNamedRdds(productFeaturesRDDName, serverModel.productFeatures());

        return model;
    }

    /**
     * create the true model from the knime reference model
     * @param aJob - spark job reference, required for access to the named rdds
     * @param aKnimeModel
     * @return the model that can be used for prediction
     */
    public static MatrixFactorizationModel fromCollaborativeFilteringModel(final KnimeSparkJob aJob, final CollaborativeFilteringModel aKnimeModel) {
        return new MatrixFactorizationModel(aKnimeModel.rank(), aKnimeModel.userFeatures(aJob), aKnimeModel.productFeatures(aJob));
    }

}
