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
 *   Created on 17.09.2015 by koetter
 */
package com.knime.bigdata.spark.node.mllib.collaborativefiltering;

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;

import com.knime.bigdata.spark.node.mllib.MLlibSettings;
import com.knime.bigdata.spark.port.model.SparkModel;
import com.knime.bigdata.spark.port.model.interpreter.HTMLModelInterpreter;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class MatrixFactorizationModelInterpreter extends HTMLModelInterpreter<SparkModel<MatrixFactorizationModel>> {

    /**Feature column index of the user column in the {@link MLlibSettings} object.*/
    public static final int SETTINGS_USER_COL_IDX = 0;
    /**Feature column index of the product column in the {@link MLlibSettings} object.*/
    public static final int SETTINGS_PRODUCT_COL_IDX = 1;

    private static final long serialVersionUID = 1L;

    private static volatile MatrixFactorizationModelInterpreter instance;

    private MatrixFactorizationModelInterpreter() {
        //avoid object creation
    }

    /**
     * Returns the only instance of this class.
     * @return the only instance
     */
    public static MatrixFactorizationModelInterpreter getInstance() {
        if (instance == null) {
            synchronized (MatrixFactorizationModelInterpreter.class) {
                if (instance == null) {
                    instance = new MatrixFactorizationModelInterpreter();
                }
            }
        }
        return instance;
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public String getModelName() {
        return "Matrix Factorization Model";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSummary(final SparkModel<MatrixFactorizationModel> sparkModel) {
        final MatrixFactorizationModel model = sparkModel.getModel();
        return "Rank: " + model.rank() + " Log name: " + model.logName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getHTMLDescription(final SparkModel<MatrixFactorizationModel> sparkModel) {
        final MatrixFactorizationModel model = sparkModel.getModel();
        StringBuilder buf = new StringBuilder();
        RDD<Tuple2<Object, double[]>> userFeatures = model.userFeatures();
        createHTMLDesc(buf, "User" , userFeatures);
        RDD<Tuple2<Object, double[]>> productFeatures = model.productFeatures();
        createHTMLDesc(buf, "Product" , productFeatures);
        return "Rank: " + model.rank() + " Log name: " + model.logName();
    }

    private void createHTMLDesc(final StringBuilder buf, final String type,
        final RDD<Tuple2<Object, double[]>> features) {
        Object collect = features.collect();
    }
}
