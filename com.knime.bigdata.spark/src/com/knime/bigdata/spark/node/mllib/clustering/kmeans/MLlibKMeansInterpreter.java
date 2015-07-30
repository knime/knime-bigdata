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
 *   Created on 21.07.2015 by koetter
 */
package com.knime.bigdata.spark.node.mllib.clustering.kmeans;

import java.util.Arrays;

import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;

import com.knime.bigdata.spark.port.model.SparkModelInterpreter;

/**
 *
 * @author koetter
 */
public class MLlibKMeansInterpreter implements SparkModelInterpreter<KMeansModel> {

    private static final long serialVersionUID = 1L;

    private static volatile MLlibKMeansInterpreter instance;

    private MLlibKMeansInterpreter() {
        //avoid object creation
    }

    /**
     * Returns the only instance of this class.
     * @return the only instance
     */
    public static MLlibKMeansInterpreter getInstance() {
        if (instance == null) {
            synchronized (MLlibKMeansInterpreter.class) {
                if (instance == null) {
                    instance = new MLlibKMeansInterpreter();
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
        return "KMeans";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSummary(final KMeansModel model) {
        final Vector[] clusterCenters = model.clusterCenters();
        return "No of cluster centers: " + clusterCenters.length;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDescription(final KMeansModel model) {
        final Vector[] clusterCenters = model.clusterCenters();
        return "<b>No of cluster centers: </b>" + clusterCenters.length + "<br>"
                + "<b>Cluster centers: </b>" + convertToString(clusterCenters);
    }

    private String convertToString(final Vector[] clusterCenters) {
        final StringBuilder buf = new StringBuilder();
        buf.append("<ol>");
        for (Vector vector : clusterCenters) {
            buf.append("<li>");
            buf.append(Arrays.toString(vector.toArray()));
            buf.append("</li>");
        }
        buf.append("</ol>");
        return buf.toString();
    }

}
