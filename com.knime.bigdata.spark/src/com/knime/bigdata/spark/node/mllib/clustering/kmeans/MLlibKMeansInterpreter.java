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

import java.util.List;

import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;

import com.knime.bigdata.spark.port.model.SparkModel;
import com.knime.bigdata.spark.port.model.interpreter.HTMLModelInterpreter;

/**
 *
 * @author koetter
 */
public class MLlibKMeansInterpreter extends HTMLModelInterpreter<SparkModel<KMeansModel>> {

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
    public String getSummary(final SparkModel<KMeansModel> model) {
        final Vector[] clusterCenters = model.getModel().clusterCenters();
        return "No of cluster centers: " + clusterCenters.length;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getHTMLDescription(final SparkModel<KMeansModel> model) {
        final Vector[] clusterCenters = model.getModel().clusterCenters();
        final StringBuilder buf = new StringBuilder();
//        buf.append("<b>No of cluster centers: </b>").append(clusterCenters.length).append("<br>");
        List<String> columnNames = model.getLearningColumnNames();
        buf.append("<table border=0>");
        buf.append("<tr>");
        buf.append("<th>Cluster</th>");
        for (String colName : columnNames) {
            buf.append("<th>").append(colName).append("</th>");
        }
        buf.append("</tr>");
        int idx = 1;
        for (Vector center : clusterCenters) {
            if (idx % 2 == 0) {
                buf.append("<tr bgcolor='#EEEEEE'>");
            } else {
                buf.append("<tr>");
            }
            buf.append("<th>").append(idx++).append("</th>");
            double[] dims = center.toArray();
            for (double dim : dims) {
                buf.append("<td>").append(dim).append("</td>");
            }
            buf.append("</tr>");
        }
        buf.append("</table>");
        return buf.toString();
    }
}
