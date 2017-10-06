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
 *   Created on 06.10.2017 by dwk
 */
package com.knime.bigdata.spark2_0.api;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegressionModel;

/**
 *
 * @author dwk
 */
public class PipelineUtils {

    /**
     * @param aModel PipelineModel to search for a LinearRegressionModel
     * @param aModelName
     * @return LinearRegressionModel
     * @throws RuntimeException if pipeline does not contain a RegressionModel
     */
    public static LinearRegressionModel getRegressionModelStage(final PipelineModel aModel, final String aModelName) {
        for (PipelineStage stage : aModel.stages()) {
            if (stage instanceof LinearRegressionModel) {
                return (LinearRegressionModel)stage;
            }
        }
        throw new RuntimeException("SparkModel "+aModelName + " is not of type 'ml.LinearRegressionModel'");
    }

    /**
     * @param aModel
     * @param aModelName
     * @return List of Feature Names are used be the learner
     * @throws RuntimeException if pipeline does not contain a VectorAssembler
     */
    public static List<String> getLearningFeatureNamesFromVectorAssembler(final PipelineModel aModel, final String aModelName) {
        for (PipelineStage stage : aModel.stages()) {
            if (stage instanceof VectorAssembler) {
                String[] cols = ((VectorAssembler)stage).getInputCols();
                List<String> allCols = new ArrayList<>();
                for (String col : cols) {
                    //TODO - is there a better way? Set as param on PipelineModel?
                    if (col.startsWith("feature_") && col.indexOf("_", 10) > 0) {
                        String[] cols2 = col.substring(col.indexOf("_", 10)).split("_");
                        // first non-empty elem is name of original feature, further elems are values
                        final String origFeatureName = cols2[1];
                        for (int ix = 2; ix < cols2.length; ix++) {
                            allCols.add(origFeatureName + "_" + cols2[ix]);
                        }
                    } else {
                        allCols.add(col);
                    }
                }
                return allCols;
            }
        }
        throw new RuntimeException("SparkModel "+aModelName + " does not contain a vector assembler of type 'ml.VectorAssembler'");
    }


}
