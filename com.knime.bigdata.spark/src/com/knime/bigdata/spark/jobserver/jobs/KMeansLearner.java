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
 *   Created on Feb 13, 2015 by koetter
 */
package com.knime.bigdata.spark.jobserver.jobs;

import java.io.Serializable;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.api.java.StructType;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.RDDUtils;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.jobserver.server.transformation.InvalidSchemaException;
import com.knime.bigdata.spark.jobserver.server.transformation.StructTypeBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

/**
 * runs MLlib KMeans on a given RDD, model is returned as result
 *
 * @author koetter, dwk
 */
public class KMeansLearner extends KnimeSparkJob implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final String PARAM_NUM_CLUSTERS = ParameterConstants.PARAM_INPUT
			+ "." + ParameterConstants.PARAM_NUM_CLUSTERS;
	private static final String PARAM_NUM_ITERATIONS = ParameterConstants.PARAM_INPUT
			+ "." + ParameterConstants.PARAM_NUM_ITERATIONS;
	private static final String PARAM_DATA_FILE_NAME = ParameterConstants.PARAM_INPUT
			+ "." + ParameterConstants.PARAM_DATA_PATH;
	private static final String PARAM_OUTPUT_DATA_PATH = ParameterConstants.PARAM_OUTPUT
			+ "." + ParameterConstants.PARAM_DATA_PATH;
	private static final String PARAM_COL_IDXS = ParameterConstants.PARAM_INPUT
            + "." + ParameterConstants.PARAM_COL_IDXS;

	private final static Logger LOGGER = Logger.getLogger(KMeansLearner.class
			.getName());

	/**
	 * parse parameters - there are no default values, all values are required
	 *
	 */
	@Override
	public SparkJobValidation validateWithContext(final SparkContext sc,
			final Config aConfig) {
		String msg = null;
		if (!aConfig.hasPath(PARAM_NUM_CLUSTERS)) {
			msg = "Input parameter '" + PARAM_NUM_CLUSTERS + "' missing.";
		} else {
			try {
				aConfig.getInt(PARAM_NUM_CLUSTERS);
			} catch (ConfigException e) {
				msg = "Input parameter '" + PARAM_NUM_CLUSTERS
						+ "' is not of expected type 'integer'.";
			}
		}
		if (msg == null && !aConfig.hasPath(PARAM_NUM_ITERATIONS)) {
			msg = "Input parameter '" + PARAM_NUM_ITERATIONS + "' missing.";
		} else {
			try {
				aConfig.getInt(PARAM_NUM_ITERATIONS);
			} catch (ConfigException e) {
				msg = "Input parameter '" + PARAM_NUM_ITERATIONS
						+ "' is not of expected type 'integer'.";
			}
		}
		if (msg == null && !aConfig.hasPath(PARAM_COL_IDXS)) {
            msg = "Input parameter '" + PARAM_COL_IDXS + "' missing.";
        } else {
            try {
                aConfig.getIntList(PARAM_COL_IDXS);
            } catch (ConfigException e) {
                msg = "Input parameter '" + PARAM_NUM_ITERATIONS
                        + "' is not of expected type 'integer list'.";
            }
        }

		if (msg == null && !aConfig.hasPath(PARAM_DATA_FILE_NAME)) {
			msg = "Input parameter '" + PARAM_DATA_FILE_NAME + "' missing.";
		}

		if (msg != null) {
			return ValidationResultConverter.invalid(msg);
		}
		return ValidationResultConverter.valid();
	}

	private SparkJobValidation validateInput(final Config aConfig) {
		String msg = null;
		final String key = aConfig.getString(PARAM_DATA_FILE_NAME);
		if (key == null) {
			msg = "Input parameter at port 1 is missing!";
		} else if (!validateNamedRdd(key)) {
			msg = "Input data table missing!";
		}
		if (msg != null) {
			LOGGER.severe(msg);
			return ValidationResultConverter.invalid(GenericKnimeSparkException.ERROR + msg);
		}
		return ValidationResultConverter.valid();
	}

	/**
	 * run the actual job, the result is serialized back to the client
	 */
	@Override
	public JobResult runJobWithContext(final SparkContext sc, final Config aConfig) {
		SparkJobValidation validation = validateInput(aConfig);
		if (!ValidationResultConverter.isValid(validation)) {
			return JobResult.emptyJobResult().withMessage(validation.toString());
		}
		LOGGER.log(Level.INFO, "starting kMeans job...");
		final JavaRDD<Row> rowRDD = getFromNamedRdds(aConfig
				.getString(PARAM_DATA_FILE_NAME));
		final List<Integer> colIdxs = aConfig.getIntList(PARAM_COL_IDXS);
		int[] idxs = new int[colIdxs.size()];
		int idx = 0;
		for (Integer colIdx : colIdxs) {
		    idxs[idx++] = colIdx;
        }
		//TODO: Use only the column indices when convert to vector
		final JavaRDD<Vector> inputRDD = RDDUtils.toJavaRDDOfVectorsOfSelectedIndices(rowRDD, idxs);

		final KMeansModel model = execute(sc, aConfig, inputRDD);

		JobResult res = JobResult.emptyJobResult().withMessage("OK").withObjectResult(model);

		if (aConfig.hasPath(PARAM_OUTPUT_DATA_PATH)) {
			LOGGER.log(Level.INFO, "Storing predicted data unter key: "
					+ aConfig.getString(PARAM_OUTPUT_DATA_PATH));
			JavaRDD<Row> predictedData = KMeansPredictor.predict(sc, inputRDD,
					model);
			try {
				addToNamedRdds(aConfig.getString(PARAM_OUTPUT_DATA_PATH),
						predictedData);
		        try {
		            final StructType schema = StructTypeBuilder.fromRows(predictedData.take(10)).build();
		            res = res
		                    .withTable(aConfig.getString(PARAM_DATA_FILE_NAME), schema);
		        } catch (InvalidSchemaException e) {
		            return JobResult.emptyJobResult().withMessage("ERROR: "+e.getMessage());
		        }
			} catch (Exception e) {
				LOGGER.severe("ERROR: failed to predict and store results for training data.");
				LOGGER.severe(e.getMessage());
			}
		}
		LOGGER.log(Level.INFO, "kMeans done");
		// note that with Spark 1.4 we can use PMML instead
		return res;
	}

	private KMeansModel execute(final SparkContext aContext,
			final Config aConfig, final JavaRDD<Vector> aInputData) {
		aInputData.cache();

		final int noOfCluster = aConfig.getInt(PARAM_NUM_CLUSTERS);
		final int noOfIteration = aConfig.getInt(PARAM_NUM_ITERATIONS);

		// Cluster the data into m_noOfCluster classes using KMeans
		return KMeans.train(aInputData.rdd(), noOfCluster, noOfIteration);
	}

}
