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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.api.java.Row;
import org.knime.sparkClient.jobs.ValidationResultConverter;
import org.knime.utils.RDDUtils;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ModelUtils;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
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
	public Object runJobWithContext(final SparkContext sc, final Config aConfig) {
		SparkJobValidation validation = validateInput(aConfig);
		if (!ValidationResultConverter.isValid(validation)) {
			return validation;
		}
		LOGGER.log(Level.INFO, "starting kMeans job...");
		final JavaRDD<Row> rowRDD = getFromNamedRdds(aConfig
				.getString(PARAM_DATA_FILE_NAME));
		final JavaRDD<Vector> inputRDD = RDDUtils.toJavaRDD(rowRDD);

		final KMeansModel model = execute(sc, aConfig, inputRDD);

		if (aConfig.hasPath(PARAM_OUTPUT_DATA_PATH)) {
			LOGGER.log(Level.INFO, "Storing predicted data unter key: "
					+ aConfig.getString(PARAM_OUTPUT_DATA_PATH));
			JavaRDD<Row> predictedData = KMeansPredictor.predict(sc, inputRDD,
					model);
			try {
				addToNamedRdds(aConfig.getString(PARAM_OUTPUT_DATA_PATH),
						predictedData);
			} catch (Exception e) {
				LOGGER.severe("ERROR: failed to predict and store results for training data.");
				LOGGER.severe(e.getMessage());
			}
		}
		LOGGER.log(Level.INFO, "kMeans done");
		// note that with Spark 1.4 we can use PMML instead
		return ModelUtils.toString(model);
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
