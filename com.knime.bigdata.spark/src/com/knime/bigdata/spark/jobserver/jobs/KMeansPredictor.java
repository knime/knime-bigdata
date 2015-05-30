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
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.api.java.StructType;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ModelUtils;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.RDDUtils;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.jobserver.server.transformation.InvalidSchemaException;
import com.knime.bigdata.spark.jobserver.server.transformation.StructTypeBuilder;
import com.typesafe.config.Config;

/**
 * applies previously learned MLlib KMeans model to given RDD, predictions are
 * inserted into a new RDD and (temporarily) stored in the map of named RDDs,
 * optionally saved to disk
 *
 * @author koetter, dwk
 */
public class KMeansPredictor extends KnimeSparkJob implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final String PARAM_OUTPUT_DATA_PATH = ParameterConstants.PARAM_OUTPUT
			+ "." + ParameterConstants.PARAM_TABLE_1;

	private static final String PARAM_DATA_FILE_NAME = ParameterConstants.PARAM_INPUT
			+ "." + ParameterConstants.PARAM_TABLE_1;

	private static final String PARAM_MODEL = ParameterConstants.PARAM_INPUT
			+ "." + ParameterConstants.PARAM_MODEL_NAME;

	private final static Logger LOGGER = Logger.getLogger(KMeansPredictor.class
			.getName());

	/**
	 * parse parameters - there are no default values, but two required values:
	 * - the kmeans model - the input JavaRDD
	 */
	@Override
	public SparkJobValidation validate(final Config aConfig) {
		String msg = null;
		if (!aConfig.hasPath(PARAM_DATA_FILE_NAME)) {
			msg = "Input parameter '" + PARAM_DATA_FILE_NAME + "' missing.";
		}
		if (msg == null && !aConfig.hasPath(PARAM_OUTPUT_DATA_PATH)) {
			msg = "Output parameter '" + PARAM_OUTPUT_DATA_PATH + "' missing.";
		}
		if (msg == null && !aConfig.hasPath(PARAM_MODEL)) {
			msg = "Input model 'kmeans' missing!";
		}
		if (msg != null) {
			return ValidationResultConverter.invalid(msg);
		}

		return ValidationResultConverter.valid();
	}

	private void validateInput(final Config aConfig) throws GenericKnimeSparkException  {
		String msg = null;
		if (!validateNamedRdd(aConfig.getString(PARAM_DATA_FILE_NAME))) {
			msg = "Input data table missing!";
		}
		if (msg != null) {
			LOGGER.severe(msg);
            throw new GenericKnimeSparkException(GenericKnimeSparkException.ERROR + ":" + msg);
		}
	}

	/**
	 * run the actual job, the result is serialized back to the client
	 *
	 * @return "OK" - the actual predictions are stored in a named rdd and can
	 *         be retrieved by a separate job or used later on
	 * @throws GenericKnimeSparkException
	 */
	@Override
	public JobResult runJobWithContext(final SparkContext sc, final Config aConfig) throws GenericKnimeSparkException {
		validateInput(aConfig);

		LOGGER.log(Level.INFO, "starting kMeans prediction job...");
		final JavaRDD<Row> rowRDD = getFromNamedRdds(aConfig
				.getString(PARAM_DATA_FILE_NAME));
		final JavaRDD<Vector> inputRDD = RDDUtils.toJavaRDDOfVectors(rowRDD);

		final KMeansModel kMeansModel = ModelUtils.fromString(aConfig
				.getString(PARAM_MODEL));

		final JavaRDD<Row> predictions = predict(sc, inputRDD, kMeansModel);

		LOGGER.log(Level.INFO, "kMeans prediction done");
		addToNamedRdds(aConfig.getString(PARAM_OUTPUT_DATA_PATH), predictions);
        try {
            final StructType schema = StructTypeBuilder.fromRows(predictions.take(10)).build();
            return JobResult.emptyJobResult().withMessage("OK").withTable(aConfig.getString(PARAM_DATA_FILE_NAME), schema);
        } catch (InvalidSchemaException e) {
            return JobResult.emptyJobResult().withMessage("ERROR: "+e.getMessage());
        }
	}

	static JavaRDD<Row> predict(final SparkContext aContext,
			final JavaRDD<Vector> aInputData, final KMeansModel aModel) {
		aInputData.cache();

		final JavaRDD<Integer> predictions = aModel.predict(aInputData);

		final JavaRDD<Row> predictedData = RDDUtils.toJavaRDDOfRows(aInputData
				.zip(predictions));

		return predictedData;
	}
}
