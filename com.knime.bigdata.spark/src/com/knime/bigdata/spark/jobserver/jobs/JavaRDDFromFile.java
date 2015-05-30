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

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.api.java.StructType;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.jobserver.server.transformation.InvalidSchemaException;
import com.knime.bigdata.spark.jobserver.server.transformation.RowBuilder;
import com.knime.bigdata.spark.jobserver.server.transformation.StructTypeBuilder;
import com.typesafe.config.Config;

/**
 * converts a text file that is read from disk to a JavaRDD
 *
 * @author dwk
 */
public class JavaRDDFromFile extends KnimeSparkJob implements Serializable {

	private static final long serialVersionUID = 1L;

	static final String PARAM_DATA_FILE_NAME = ParameterConstants.PARAM_INPUT
			+ "." + ParameterConstants.PARAM_TABLE_1;

	private final static Logger LOGGER = Logger.getLogger(JavaRDDFromFile.class
			.getName());

	/**
	 * parse parameters - there are no default values, all values are required
	 *
	 */
	@Override
	public SparkJobValidation validate(final Config config) {
		String msg = null;

		if (!config.hasPath(PARAM_DATA_FILE_NAME)) {
			msg = "Input parameter '" + PARAM_DATA_FILE_NAME + "' missing.";
		}

		// further checks - in this case we check whether the input data file
		// exists
		if (msg == null
				&& !new File(config.getString(PARAM_DATA_FILE_NAME)).exists()) {
			msg = "Input data file "
					+ new File(config.getString(PARAM_DATA_FILE_NAME))
							.getAbsolutePath() + " does not exist!";
		}
		if (msg != null) {
			return ValidationResultConverter.invalid(msg);
		}
		return ValidationResultConverter.valid();
	}

	/**
	 * run the actual job, the result is serialized back to the client
	 * the true result is stored in the map of named RDDs
	 * @return "OK"
	 */
	@Override
	public JobResult runJobWithContext(final SparkContext sc, final Config aConfig) {
		LOGGER.log(Level.INFO, "reading and converting text file...");
		final JavaRDD<Row> parsedData = javaRDDFromFile(sc, aConfig);
		LOGGER.log(Level.INFO, "done");

		LOGGER.log(Level.INFO, "Storing predicted data unter key: "+aConfig.getString(PARAM_DATA_FILE_NAME));
		LOGGER.log(Level.INFO, "Cashing data of size: "+parsedData.count());
		addToNamedRdds(aConfig.getString(PARAM_DATA_FILE_NAME), parsedData);
        try {
            final StructType schema = StructTypeBuilder.fromRows(parsedData.take(10)).build();
            return JobResult.emptyJobResult().withMessage("OK")
                    .withTable(aConfig.getString(PARAM_DATA_FILE_NAME), schema);
        } catch (InvalidSchemaException e) {
            return JobResult.emptyJobResult().withMessage("ERROR: "+e.getMessage());
        }
	}

	static JavaRDD<Row> javaRDDFromFile(final SparkContext sc,
			final Config config) {
		@SuppressWarnings("resource")
		JavaSparkContext ctx = new JavaSparkContext(sc);
		String fName = config.getString(PARAM_DATA_FILE_NAME);

		final Function<String, Row> rowFunction = new Function<String, Row>() {
			private static final long serialVersionUID = 1L;

			@Override
            public Row call(final String aLine) {
				String[] terms = aLine.split(" ");
				final ArrayList<Double> vals = new ArrayList<Double>();
				for (int i = 0; i < terms.length; i++) {
					vals.add(Double.parseDouble(terms[i]));
				}
				return RowBuilder.emptyRow().addAll(vals).build();
			}
		};
		final JavaRDD<Row> parsedData = ctx.textFile(fName, 1).map(
				rowFunction);
		return parsedData;
	}
}
