package com.knime.bigdata.spark.jobserver.jobs;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;

import spark.jobserver.SparkJobValidation;

/**
 * @author dwk
 *
 *         SparkJob that fetches and serializes a number of rows from the
 *         specified RDD (some other job must have previously stored this RDD
 *         under this name in the named rdds map)
 *
 *
 */
public class FetchRowsJob extends KnimeSparkJob {

	private final static Logger LOGGER = Logger.getLogger(FetchRowsJob.class
			.getName());

	private static final String PARAM_NUM_ROWS =ParameterConstants.PARAM_NUMBER_ROWS;

	/**
	 * parse command line parameters
	 */
	@Override
	public SparkJobValidation validate(final JobConfig aConfig) {
		String msg = null;
		if (!aConfig.hasInputParameter(PARAM_NUM_ROWS)) {
			msg = "Input parameter '" + PARAM_NUM_ROWS + "' missing.";
		} else {
			try {
				@SuppressWarnings("unused")
                int x = aConfig.getInputParameter(PARAM_NUM_ROWS, Integer.class);
			} catch (Exception e) {
				msg = "Input parameter '" + PARAM_NUM_ROWS + "' is not of expected type 'integer'.";
			}
		}

		if (msg == null && !aConfig.hasInputParameter(PARAM_INPUT_TABLE)) {
			msg = "Input parameter '" + PARAM_INPUT_TABLE + "' missing.";
		}

		if (msg != null) {
			return ValidationResultConverter.invalid(msg);
		}

		return ValidationResultConverter.valid();
	}

	@Override
	public JobResult runJobWithContext(final SparkContext sc, final JobConfig aConfig) throws GenericKnimeSparkException {

	    if (!validateNamedRdd(aConfig.getInputParameter(PARAM_INPUT_TABLE))) {
            throw new GenericKnimeSparkException("Input data table missing for key: "+aConfig.getInputParameter(PARAM_INPUT_TABLE));
        }
		final int numRows = aConfig.getInputParameter(PARAM_NUM_ROWS, Integer.class);
		FetchRowsJob.LOGGER.log(Level.INFO, "Fetching " + numRows + " rows from input RDD");
		final JavaRDD<Row> inputRDD = getFromNamedRdds(aConfig.getInputParameter(PARAM_INPUT_TABLE));
		final List<Row> res;
		if (numRows > 0) {
		    res = inputRDD.take(numRows);
		} else {
		    res = inputRDD.collect();
		}
		return JobResult.emptyJobResult().withMessage("OK").withObjectResult(mapTo2DimArray(res));
	}

	private Object[][] mapTo2DimArray(final List<Row> aRows) {
		Object[][] rows = new Object[aRows.size()][];
		for (int i = 0; i < aRows.size(); i++) {
			rows[i] = mapToArray(aRows.get(i));
		}
		return rows;
	}

	private Object[] mapToArray(final Row aRow) {
		Object[] res = new Object[aRow.length()];
		for (int i = 0; i < aRow.length(); i++) {
			res[i] = aRow.get(i);
		}
		return res;
	}

}