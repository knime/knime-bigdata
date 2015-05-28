package com.knime.bigdata.spark.jobserver.server;

/**
 * some constants, primarily for JSON parameters
 * for more reliable unit testing and easier refactoring
 * @author dwk
 *
 */
public class ParameterConstants {

	/**
	 * JASON group {input {....} } for input parameters
	 */
	public static final String PARAM_INPUT = "input";

	/**
	 * JASON group {output {....} } for output parameters
	 */
	public static final String PARAM_OUTPUT = "output";

	/**
	 * specific parameters, may be used as input and/or output
	 * parameters
	 */
	/**
	 * number of clusters for cluster learners
	 */
	public static final String PARAM_NUM_CLUSTERS = "noOfClusters";
	/**
	 * number of iterations
	 */
	public static final String PARAM_NUM_ITERATIONS = "noOfIterations";
	/**
	 * path to data, may also be used as a key
	 */
	public static final String PARAM_DATA_PATH = "dataPath";
	/**
	 * name of model
	 */
	public static final String PARAM_MODEL_NAME = "modelName";

	/**
	 * number of rows
	 */
	public static final String PARAM_NUMBER_ROWS = "numRows";

	/**
	 * sql statement parameter
	 */
    public static final String PARAM_SQL_STATEMENT = "sql";
    /**
     * column indices starting with 0
     */
    public static final String PARAM_COL_IDXS = "colIndices";
}
