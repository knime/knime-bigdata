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
	 * number of iterations
	 */
	public static final String PARAM_NUM_ITERATIONS = "noOfIterations";

    /**
	 * name of model or the model itself
	 */
	public static final String PARAM_MODEL_NAME = "modelName";

    /**
     * String that contains the name of the main class to execute
     */
    public static final String PARAM_MAIN_CLASS = "mainClass";

	/**
	 * number of rows
	 */
	public static final String PARAM_NUMBER_ROWS = "numRows";

    /**
     * column indices starting with 0
     */
    public static final String PARAM_COL_IDXS = "colIndices";

    /**
     * names of selected columns
     */
    public static final String PARAM_COL_NAMES = "ColumnNames";

    /**
     * Classification column index starting with 0
     */
    public static final String PARAM_CLASS_COL_IDX = "classColIndex";

    /**
     * parameter for maximal (search, tree, ...) depth
     */
    public static final String PARAM_MAX_DEPTH = "maxDepth";

    /**
     * a random seed to have deterministic results.
     */
    public static final String PARAM_SEED = "Seed";

    /**
     * param for maximal number of bins
     */
    public static final String PARAM_MAX_BINS = "maxBins";

    /**
     * csv separator or some other separator
     */
    public static final String PARAM_SEPARATOR = "sep";

    /**
     * index of class label
     */
    public static final String PARAM_LABEL_INDEX = "labelIx";

    /**
     * Schema of an RDD.
     */
    public static final String PARAM_SCHEMA = "tableSchema";


}
