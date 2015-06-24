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
	 * path to first data table, may also be used as a key
	 */
	public static final String PARAM_TABLE_1 = "table1";

    /**
     * path to second data table, may also be used as a key
     */
    public static final String PARAM_TABLE_2 = "table2";

    /**
	 * name of model or the model itself
	 */
	public static final String PARAM_MODEL_NAME = "modelName";


    /**
     * boolean that indicates if probabilities should be added
     */
    public static final String PARAM_APPEND_PROBABILITIES = "appendProbabilities";

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
    /**
     * Criterion used for information gain calculation. Supported values: "gini" (recommended) or "entropy".
     */
    public static final String PARAM_INFORMATION_GAIN = "impurity";
    /**
     * supported information gain criterion
     */
    public static final String VALUE_GINI = "gini";
    /**
     * supported information gain criterion
     */
    public static final String VALUE_ENTROPY = "entropy";

    /**
     * parameter for maximal (search, tree, ...) depth
     */
    public static final String PARAM_MAX_DEPTH = "maxDepth";

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
