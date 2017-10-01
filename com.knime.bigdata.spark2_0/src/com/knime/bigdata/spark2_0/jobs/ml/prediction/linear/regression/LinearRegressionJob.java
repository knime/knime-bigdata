package com.knime.bigdata.spark2_0.jobs.ml.prediction.linear.regression;

import java.io.Serializable;

import org.apache.log4j.Logger;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.node.ml.prediction.linear.LinearLearnerJobInput;
import com.knime.bigdata.spark2_0.jobs.ml.prediction.linear.AbstractRegularizationJob;

/**
 * @author dwk
 */
@SparkClass
public class LinearRegressionJob extends AbstractRegularizationJob<LinearLearnerJobInput> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger.getLogger(LinearRegressionJob.class.getName());

    /**
     * supports multiple types of regularization: - none (a.k.a. ordinary least squares) - L2 (ridge regression) - L1
     * (Lasso) - L2 + L1 (elastic net) {@inheritDoc}
     */
    @Override
    protected LinearRegression getConfiguredRegressor(final LinearLearnerJobInput aConfig) {
        final LinearRegression lr = new LinearRegression().
            // Set the maximum number of iterations.  Default is 100.
            setMaxIter(aConfig.getNoOfIterations()).
            // Set the regularization parameter. Default is 0.0.
            setRegParam(aConfig.getRegularization()).
            // Set the ElasticNet mixing parameter.
            //  For alpha = 0, the penalty is an L2 penalty.
            //  For alpha = 1, it is an L1 penalty.
            //  For alpha in (0,1), the penalty is a combination of L1 and L2.
            //  Default is 0.0 which is an L2 penalty.
            setElasticNetParam(aConfig.getElasticNetParam()).
            //as of Spark 2.2:       Suggested depth for treeAggregate (greater than or equal to 2).
            //as of Spark 2.2: setAggregationDepth(2).
            // Set if we should fit the intercept. Default is true.
            setFitIntercept(aConfig.addIntercept()).

            // Set the solver algorithm used for optimization.
            //  In case of linear regression, this can be "l-bfgs", "normal" and "auto".
            //  "l-bfgs" denotes Limited-memory BFGS which is a limited-memory quasi-Newton optimization method.
            //  "normal" denotes using Normal Equation as an analytical solution to the linear regression problem.
            //  The default value is "auto" which means that the solver algorithm is selected automatically.
            setSolver(aConfig.getSolver()).
            //  Whether to standardize the training features before fitting the model.
            // The coefficients of models will be always returned on the original scale, so it will be transparent for users.
            //  Note that with/without standardization, the models should be always converged to the same solution when no regularization is applied.
            //  In R's GLMNET package, the default behavior is true as well. Default is true.
            setStandardization(aConfig.useFeatureScaling()).
            // Set the convergence tolerance of iterations. Smaller value will lead to higher accuracy with the cost of more iterations. Default is 1E-6.
            setTol(aConfig.getTolerance());
        if (aConfig.getWeightColumn() != null) {
            // Whether to over-/under-sample training instances according to the given weights in weightCol.
            // If not set or empty, all instances are treated equally (weight 1.0). Default is not set, so all instances have weight one.
            return lr.setWeightCol(aConfig.getWeightColumn());
        } else {
            return lr;
        }
    }

    /**
     * @param aData
     * @param aModel
     * @return Mean squared error
     */
    private double evaluateModel(final Dataset<Row> aData, final Serializable aModel) {
        // Evaluate model on training examples and compute training error
        if (aModel instanceof LinearRegressionModel) {
            LinearRegressionModel model = (LinearRegressionModel)aModel;
            // Summarize the model over the training set and print out some metrics
            LinearRegressionTrainingSummary trainingSummary = model.summary();
            LOGGER.debug("numIterations: " + trainingSummary.totalIterations());
            LOGGER.debug("objectiveHistory: " + Vectors.dense(trainingSummary.objectiveHistory()));
            trainingSummary.residuals().show();

            LOGGER.debug("training Mean Squared Error = " + trainingSummary.rootMeanSquaredError());
            LOGGER.debug("r2: " + trainingSummary.r2());

            return trainingSummary.rootMeanSquaredError();
        }
        return -1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Logger getLogger() {
        return LOGGER;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getAlgName() {
        return "Linear Regression (ML)";
    }
}