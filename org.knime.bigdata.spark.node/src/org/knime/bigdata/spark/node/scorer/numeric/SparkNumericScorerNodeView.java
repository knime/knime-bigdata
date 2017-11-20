package org.knime.bigdata.spark.node.scorer.numeric;

import org.knime.base.node.mine.scorer.numeric.AbstractNumericScorerNodeView;

/**
 * This implements the {@link AbstractNumericScorerNodeView} for the {@link SparNumericScorerNodeModel}.
 *
 * @author Gabor Bakos
 * @author Bjoern Lohrmann, KNIME.com
 * @author Ole Ostergaard
 */
class SparkNumericScorerNodeView extends AbstractNumericScorerNodeView<SparkNumericScorerNodeModel> {


    /** Delegates to super class.
     * @param nodeModel
     */
    protected SparkNumericScorerNodeView(final SparkNumericScorerNodeModel nodeModel) {
        super(nodeModel);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void modelChanged() {
        SparkNumericScorerViewData model = getNodeModel().getViewData();

        if (model != null) {
            setLabels(model.getRSquare(), model.getMeanAbsError(), model.getMeanSquaredError(), model.getRootMeanSquaredDeviation(), model.getMeanSignedDifference());
        } else {
            setLabels(Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN);
        }
    }

}

