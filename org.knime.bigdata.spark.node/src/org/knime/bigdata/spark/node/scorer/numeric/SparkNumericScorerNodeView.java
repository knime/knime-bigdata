package org.knime.bigdata.spark.node.scorer.numeric;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.text.NumberFormat;

import javax.swing.JLabel;
import javax.swing.JPanel;

import org.knime.core.node.NodeView;

/**
 * This implements a scorer view for the {@link SparNumericScorerNodeModel}.
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
class SparkNumericScorerNodeView extends NodeView<SparkNumericScorerNodeModel> {

    private JLabel m_rSquared;

    private JLabel m_meanAbsError;

    private JLabel m_meanSquaredError;

    private JLabel m_rootMeanSquaredError;

    private JLabel m_meanSignedDifference;

    /** Delegates to super class.
     * @param nodeModel
     */
    protected SparkNumericScorerNodeView(final SparkNumericScorerNodeModel nodeModel) {
        super(nodeModel);

        JPanel summary = new JPanel(new GridBagLayout());

        GridBagConstraints c = new GridBagConstraints();
        c.gridx = 0;
        c.gridy = 0;
        c.insets = new Insets(3, 3, 3, 5);
        c.anchor = GridBagConstraints.WEST;

        summary.add(new JLabel("R\u00b2: "), c);
        m_rSquared = new JLabel("n/a");
        c.gridx = 1;
        summary.add(m_rSquared, c);

        c.gridx = 0;
        c.gridy++;
        summary.add(new JLabel("Mean absolute error: "), c);
        m_meanAbsError = new JLabel("n/a");
        c.gridx = 1;
        summary.add(m_meanAbsError, c);

        c.gridx = 0;
        c.gridy++;
        summary.add(new JLabel("Mean squared error: "), c);
        m_meanSquaredError = new JLabel("n/a");
        c.gridx = 1;
        summary.add(m_meanSquaredError, c);

        c.gridx = 0;
        c.gridy++;
        summary.add(new JLabel("Root mean squared error: "), c);
        m_rootMeanSquaredError = new JLabel("n/a");
        c.gridx = 1;
        summary.add(m_rootMeanSquaredError, c);

        c.gridx = 0;
        c.gridy++;
        summary.add(new JLabel("Mean signed difference: "), c);
        m_meanSignedDifference = new JLabel("n/a");
        c.gridx = 1;
        summary.add(m_meanSignedDifference, c);

        setComponent(summary);
    }

    /**
     * Sets all the labels in the nodeView with the values of the numeric scorers outcome.
     *
     * @param rSquare the rSquared to set
     * @param meanAbsError the meanAbsError to set
     * @param meanSquaredError the meanSquaredError to set
     * @param rootMeanSquaredDeviation the rootMeanSquaredError to set
     * @param meanSignedDifference the meanSignedDifference to set
     */
    protected void setLabels(final double rSquare, final double meanAbsError, final double meanSquaredError,
        final double rootMeanSquaredDeviation, final double meanSignedDifference) {
        NumberFormat nf = NumberFormat.getNumberInstance();
        m_rSquared.setText(nf.format(rSquare));
        m_meanAbsError.setText(nf.format(meanAbsError));
        m_meanSquaredError.setText(nf.format(meanSquaredError));
        m_rootMeanSquaredError.setText(nf.format(rootMeanSquaredDeviation));
        m_meanSignedDifference.setText(nf.format(meanSignedDifference));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void onClose() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void onOpen() {
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

