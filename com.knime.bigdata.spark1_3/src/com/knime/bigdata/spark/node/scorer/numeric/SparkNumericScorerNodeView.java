package com.knime.bigdata.spark.node.scorer.numeric;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.text.NumberFormat;

import javax.swing.JLabel;
import javax.swing.JPanel;

import org.knime.core.node.NodeView;

/**
 * <code>NodeView</code> for the "Spark Numeric Scorer" Node.
 *
 * TODO: This is mostly a code duplicate to {@link org.knime.base.node.mine.scorer.numeric.NumericScorerNodeView}
 *
 * @author Gabor Bakos
 * @author Bjoern Lohrmann, KNIME.com
 */
class SparkNumericScorerNodeView extends NodeView<SparkNumericScorerNodeModel> {

    private final JLabel m_rSquared;
    private final JLabel m_meanAbsError;
    private final JLabel m_meanSquaredError;
    private final JLabel m_rootMeanSquaredError;
    private final JLabel m_meanSignedDifference;

    /**
     * Creates a new view.
     *
     * @param nodeModel The model (class: {@link SparkNumericScorerNodeModel})
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
     * {@inheritDoc}
     */
    @Override
    protected void modelChanged() {
        SparkNumericScorerViewData model = getNodeModel().getViewData();
        NumberFormat nf = NumberFormat.getNumberInstance();

        if (model != null) {
            m_rSquared.setText(nf.format(model.getRSquare()));
            m_meanAbsError.setText(nf.format(model.getMeanAbsError()));
            m_meanSquaredError.setText(nf.format(model.getMeanSquaredError()));
            m_rootMeanSquaredError.setText(nf.format(model.getRootMeanSquaredDeviation()));
            m_meanSignedDifference.setText(nf.format(model.getMeanSignedDifference()));
        } else {
            m_rSquared.setText(nf.format(Double.NaN));
            m_meanAbsError.setText(nf.format(Double.NaN));
            m_meanSquaredError.setText(nf.format(Double.NaN));
            m_rootMeanSquaredError.setText(nf.format(Double.NaN));
            m_meanSignedDifference.setText(nf.format(Double.NaN));
        }
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
}

