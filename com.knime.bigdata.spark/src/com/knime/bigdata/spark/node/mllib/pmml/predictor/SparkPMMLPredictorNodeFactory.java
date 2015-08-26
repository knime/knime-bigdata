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
 *   Created on 12.02.2015 by koetter
 */
package com.knime.bigdata.spark.node.mllib.pmml.predictor;

import java.awt.Dimension;

import javax.swing.JPanel;

import org.knime.base.node.mine.util.PredictorNodeDialog;
import org.knime.base.pmml.translation.CompiledModel.MiningFunction;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataType;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;
import org.knime.core.node.port.PortObjectSpec;

import com.knime.pmml.compilation.java.compile.CompiledModelPortObjectSpec;

/**
 *
 * @author koetter
 */
public class SparkPMMLPredictorNodeFactory extends NodeFactory<SparkPMMLPredictorNodeModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public SparkPMMLPredictorNodeModel createNodeModel() {
        return new SparkPMMLPredictorNodeModel();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected int getNrNodeViews() {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeView<SparkPMMLPredictorNodeModel>
        createNodeView(final int viewIndex, final SparkPMMLPredictorNodeModel nodeModel) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean hasDialog() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected NodeDialogPane createNodeDialogPane() {
        return new PredictorNodeDialog(SparkPMMLPredictorNodeModel.createOutputProbabilitiesSettingsModel()) {
            {
                getPanel().setPreferredSize(new Dimension(510, 200));
            }

            @Override
            protected void extractTargetColumn(final PortObjectSpec[] specs) {
                final CompiledModelPortObjectSpec cmpos = ((CompiledModelPortObjectSpec)specs[0]);
                if (cmpos == null) {
                    return;
                }
                final String outName = cmpos.getOutputFields()[0];
                final DataType cellType = cmpos.getMiningFunction() == MiningFunction.REGRESSION
                        ? DoubleCell.TYPE : StringCell.TYPE;
                setLastTargetColumn(new DataColumnSpecCreator(outName, cellType).createSpec());
            }

            /**
             * {@inheritDoc}
             */
            @Override
            protected void addOtherControls(final JPanel panel) {

            }
        };
    }

}
