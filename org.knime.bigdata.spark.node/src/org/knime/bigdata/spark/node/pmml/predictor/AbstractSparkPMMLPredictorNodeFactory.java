/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
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
 *   Created on 16.05.2016 by koetter
 */
package com.knime.bigdata.spark.node.pmml.predictor;

import java.awt.Dimension;

import javax.swing.JPanel;

import org.knime.base.node.mine.util.PredictorNodeDialog;
import org.knime.base.pmml.translation.CompiledModel.MiningFunction;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataType;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.pmml.PMMLPortObjectSpec;

import com.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;
import com.knime.bigdata.spark.core.node.SparkNodeModel;
import com.knime.pmml.compilation.java.compile.CompiledModelPortObjectSpec;

/**
 *
 * @author Tobias Koetter, KNIME.com
 * @param <M> the {@link SparkNodeModel} implementation
 */
public abstract class AbstractSparkPMMLPredictorNodeFactory<M extends SparkNodeModel> extends DefaultSparkNodeFactory<M> {

    /**Constructor.*/
    protected AbstractSparkPMMLPredictorNodeFactory() {
        super("mining/pmml");
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
        return new PredictorNodeDialog(AbstractSparkPMMLPredictorNodeModel.createOutputProbabilitiesSettingsModel()) {
            {
                getPanel().setPreferredSize(new Dimension(510, 200));
            }

            @Override
            protected void extractTargetColumn(final PortObjectSpec[] specs) {
                if (specs.length < 1 || specs[0] == null) {
                    return;
                }

                if (specs[0] instanceof CompiledModelPortObjectSpec) {
                    final CompiledModelPortObjectSpec cmpos = ((CompiledModelPortObjectSpec)specs[0]);
                    final String outName = cmpos.getOutputFields()[0];
                    final DataType cellType = cmpos.getMiningFunction() == MiningFunction.REGRESSION
                            ? DoubleCell.TYPE : StringCell.TYPE;
                    setLastTargetColumn(new DataColumnSpecCreator(outName, cellType).createSpec());

                } else if (specs[0] instanceof PMMLPortObjectSpec) {
                    final PMMLPortObjectSpec pmmlSpec = ((PMMLPortObjectSpec) specs[0]);
                    setLastTargetColumn(pmmlSpec.getTargetCols().iterator().next());
                }
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