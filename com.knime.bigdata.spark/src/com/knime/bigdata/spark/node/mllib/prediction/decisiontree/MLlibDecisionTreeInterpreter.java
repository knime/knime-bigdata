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
 *   Created on 21.07.2015 by koetter
 */
package com.knime.bigdata.spark.node.mllib.prediction.decisiontree;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.SwingWorker;
import javax.xml.transform.stream.StreamResult;

import org.apache.spark.mllib.pmml.export.PMMLModelExport;
import org.apache.spark.mllib.pmml.export.PMMLModelExportFactory;
import org.apache.spark.mllib.tree.configuration.Algo;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.xmlbeans.XmlException;
import org.dmg.pmml.PMML;
import org.jpmml.model.JAXBUtil;
import org.knime.base.node.io.pmml.read.PMMLImport;
import org.knime.base.node.mine.decisiontree2.PMMLDecisionTreeTranslator;
import org.knime.base.node.mine.decisiontree2.model.DecisionTree;
import org.knime.base.node.mine.decisiontree2.view.DecTreeGraphView;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeView;
import org.knime.core.node.port.pmml.PMMLPortObject;

import com.knime.bigdata.spark.port.model.SparkModel;
import com.knime.bigdata.spark.port.model.interpreter.HTMLModelInterpreter;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class MLlibDecisionTreeInterpreter extends HTMLModelInterpreter<SparkModel<DecisionTreeModel>> {
    //implements SparkModelInterpreter<SparkModel<DecisionTreeModel>> {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(MLlibDecisionTreeInterpreter.class);

    private static final long serialVersionUID = 1L;

    private static volatile MLlibDecisionTreeInterpreter instance;

    private MLlibDecisionTreeInterpreter() {
        //avoid object creation
    }

    /**
     * Returns the only instance of this class.
     *
     * @return the only instance
     */
    public static MLlibDecisionTreeInterpreter getInstance() {
        if (instance == null) {
            synchronized (MLlibDecisionTreeInterpreter.class) {
                if (instance == null) {
                    instance = new MLlibDecisionTreeInterpreter();
                }
            }
        }
        return instance;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getModelName() {
        return "DecisionTree";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSummary(final SparkModel<DecisionTreeModel> model) {
        final DecisionTreeModel treeModel = model.getModel();
        return "Tree depth: " + treeModel.depth() + " Number of nodes: " + treeModel.numNodes();
    }

    @Override
    protected String getHTMLDescription(final SparkModel<DecisionTreeModel> model) {
        final DecisionTreeModel treeModel = model.getModel();
        final StringBuilder buf = new StringBuilder();
        buf.append("Tree depth: " + treeModel.depth() + " Number of nodes: " + treeModel.numNodes());
        buf.append(" Features: ");
        final List<String> colNames = model.getLearningColumnNames();
        int idx = 0;
        for (final String colName : colNames) {
            if (idx > 0) {
                buf.append(", ");
            }
            buf.append(idx++ + "=" + colName);
        }
        buf.append("<br>");
        String debugString = treeModel.toDebugString();
        //remove first line
        debugString = debugString.replaceFirst(".*\n", "");
        debugString = debugString.replaceAll("\\n", "<br>");
        debugString = debugString.replaceAll("<=", "&le");
        debugString = debugString.replaceAll(">=", "&ge");
        debugString = debugString.replaceAll("\\s", "&nbsp;");
        buf.append(debugString);
        return buf.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JComponent[] getViews(final SparkModel<DecisionTreeModel> aDecisionTreeModel) {
        return new JComponent[]{getTreePanel(aDecisionTreeModel), super.getViews(aDecisionTreeModel)[0]};
    }

    /**
     * converts the given tree model into PMML and packs it into a JComponent
     *
     * @param aDecisionTreeModel
     * @param aColNames
     * @param aClassColName
     * @return displayable component
     */
    public static JComponent getTreeView(final DecisionTreeModel aDecisionTreeModel, final List<String> aColNames,
        final String aClassColName) {
        final Map<Integer, String> features = new HashMap<>();
        int ctr =0;
        for (String col : aColNames) {
            features.put(ctr++, col);
        }
        //TODO - this is a HACK to avoid the error message in
        //  org.knime.base.node.mine.decisiontree2.PMMLDecisionTreeTranslator.parseDecTreeFromModel(TreeModel)
        final DecisionTreeModel decisionTreeModel;
        if (aDecisionTreeModel.algo().toString().equalsIgnoreCase("Classification")) {
            decisionTreeModel = aDecisionTreeModel;
        } else {
            decisionTreeModel = new DecisionTreeModel(aDecisionTreeModel.topNode(), Algo.Classification());
        }
        PMMLModelExport pmmlModel = PMMLModelExportFactory.createPMMLModelExport(decisionTreeModel, features);
        PMML pmml = pmmlModel.pmml();
        return getTreeView(pmml);
    }

    static JComponent getTreeView(final PMML aPMML) {
        try {
            final PMMLPortObject outPMMLPort = createPMMLPortObjectSpec(aPMML);

            final PMMLDecisionTreeTranslator trans = new PMMLDecisionTreeTranslator();
            outPMMLPort.initializeModelTranslator(trans);
            final DecisionTree decTree = trans.getDecisionTree();

            decTree.resetColorInformation();
            DecTreeGraphView graph = new DecTreeGraphView(decTree.getRootNode(), decTree.getColorColumn());
            final JComponent view = new MLlibDecTreeGraphView(new MLlibDecisionTreeNodeModel(), graph).getView();
            view.setName("MLLib TreeView");
            return view;
        } catch (Exception e) {
            LOGGER.warn("Error converting Spark tree model, reason: " + e.getMessage(), e);
        }
        return null;
    }

    static private PMMLPortObject createPMMLPortObjectSpec(final PMML pmml) throws Exception {
        pmml.setVersion("4.2");
        File temp = File.createTempFile("dtpmml", ".xml");
        try {
            try (OutputStream os = new FileOutputStream(temp)) {
                StreamResult result = new StreamResult(os);

                JAXBUtil.marshalPMML(pmml, result);
            }
            try {
                PMMLImport pmmlImport = new PMMLImport(temp, true);
                return pmmlImport.getPortObject();
            } catch (IllegalArgumentException e) {
                String msg = "PMML model \"" + pmml.getHeader() + "\" is not a valid PMML model:\n" + e.getMessage();
                //setWarningMessage(msg);
                throw new InvalidSettingsException(msg);
            } catch (XmlException e) {
                throw new InvalidSettingsException(e);
            } catch (IOException e) {
                throw new InvalidSettingsException(e);
            }
        } finally {
            temp.delete();
        }
    }

    private JComponent getTreePanel(final SparkModel<DecisionTreeModel> aDecisionTreeModel) {

        final DecisionTreeModel treeModel = aDecisionTreeModel.getModel();
        final List<String> colNames = aDecisionTreeModel.getLearningColumnNames();
        final String classColName = aDecisionTreeModel.getClassColumnName();

        final JComponent component = new JPanel();
        component.setLayout(new BorderLayout());
        component.setBackground(NodeView.COLOR_BACKGROUND);

        final JPanel p = new JPanel(new FlowLayout());
        final JButton b = new JButton("Show tree");

        p.add(b);

        component.add(p, BorderLayout.NORTH);
        final JPanel treePanel = new JPanel();
        treePanel.setLayout(new BorderLayout());
        component.add(treePanel, BorderLayout.CENTER);
        component.setName("Decision Tree View");
        b.addActionListener(new ActionListener() {
            /** {@inheritDoc} */
            @Override
            public void actionPerformed(final ActionEvent e) {

                treePanel.removeAll();
                treePanel.add(new JLabel("Converting decision tree ..."), BorderLayout.NORTH);
                treePanel.repaint();
                treePanel.revalidate();
                //TK_TODO: Add job cancel button to the dialog to allow users to stop the fetching job
                final SwingWorker<JComponent, Void> worker = new SwingWorker<JComponent, Void>() {
                    /** {@inheritDoc} */
                    @Override
                    protected JComponent doInBackground() throws Exception {
                        return MLlibDecisionTreeInterpreter.getTreeView(treeModel, colNames, classColName);
                    }

                    /** {@inheritDoc} */
                    @Override
                    protected void done() {
                        JComponent dt = null;
                        try {
                            dt = super.get();
                        } catch (ExecutionException | InterruptedException ee) {
                            LOGGER.warn("Error converting Spark tree model, reason: " + ee.getMessage(), ee);
                            final Throwable cause = ee.getCause();
                            final String msg;
                            if (cause != null) {
                                msg = cause.getMessage();
                            } else {
                                msg = ee.getMessage();
                            }
                            treePanel.removeAll();
                            treePanel.add(new JLabel("Error converting Spark tree model: " + msg), BorderLayout.NORTH);
                            treePanel.repaint();
                            treePanel.revalidate();
                            return;
                        }
                        if (dt == null) {
                            treePanel.removeAll();
                            treePanel.add(new JLabel("Error converting Spark tree model. For details see log file."),
                                BorderLayout.NORTH);
                            treePanel.repaint();
                            treePanel.revalidate();
                        } else {
                            treePanel.removeAll();
                            treePanel.add(dt, BorderLayout.CENTER);
                            component.setName(dt.getName());
                            component.repaint();
                            component.revalidate();
                        }
                    }
                };
                worker.execute();
            }
        });
        return component;
    }
}
