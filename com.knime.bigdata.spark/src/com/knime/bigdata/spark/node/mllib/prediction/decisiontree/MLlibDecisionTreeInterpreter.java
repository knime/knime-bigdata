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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import javax.swing.JComponent;
import javax.xml.transform.stream.StreamResult;

import org.apache.spark.mllib.pmml.export.PMMLModelExport;
import org.apache.spark.mllib.pmml.export.PMMLModelExportFactory;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.xmlbeans.XmlException;
import org.dmg.pmml.DataField;
import org.dmg.pmml.PMML;
import org.jpmml.model.JAXBUtil;
import org.knime.base.node.io.pmml.read.PMMLImport;
import org.knime.base.node.mine.decisiontree2.PMMLDecisionTreeTranslator;
import org.knime.base.node.mine.decisiontree2.model.DecisionTree;
import org.knime.base.node.mine.decisiontree2.view.DecTreeGraphView;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.pmml.PMMLPortObject;

import com.knime.bigdata.spark.port.model.SparkModel;
import com.knime.bigdata.spark.port.model.interpreter.HTMLModelInterpreter;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class MLlibDecisionTreeInterpreter extends HTMLModelInterpreter<SparkModel<DecisionTreeModel>> {
    //implements SparkModelInterpreter<SparkModel<DecisionTreeModel>> {

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
        PMMLModelExport pmmlModel = PMMLModelExportFactory.createPMMLModelExport(aDecisionTreeModel.getModel());
        PMML pmml = pmmlModel.pmml();
        //TODO - verify that we can really assume that the order is the same....
        List<DataField> fields = pmml.getDataDictionary().getDataFields();
        final List<String> colNames = aDecisionTreeModel.getLearningColumnNames();

        for (int i = 0; i < colNames.size(); i++) {
            DataField field = fields.get(i);
            field.setDisplayName(colNames.get(i));
        }
        fields.get(fields.size() - 1).setDisplayName(aDecisionTreeModel.getClassColumnName());
        try {
            PMMLPortObject outPMMLPort = createPMMLPortObjectSpec(pmml);

            PMMLDecisionTreeTranslator trans = new PMMLDecisionTreeTranslator();
            outPMMLPort.initializeModelTranslator(trans);
            DecisionTree decTree = trans.getDecisionTree();

            decTree.resetColorInformation();
            JComponent view = new DecTreeGraphView(decTree.getRootNode(), decTree.getColorColumn()).getView();
            view.setName("MLLib TreeView");
            //
            return new JComponent[]{view, super.getViews(aDecisionTreeModel)[0]};
        } catch (Exception e) {
            // TODO ????
            e.printStackTrace();
        }
        return null;
    }

    private PMMLPortObject createPMMLPortObjectSpec(final PMML pmml) throws Exception {
        pmml.setVersion("4.2");
        File temp = File.createTempFile("dtpmml", ".xml");
        OutputStream os = new FileOutputStream(temp);
        StreamResult result = new StreamResult(os);

        JAXBUtil.marshalPMML(pmml, result);

        PMMLPortObject m_pmmlPort;
        try {
            PMMLImport pmmlImport = new PMMLImport(temp, false);
            m_pmmlPort = pmmlImport.getPortObject();
        } catch (IllegalArgumentException e) {
            String msg = "PMML model \"" + pmml.getHeader() + "\" is not a valid PMML model:\n" + e.getMessage();
            //setWarningMessage(msg);
            throw new InvalidSettingsException(msg);
        } catch (XmlException e) {
            throw new InvalidSettingsException(e);
        } catch (IOException e) {
            throw new InvalidSettingsException(e);
        }
        return m_pmmlPort;
    }

}
