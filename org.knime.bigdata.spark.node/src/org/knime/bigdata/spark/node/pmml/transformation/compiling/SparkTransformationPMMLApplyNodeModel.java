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
 *   Created on 31.07.2015 by dwk
 */
package org.knime.bigdata.spark.node.pmml.transformation.compiling;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.xml.transform.SourceLocator;

import org.apache.xmlbeans.XmlException;
import org.dmg.pmml.DerivedFieldDocument.DerivedField;
import org.dmg.pmml.PMMLDocument;
import org.knime.base.pmml.translation.PMMLTranslator;
import org.knime.base.pmml.translation.TerminatingMessageException;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.node.pmml.transformation.AbstractSparkTransformationPMMLApplyNodeModel;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.pmml.PMMLPortObject;
import org.knime.ext.sun.nodes.script.compile.CompilationFailedException;
import org.knime.ext.sun.nodes.script.compile.JavaCodeCompiler;

import com.knime.pmml.compilation.java.compile.CompiledModelPortObject;
import com.knime.pmml.compilation.java.compile.CompiledModelPortObjectSpec;

import net.sf.saxon.s9api.MessageListener;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.XdmNode;

/**
 * The PMML transformation node model.
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkTransformationPMMLApplyNodeModel extends AbstractSparkTransformationPMMLApplyNodeModel
    implements MessageListener {
    private static final NodeLogger LOGGER = NodeLogger.getLogger(SparkTransformationPMMLApplyNodeModel.class);
    /**The name of the java package.*/
    private static final String PACKAGE_NAME = "";
    /**The name of the java class.*/
    private static final String MODEL_NAME = "MainModel";

    SparkTransformationPMMLApplyNodeModel() {
        super(new PortType[]{PMMLPortObject.TYPE, SparkDataPortObject.TYPE},
            new PortType[]{SparkDataPortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
//        final PMMLPortObjectSpec pmmlSpec = (PMMLPortObjectSpec) inSpecs[0];
//        final SparkDataPortObjectSpec sparkSpec = (SparkDataPortObjectSpec) inSpecs[1];
        return new PortObjectSpec[] {null};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void message(final XdmNode arg0, final boolean arg1, final SourceLocator arg2) {
        if (!arg1) {
            setWarningMessage(arg0.toString());
            LOGGER.warn(arg0.toString());
        } else {
            throw new TerminatingMessageException(arg0.toString());
        }
    }

    /**
     * {@inheritDoc}
     * @throws InvalidSettingsException
     * @throws SaxonApiException
     * @throws IOException
     */
    @Override
    public CompiledModelPortObject getCompiledPMMLModel(final ExecutionMonitor exec, final PortObject[] inObjects)
            throws Exception {
         final PMMLPortObject pmml = (PMMLPortObject)inObjects[0];
         String doc = pmml.getPMMLValue().toString();
         String code;
         try {
             code = PMMLTranslator.generateJava(doc, this, PACKAGE_NAME, MODEL_NAME);
         } catch (TerminatingMessageException tme) {
             throw new UnsupportedOperationException(tme.getMessage());
         }

         try {
             return new CompiledModelPortObject(code, PACKAGE_NAME, MODEL_NAME, JavaCodeCompiler.JavaVersion.JAVA_7);
         } catch (CompilationFailedException e) {
             throw new InvalidSettingsException("The compilation of the generated code failed.\n" + e.getMessage());
         }
    }

    @Override
    protected DataTableSpec createTransformationResultSpec(final DataTableSpec inSpec, final PortObject pmmlPort,
        final CompiledModelPortObjectSpec cms, final List<Integer> addCols, final List<Integer> skipCols)
        throws InvalidSettingsException {

        final PMMLDocument pmmlDoc = parsePMML(pmmlPort);
        if (pmmlDoc.getPMML().getTransformationDictionary() == null
                || pmmlDoc.getPMML().getTransformationDictionary().getDerivedFieldList().size() == 0) {
            setWarningMessage("Empty PMML detected.");
            return inSpec;
        }

        final DataColumnSpec pmmlResultColSpecs[] = cms.getTransformationsResultColSpecs(inSpec);
        final DerivedField newColumns[] = extractNewPMMLResultColumns(pmmlDoc);
        final List<DataColumnSpec> resultCols = new ArrayList<>(inSpec.getNumColumns());
        for (int i = 0; i < inSpec.getNumColumns(); i++) {
            resultCols.add(inSpec.getColumnSpec(i));
        }

        // add new columns and drop input columns in replace mode
        for (int i = 0; i < newColumns.length; i++) {
            final DerivedField df = newColumns[i];
            final int inputColIdx = inSpec.findColumnIndex(df.getDisplayName()); // this works only in KNIME
            final int pmmlResultColIdx = findColumnIndex(pmmlResultColSpecs, df.getName());

            if (inputColIdx >= 0 && pmmlResultColIdx >= 0) { // add only the specs to the result that have a matching input column
                final DataColumnSpec pmmlResultCol = pmmlResultColSpecs[pmmlResultColIdx];
                final DataColumnSpecCreator creator = new DataColumnSpecCreator(pmmlResultCol);

                if (replace()) {
                    final DataColumnSpec inputColumn = inSpec.getColumnSpec(inputColIdx);
                    resultCols.remove(inputColumn);
                    skipCols.add(inputColIdx);
                    creator.setName(inputColumn.getName());
                }

                resultCols.add(creator.createSpec());
                addCols.add(pmmlResultColIdx);
            }
        }

        return new DataTableSpec(resultCols.toArray(new DataColumnSpec[0]));
    }

    /** Parses PMML from given port or throws {@link InvalidSettingsException} */
    private PMMLDocument parsePMML(final PortObject pmmlPort) throws InvalidSettingsException {
        if (pmmlPort != null && pmmlPort instanceof PMMLPortObject) {
            try {
                final PMMLPortObject pmmlIn = (PMMLPortObject) pmmlPort;
                pmmlIn.validate();
                return PMMLDocument.Factory.parse(pmmlIn.getPMMLValue().getDocument());
            } catch (XmlException e) {
                throw new InvalidSettingsException("Unable to parse PMML", e);
            }
        } else {
            throw new InvalidSettingsException("Unable to read PMML from input port.");
        }
    }

    /**
     * Returns array of PMML generated output columns. In replace mode, only the last applied transformation per input
     * column will be used and no intermediate columns are appended.
     */
    private DerivedField[] extractNewPMMLResultColumns(final PMMLDocument pmmlDoc) throws InvalidSettingsException {
        final List<DerivedField> dfs = pmmlDoc.getPMML().getTransformationDictionary().getDerivedFieldList();

        if (replace()) {
            // find the final transformation and ignore intermediate columns
            final HashMap<String, DerivedField> columnReplacement = new HashMap<>();
            for (DerivedField df : dfs) {
                DerivedField other = columnReplacement.get(df.getDisplayName());
                if (other == null) {
                    columnReplacement.put(df.getDisplayName(), df);
                } else if (other.getName().length() < df.getName().length() && df.getName().startsWith(other.getName())) {
                    // other is an intermediate field
                    columnReplacement.put(df.getDisplayName(), df);
                } else if (other.getName().length() > df.getName().length() && other.getName().startsWith(df.getName())) {
                    // df is an intermediate field
                } else {
                    throw new InvalidSettingsException("Found more than on transformation with same input column "
                            + df.getDisplayName() + ", unable to replace columns.");
                }
            }

            return columnReplacement.values().toArray(new DerivedField[0]);

        } else {
            // use all transformations
            return dfs.toArray(new DerivedField[0]);
        }
    }

    /** @return index of column with given name in given array or <code>-1</code> */
    private int findColumnIndex(final DataColumnSpec specs[], final String name) {
        for (int i = 0; i < specs.length; i++) {
            if (specs[i].getName().equals(name)) {
                return i;
            }
        }

        return -1;
    }
}
