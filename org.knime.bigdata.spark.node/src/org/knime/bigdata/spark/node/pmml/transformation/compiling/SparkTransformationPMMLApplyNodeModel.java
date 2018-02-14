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
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.transform.SourceLocator;

import org.apache.commons.lang3.StringUtils;
import org.dmg.pmml.DerivedFieldDocument.DerivedField;
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

    /**
     * {@inheritDoc}
     *
     * Only transformations with a valid input column are added to the result table. If the transformation does not
     * contain a display name, the input column with longest matching name of the output column name will be used.
     *
     * <p>
     * Note on filtering intermediate columns: It is not possible to decide if one of two derived fields, with the same
     * input column, is an intermediate field or if it belongs to e.g. a binary category2number translation.
     * </p>
     */
    @Override
    protected DataTableSpec createTransformationResultSpec(final DataTableSpec inSpec, final PortObject pmmlPort,
        final CompiledModelPortObjectSpec cms, final List<Integer> addCols, final List<Integer> skipCols,
        final Map<Integer, Integer> replaceCols) throws InvalidSettingsException {

        final DerivedField derivedFields[] = extractNewPMMLResultColumns(((PMMLPortObject) pmmlPort).getDerivedFields());
        if (derivedFields.length == 0) {
            setWarningMessage("PMML document contains no transformations. Doing nothing.");
            return inSpec;
        }

        // outputs
        final List<String> warnings = new LinkedList<>();
        final List<DataColumnSpec> resultCols = new ArrayList<>(inSpec.getNumColumns());
        for (int i = 0; i < inSpec.getNumColumns(); i++) {
            resultCols.add(inSpec.getColumnSpec(i));
        }


        final int inputColIndicesOfDerivedField[] = findInputColumnIndicesOfDerivedFields(inSpec, cms, derivedFields);
        final int inputColUsage[] = countUsage(inSpec.getNumColumns(), inputColIndicesOfDerivedField);
        final DataColumnSpec pmmlResultColSpecs[] = cms.getTransformationsResultColSpecs(inSpec);

        // add all new columns and drop the input columns in replace mode
        for (int i = 0; i < derivedFields.length; i++) {
            final DerivedField df = derivedFields[i];

            final int inputColIdx = inputColIndicesOfDerivedField[i];
            if (inputColIdx < 0) {
                warnings.add(String.format("Missing input column%s, ignoring transformation %s",
                    StringUtils.isBlank(df.getDisplayName())
                        ? ""
                        : " " + df.getDisplayName(),
                    df.getName()));
                continue;
            }

            // add only the specs to the result that have a matching input column
            final int pmmlResultColIdx = findColumnIndex(pmmlResultColSpecs, df.getName());
            if (pmmlResultColIdx < 0) {
                warnings.add(String.format("Missing column %s in PMML result spec. Ignoring transformation.",
                    df.getName()));
                continue;
            }

            final DataColumnSpec pmmlResultCol = pmmlResultColSpecs[pmmlResultColIdx];
            if (replace()) {
                final DataColumnSpec inputColumn = inSpec.getColumnSpec(inputColIdx);

                if (inputColUsage[inputColIdx] == 1) {
                    int outputColIdx = resultCols.indexOf(inputColumn);
                    final DataColumnSpecCreator creator = new DataColumnSpecCreator(pmmlResultCol);
                    creator.setName(inputColumn.getName());
                    resultCols.set(outputColIdx, creator.createSpec());
                    replaceCols.put(inputColIdx, pmmlResultColIdx);

                } else {
                    resultCols.remove(inputColumn);
                    skipCols.add(inputColIdx);
                    resultCols.add(pmmlResultCol);
                    addCols.add(pmmlResultColIdx);
                }

            } else {
                resultCols.add(pmmlResultCol);
                addCols.add(pmmlResultColIdx);
            }
        }

        if (warnings.size() > 0) {
            final List<String> sub = warnings.subList(0, Math.min(10, warnings.size()));
            setWarningMessage(StringUtils.join(sub, "\n"));
        }

        return new DataTableSpec(resultCols.toArray(new DataColumnSpec[0]));
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

    /**
     * Returns array of PMML generated output columns.
     *
     * In replace mode: If field has a display name, only the last applied transformation per input column will be used
     * and no intermediate columns are appended.
     */
    private DerivedField[] extractNewPMMLResultColumns(final DerivedField derivedFields[]) throws InvalidSettingsException {
        if (replace()) {
            final List<DerivedField> result = new ArrayList<>(derivedFields.length);

            // source -> derived field map that contains last transformation in a chain and no intermediate fields
            final HashMap<String, DerivedField> chainResultFields = new HashMap<>();

            for (DerivedField df : derivedFields) {
                if (supportsIntermediateReplace(df)) {
                    addPMMLResultField(chainResultFields, df);
                } else {
                    result.add(df);
                }
            }

            result.addAll(chainResultFields.values());

            return result.toArray(new DerivedField[0]);

        } else {
            // use all transformations
            return derivedFields;
        }
    }

    /** @return <code>true</code> if display name equals source name with appended stars */
    private boolean supportsIntermediateReplace(final DerivedField derivedField) {
        final String destName = derivedField.getName();
        final String sourceName = derivedField.getDisplayName();
        return !StringUtils.isBlank(sourceName)
                && destName.startsWith(sourceName)
                && destName.substring(sourceName.length()).matches("\\*+");
    }

    /**
     * Eliminate intermediate fields by adding a derived field to given map if map does not contain a field with the
     * same display name or overwrite a field with same display name and shorter name.
     *
     * @param chainResultFields map with source column (display name) to derived field mapping
     * @param df derived field with display name to add, that {@link #supportsIntermediateReplace(DerivedField)}
     * @throws InvalidSettingsException if two columns with same input and output columns present
     */
    private void addPMMLResultField(final HashMap<String, DerivedField> chainResultFields, final DerivedField df) throws InvalidSettingsException {
        DerivedField other = chainResultFields.get(df.getDisplayName());
        if (other == null) {
            chainResultFields.put(df.getDisplayName(), df);
        } else if (other.getName().length() < df.getName().length()) {
            // other is an intermediate field, replace it
            chainResultFields.put(df.getDisplayName(), df);
        } else if (other.getName().length() > df.getName().length()) {
            // this is an intermediate field, keep other
        } else {
            throw new InvalidSettingsException("Found more than on transformation with same input column "
                    + df.getDisplayName() + " and same output column " + df.getName() + ", unable to replace columns.");
        }
    }

    /**
     * Find index of input column with matching display name or best matching name if display name is
     * not present.
     *
     * @param inSpec input table spec containing input columns
     * @param cms compiled PMML model (data dictionary)
     * @param derivedFields derived fields with name and optional display name
     * @return array with index of input column or -1 for each derived field
     */
    private int[] findInputColumnIndicesOfDerivedFields(final DataTableSpec inSpec, final CompiledModelPortObjectSpec cms,
        final DerivedField derivedFields[]) {

        final Set<String> pmmlModelInputColumns = cms.getInputIndices().keySet();
        final int inputColIndicesOfDerivedField[] = new int[derivedFields.length];

        for (int i = 0; i < derivedFields.length; i++) {
            final DerivedField df = derivedFields[i];

            if (df.getDisplayName() != null) {
                // most PMML transformations specify a "display name", which names the input column that
                // is consumed by the transformation
                inputColIndicesOfDerivedField[i] = inSpec.findColumnIndex(df.getDisplayName());

            } else {
                // some PMML transformations do not have a display name set (e.g. those produced by the "Category to Number" node
                // use column with most matching name instead of display name
                String derivedFielName = df.getName();
                String inputColumnName = "";
                inputColIndicesOfDerivedField[i] = -1;
                for (int j = 0; j < inSpec.getNumColumns(); j++) {
                    final String inCol = inSpec.getColumnSpec(j).getName();
                    if (derivedFielName.startsWith(inCol) && pmmlModelInputColumns.contains(inCol) && inCol.length() > inputColumnName.length()) {
                        inputColumnName = inCol;
                        inputColIndicesOfDerivedField[i] = j;
                    }
                }
            }
        }

        return inputColIndicesOfDerivedField;
    }

    /**
     * Count occurrences of input column indices in inputIndices array.
     *
     * @param inputColumns number of input columns
     * @param inputIndices array containing input column indices
     * @return array with usage count of each input column
     */
    private int[] countUsage(final int inputColumns, final int inputIndices[]) {
        final int usage[] = new int[inputColumns];
        Arrays.fill(usage, 0);
        for (int i = 0; i < inputIndices.length; i++) {
            if (inputIndices[i] >= 0) {
                usage[inputIndices[i]]++;
            }
        }
        return usage;
    }
}
