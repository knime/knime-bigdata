/**
 * 
 */
package org.knime.bigdata.spark.core.livy;

import org.knime.bigdata.spark.core.livy.node.create.LivySparkContextCreatorNodeFactory;
import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactoryProvider;
import org.knime.bigdata.spark.core.version.AllVersionCompatibilityChecker;

/**
 * Node factory provider for Spark local.
 * 
 * @author Oleg Yasnev, KNIME GmbH
 */
public class LivyNodeFactoryProvider extends DefaultSparkNodeFactoryProvider {

    /**
     * Default constructor.
     */
    public LivyNodeFactoryProvider() {
        super(AllVersionCompatibilityChecker.INSTANCE, new LivySparkContextCreatorNodeFactory());
    }

}
