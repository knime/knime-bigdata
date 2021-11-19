#!groovy
def BN = (BRANCH_NAME == 'master' || BRANCH_NAME.startsWith('releases/')) ? BRANCH_NAME : 'releases/2021-12'

library "knime-pipeline@$BN"

properties([
    pipelineTriggers([
        upstream("knime-bigdata-externals/${env.BRANCH_NAME.replaceAll('/', '%2F')}" +
            ", knime-database/${env.BRANCH_NAME.replaceAll('/', '%2F')}" +
            ", knime-pmml-compilation/${env.BRANCH_NAME.replaceAll('/', '%2F')}" +
            ", knime-python/${env.BRANCH_NAME.replaceAll('/', '%2F')}")
    ]),
	parameters(workflowTests.getConfigurationsAsParameters() + fsTests.getFSConfigurationsAsParameters()),
    buildDiscarder(logRotator(numToKeepStr: '5')),
    disableConcurrentBuilds()
])

try {
    withEnv(["MAVEN_OPTS=-Xmx4G"]){                                                                                                                                                                            
        knimetools.defaultTychoBuild('org.knime.update.bigdata', 'maven && java11 && large')
    }


    testConfigs = [
        WorkflowTests: {
            // TEST REGEX
            def local_bd_tests = "(KnimeOnSpark|SparkLocal|SparkExecutor|SparklingWater|BigDataFileFormats)/(spark_2_[0-4]_higher|spark_all|ORC|Parquet)/(?!KnimeOnSpark_ServerProfiles_[a-z])(?!BD163_GenericDataSource_NoDriver)(?!BD892_KerberosImpersonation_Spark)(?!NewDBFramework/BD921_Spark2Hive_no_default_db).+"

    
            def testPrefix = "BigDataTests/${BN == KNIMEConstants.NEXT_RELEASE_BRANCH ? 'master' : BN}".replaceAll('releases/', '')
            def testRegex = "^/${testPrefix}/${local_bd_tests}"

            echo "${testRegex}"

            
        },
        FileHandlingTests: {
            def baseBranch = "${BN == KNIMEConstants.NEXT_RELEASE_BRANCH ? 'master' : BN}".replaceAll('releases/', '')
            def testflowsDirOnServer = args.testflowsDir ?: "BigDataTests/${baseBranch}/File Handling v2"
            echo "${testRegex}"

            workflowTests.runFilehandlingTests(testflowsDirOnServer: testflowsDirOnServer)
        }
    ]

    parallel testConfigs

    stage('Sonarqube analysis') {
        env.lastStage = env.STAGE_NAME
        workflowTests.runSonar([])
    }
} catch (ex) {
    currentBuild.result = 'FAILURE'
    throw ex
} finally {
    notifications.notifyBuild(currentBuild.result);
}

/* vim: set shiftwidth=4 expandtab smarttab: */
