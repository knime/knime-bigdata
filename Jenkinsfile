#!groovy
def BN = (BRANCH_NAME == 'master' || BRANCH_NAME.startsWith('releases/')) ? BRANCH_NAME : 'releases/2025-07'

library "knime-pipeline@$BN"

properties([
    pipelineTriggers([
        upstream("knime-bigdata-externals/${env.BRANCH_NAME.replaceAll('/', '%2F')}" +
            ", knime-database/${env.BRANCH_NAME.replaceAll('/', '%2F')}" +
            ", knime-pmml-compilation/${env.BRANCH_NAME.replaceAll('/', '%2F')}" +
            ", knime-python-legacy/${env.BRANCH_NAME.replaceAll('/', '%2F')}")
    ]),
	parameters(workflowTests.getConfigurationsAsParameters() + fsTests.getFSConfigurationsAsParameters()),
    buildDiscarder(logRotator(numToKeepStr: '5')),
    disableConcurrentBuilds()
])

try {
    withEnv(["MAVEN_OPTS=-Xmx4G"]){
        knimetools.defaultTychoBuild('org.knime.update.bigdata', 'maven && java17 && large')
    }


    testConfigs = [
        WorkflowTests: {
            // TEST REGEX
            def local_bd_tests = "(SparkLocal|SparkExecutor|SparkDatabase|BigDataFileFormats)/(spark_2_[0-4]_higher|spark_3_5_higher|spark_all|ORC|Parquet|DeltaTable)/(?!KnimeOnSpark_ServerProfiles_[a-z])(?!BD163_GenericDataSource_NoDriver)(?!BD892_KerberosImpersonation_Spark)(?!NewDBFramework/BD921_Spark2Hive_no_default_db).+"

    
            def testPrefix = "BigDataTests/${BN == KNIMEConstants.NEXT_RELEASE_BRANCH ? 'master' : BN}".replaceAll('releases/', '')
            def testRegex = "/${testPrefix}/${local_bd_tests}"

            echo "${testRegex}"

            withEnv([ "KNIME_POSTGRES_USER=knime01", "KNIME_POSTGRES_PASSWORD=password",
                "KNIME_MYSQL_USER=root", "KNIME_MYSQL_PASSWORD=password",
                "KNIME_MSSQLSERVER_USER=sa", "KNIME_MSSQLSERVER_PASSWORD=@SaPassword123",
                "KNIME_ORACLE_USER=SYSTEM", "KNIME_ORACLE_PASSWORD=password"
            ]){
                workflowTests.runTests(
                    testflowsDir: testPrefix,
                    testflowsRegex: testRegex,
                    extraNodeLabel: 'large',
                    dependencies: [
                        repositories: [
                            'knime-aws',
                            'knime-bigdata-externals',
                            'knime-bigdata',
                            'knime-cloud',
                            'knime-conda',
                            'knime-credentials-base',
                            'knime-database',
                            'knime-database-proprietary',
                            'knime-datageneration',
                            'knime-distance',
                            'knime-ensembles',
                            'knime-expressions',
                            'knime-filehandling',
                            'knime-gateway',
                            'knime-h2o',
                            'knime-itemset',
                            'knime-jep',
                            'knime-jfreechart',
                            'knime-js-base',
                            'knime-kerberos',
                            'knime-office365',
                            'knime-on-spark',
                            'knime-perl',
                            'knime-pmml-translation',
                            'knime-pmml',
                            'knime-python-legacy',
                            'knime-rest',
                            'knime-reporting',
                            'knime-sparkling-water',
                            'knime-stats',
                            'knime-streaming',
                            'knime-svm',
                            'knime-testing-internal',
                            'knime-textprocessing',
                            'knime-timeseries',
                            'knime-virtual',
                            'knime-xml',
                        ],
                        ius: [
                            'org.knime.features.ext.h2o.mojo.spark.feature.group',
                            'org.knime.features.ext.h2o.spark.feature.group',
                        ],
                    ],
                    sidecarContainers: [
                        [ image: "${dockerTools.ECR}/knime/mssql-server", namePrefix: "MSSQLSERVER", port: 1433, 
                            envArgs: ["ACCEPT_EULA=Y", "SA_PASSWORD=${env.KNIME_MSSQLSERVER_PASSWORD}", "MSSQL_DB=knime_testing"]
                        ],
                        [ image: "${dockerTools.ECR}/knime/oracle-xe-11g", namePrefix: "ORACLE", port: 1521, 
                            envArgs: [ "DEFAULT_SYS_PASS=Pw40racle" ]
                        ],
                        [ image: "${dockerTools.ECR}/knime/postgres:12", namePrefix: "POSTGRES", port: 5432, 
                            envArgs: [
                                "POSTGRES_USER=${env.KNIME_POSTGRES_USER}", "POSTGRES_PASSWORD=${env.KNIME_POSTGRES_PASSWORD}",
                                "POSTGRES_DB=knime_testing"
                            ]
                        ],
                        [ image: "${dockerTools.ECR}/knime/mysql5", namePrefix: "MYSQL", port: 3306, 
                            envArgs: ["MYSQL_ROOT_PASSWORD=${env.KNIME_MYSQL_PASSWORD}"]
                        ],
                    ]
                )
            }
            
        },
        FileHandlingTests: {
            def baseBranch = "${BN == KNIMEConstants.NEXT_RELEASE_BRANCH ? 'master' : BN}".replaceAll('releases/', '')
            def fhTestflowsDir = "BigDataTests/${baseBranch}/File Handling v2"
            workflowTests.runFilehandlingTests (
                testflowsDir: fhTestflowsDir,
                dependencies: [
                    repositories: [
                        'knime-aws',
                        'knime-azure',
                        'knime-bigdata',
                        'knime-bigdata-externals',
                        'knime-cloud',
                        'knime-database',
                        'knime-datageneration',
                        'knime-jep',
                        'knime-kerberos',
                        'knime-office365',
                        'knime-streaming'
                    ]
                ]
            )
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
