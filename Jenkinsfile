#!groovy

library "knime-pipeline@$BRANCH_NAME"

node {
	def upstreamParams = defaultProperties('org.knime.update.org',
		'com.knime.update.pmml.compilation',
		'org.knime.update.labs',
		'org.knime.update.targetPlatform')


	stage('Clean workspace') {
		cleanWorkspace()
		try {
			sh 'rm -rf git/knime-bigdata/com.knime.tpbuilder/target com.knime.update.bigdata.externals org.knime.update.bigdata.externals'
		} catch (ex) {
			currentBuild.result = 'FAILED'
			emailext (
				subject: "FAILURE in ${env.JOB_NAME} [${env.BUILD_NUMBER}]",
				body: "Failure while cleaning workspace. Check console output at ${env.BUILD_URL}/console",
				recipientProviders: [[$class: 'DevelopersRecipientProvider'], [$class: 'RequesterRecipientProvider']]
			)
			throw ex
		}

	}

	stage('Checkout sources') {
		checkoutSources (
			defaultBranch: BRANCH_NAME,
			credentialsId: 'bitbucket-jenkins',
			repos: [
				[name : 'knime-bigdata'],
				[name : 'knime-on-spark'],
				[name : 'knime-config'],
				[name : 'knime-jenkins'],
				[name : 'knime-orc'],
				[name : 'knime-parquet'],
				[name : 'knime-sparkling-water']
			]
		)
	}

	stage('Maven-to-OSGi') {
		try {
			// in preparation for building org.knime.update.bigdata.externals, this converts
			// performs the actual conversion from maven artifacts to OSGi bundles
			withMaven(maven: 'Maven 3.2') {
				sh '''
					pushd "$WORKSPACE"/git/knime-bigdata/com.knime.tpbuilder
					mvn clean scala:compile scala:run
					popd
				'''
			}

			// build org.knime.update.bigdata.externals
			withMaven(maven: 'Maven 3.2') {
				sh '''
					pushd "$WORKSPACE"/git/knime-bigdata/org.knime.bigdata.externals-parent
					mvn -Dknime-p2="$JENKINS_URL/jobs/''' + upstreamParams['org.knime.update.targetPlatform'].p2 + '''" clean package
					popd
				'''
			}

			// copy org.knime.update.bigdata.externals into a useful location for buckminster
			sh '''
				mv "$WORKSPACE"/git/knime-bigdata/org.knime.update.bigdata.externals/target/repository org.knime.update.bigdata.externals
				rm -rf .metadata buckminster.*
			'''

			// Local Spark: Download jars from maven into the libs/ folder of the local Spark plugin
			withMaven(maven: 'Maven 3.5') {
				sh '''
					rm -f "$WORKSPACE"/git/knime-bigdata/org.knime.bigdata.spark.local/libs/*.jar
					pushd "$WORKSPACE"/git/knime-bigdata/org.knime.bigdata.spark.local/libs/fetch_jars
					mvn clean package
					popd
				'''
			}
		} catch (ex) {
			if (currentBuild.result == null) {
				currentBuild.result = 'FAILED'
				emailext (
					subject: "FAILURE in ${env.JOB_NAME} [${env.BUILD_NUMBER}]",
					body: "Failure while building target platform. Check console output at ${env.BUILD_URL}/console",
					recipientProviders: [[$class: 'DevelopersRecipientProvider'], [$class: 'RequesterRecipientProvider']]
				)
			}
			throw ex
		}
	}

	stage('Build update site') {
		buckminster (
			component: 'org.knime.update.bigdata',
			baseline: [file: 'git/knime-config/org.knime.config/API-Baseline.target', name: 'Release 2018-07'],
			repos: [
				"$JENKINS_URL/jobs/${upstreamParams['org.knime.update.org'].p2}",
				"$JENKINS_URL/jobs/${upstreamParams['org.knime.update.labs'].p2}",
				"$JENKINS_URL/jobs/${upstreamParams['com.knime.update.pmml.compilation'].p2}",
				"file:///${WORKSPACE.replace('\\', '/')}/org.knime.update.bigdata.externals/"
			]
		)

		finalizeP2Repository()
	}

	stage('Archive artifacts') {
		archive()
		try {
			archiveArtifacts artifacts: "org.knime.update.bigdata.externals/**", fingerprint: true
		} catch (ex) {
			currentBuild.result = 'FAILED'
			emailext (
				subject: "FAILURE in ${env.JOB_NAME} [${env.BUILD_NUMBER}]",
				body: "Failure while archiving artifacts. Check console output at ${env.BUILD_URL}/console",
				recipientProviders: [[$class: 'DevelopersRecipientProvider'], [$class: 'RequesterRecipientProvider']]
			)
			throw ex
		}
	}
}
