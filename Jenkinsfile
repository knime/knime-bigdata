#!groovy

library "knime-pipeline@$BRANCH_NAME"

node {
	def upstreamParams = defaultProperties('org.knime.update.analytics-platform',
		'com.knime.update.pmml.compilation',
		'org.knime.update.targetPlatform')


	stage('Clean workspace') {
		cleanWorkspace()
		try {
			sh 'rm -rf git/knime-bigdata/com.knime.tpbuilder/target com.knime.update.bigdata.externals'
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
				[name : 'knime-config'],
				[name : 'knime-jenkins']
			]
		)
	}

	stage('Maven-to-OSGi') {
		try {
			withMaven(maven: 'Maven 3.2') {
				sh '''
					pushd "$WORKSPACE"/git/knime-bigdata/com.knime.tpbuilder
					mvn clean scala:compile scala:run
					popd
				'''
			}

			withMaven(maven: 'Maven 3.2') {
				sh '''
					pushd "$WORKSPACE"/git/knime-bigdata/com.knime.bigdata.externals-parent
					mvn -Dknime-p2="$JENKINS_URL/jobs/''' + upstreamParams['org.knime.update.targetPlatform'].p2 + '''" clean package
					popd
				'''
			}

			sh '''
				mv "$WORKSPACE"/git/knime-bigdata/com.knime.update.bigdata.externals/target/repository com.knime.update.bigdata.externals
				rm -rf .metadata buckminster.*
			'''
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
			component: 'com.knime.update.bigdata',
			baseline: [file: 'git/knime-config/org.knime.config/API-Baseline.target', name: 'Release 3.4'],
			repos: [
				"$JENKINS_URL/jobs/${upstreamParams['org.knime.update.analytics-platform'].p2}",
				"$JENKINS_URL/jobs/${upstreamParams['com.knime.update.pmml.compilation'].p2}",
				"file:///${WORKSPACE.replace('\\', '/')}/com.knime.update.bigdata.externals/"
			]
		)

		finalizeP2Repository()
	}

	stage('Archive artifacts') {
		archive()
		try {
			archiveArtifacts artifacts: "com.knime.update.bigdata.externals/**", fingerprint: true
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
