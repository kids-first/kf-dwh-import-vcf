#!groovy
properties([
    pipelineTriggers([[$class:"SCMTrigger", scmpoll_spec:"* * * * *"]])
])

pipeline {
  agent { label 'terraform-testing' }

  stages {
    stage('Get Code'){
      steps {
        deleteDir()
        checkout scm
        slackResponse = slackSend (color: '#FFFF00', message: "${projectName}:sweat_smile: Starting to deploy to ${syslevel}: Branch '${env.BRANCH_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
      }
    }
    stage("Create ssh_config file") {
      steps {
        sh '''
        mkdir config/
        echo "Host *" > config/sshd_config
        echo "SendEnv LANG LC_*\n HashKnownHosts yes \n GSSAPIAuthentication yes \n GSSAPIDelegateCredentials no \n" >> config/sshd_config
        '''
      }
    }
    stage('deploy to qa'){
      when {
        expression {
          return env.BRANCH_NAME == 'master';
        }
      }
      steps{
        pending("${env.JOB_NAME}","prd","${slackResponse.threadId}")
        sh '''
           ./deploy.sh $(git log -n 1 --pretty=format:'%h') qa
          '''
        success("${env.JOB_NAME}","prd","${slackResponse.threadId}")
      }
      post {
        failure {
          fail("${env.JOB_NAME}","prd","${slackResponse.threadId}")
        }
      }
    }
    stage('deploy to prd'){
      when {
        buildingTag()
      }
      steps{
        pending("${env.JOB_NAME}","prd","${slackResponse.threadId}")
        sh '''
           ./deploy.sh $(git tag --sort version:refname | tail -1) prd
          '''
        success("${env.JOB_NAME}","prd","${slackResponse.threadId}")
      }
      post {
        failure {
          fail("${env.JOB_NAME}","prd","${slackResponse.threadId}")
        }
      }
    }
    stage('test JAR'){
      when {
        expression {
          return env.BRANCH_NAME != 'master';
        }
      }
      steps{
        pending("${env.JOB_NAME}","prd","${slackResponse.threadId}")
        sh '''
           ./test.sh
          '''
        success("${env.JOB_NAME}","prd","${slackResponse.threadId}")
      }
      post {
        failure {
          fail("${env.JOB_NAME}","prd","${slackResponse.threadId}")
        }
      }
    }
    stage('Finished Building') {
      steps{
        success("${env.JOB_NAME}","prd","${slackResponse.threadId}")
      }
    }
  }
}

void success(projectName,syslevel,channel="jenkins-kf") {
//   sendStatusToGitHub(projectName,"success")
  slackSend (color: '#00FF00', channel: "${channel}", message: "${projectName}:smile: Deployed to ${syslevel}: Branch '${env.BRANCH_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
}

void fail(projectName,syslevel,channel="jenkins-kf") {
//   sendStatusToGitHub(projectName,"failure")
  slackSend (color: '#ff0000', channel: "${channel}", message: "${projectName}:frowning: Deployed to ${syslevel} Failed: Branch '${env.BRANCH_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})", replyBroadcast: true)
}

void pending(projectName, syslevel,channel="jenkins-kf") {
  //sendStatusToGitHub(projectName, "pending")
  slackSend (color: '#FFFF00', channel: "${channel}", message: "${projectName}:sweat_smile:Starting to deploy to ${syslevel}: Branch '${env.BRANCH_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
}

void sendStatusToGitHub(projectName,status) {
sh """
export VAULT_ADDR='https://vault-dev.kids-first.io'
vault auth -method=aws role=aws-infra-jenkins_devops
"""
env.GITHUB_TOKEN = sh(script: "export VAULT_ADDR='https://vault-dev.kids-first.io' && set +x &&  vault read -field=value /secret/devops/jenkins-kf-github-token && set -x", returnStdout: true)
sh """
 set +x
 curl 'https://api.github.com/repos/kids-first/kf-lib-shiro-ego-realm/statuses/$GIT_COMMIT?access_token=$GITHUB_TOKEN' -H 'Content-Type: application/json' -X POST -d '{\"state\": \"${status}\", \"description\": \"Jenkins\", \"target_url\": \"$BUILD_URL\"}'
 set -x
"""
}
