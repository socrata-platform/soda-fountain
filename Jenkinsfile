// Set up the libraries
@Library('socrata-pipeline-library')

// set up variables
def project_wd = 'soda-fountain-jetty'
def isPr = env.CHANGE_ID != null;
def publishStage = false;

// instanciate libraries
def sbtbuild = new com.socrata.SBTBuild(steps, 'soda-fountain', project_wd)
def dockerize = new com.socrata.Dockerize(steps, 'soda-fountain', env.BUILD_NUMBER)

pipeline {
  options {
    ansiColor('xterm')
  }
  parameters {
    booleanParam(name: 'RELEASE_BUILD', defaultValue: false, description: 'Are we building a release candidate?')
    booleanParam(name: 'RELEASE_DRY_RUN', defaultValue: false, description: 'To test out the release build without creating a new tag.')
    string(name: 'AGENT', defaultValue: 'build-worker', description: 'Which build agent to use?')
    string(name: 'BRANCH_SPECIFIER', defaultValue: 'origin/main', description: 'Use this branch for building the artifact.')
  }
  agent {
    label params.AGENT
  }
  environment {
    SERVICE = 'soda-fountain'
    DOCKER_PATH = './docker'
  }
  stages {
    stage('Release Tag') {
      when {
        expression { return params.RELEASE_BUILD }
      }
      steps {
        script {
          if (params.RELEASE_DRY_RUN) {
            echo 'DRY RUN: Skipping release tag creation'
          }
          else {
            // get a list of all files changes since the last tag
            files = sh(returnStdout: true, script: "git diff --name-only HEAD `git describe --match \"v*\" --abbrev=0`").trim()
            echo "Files changed:\n${files}"

            // the release build process changes the version file, so it will always be changed
            // if there are changes in addition to the version file, then we publish them as part of the release build process
            if (files != 'version.sbt') {
              publishStage = true
            }

            echo 'Running sbt-release'
            // The git config setup required for your project prior to running 'sbt release with-defaults' may vary:
            sh(returnStdout: true, script: "git config remote.origin.fetch +refs/heads/*:refs/remotes/origin/*")
            sh(returnStdout: true, script: "git config branch.main.remote origin")
            sh(returnStdout: true, script: "git config branch.main.merge refs/heads/main")

            echo sh(returnStdout: true, script: "echo y | sbt \"release with-defaults\"")
          }

          echo 'Getting release tag'
          release_tag = sh(returnStdout: true, script: "git describe --abbrev=0 --match \"v*\"").trim()
          branchSpecifier = "refs/tags/${release_tag}"
          echo branchSpecifier

          // checkout the tag so we're performing subsequent actions on it
          sh "git checkout ${branchSpecifier}"
        }
      }
    }
    stage('Build') {
      steps {
        script {
          // perform any needed modifiers on the build parameters here
          sbtbuild.setNoSubproject(true)
          sbtbuild.setScalaVersion("2.12")
          sbtbuild.setSubprojectName("sodaFountainJetty")

          // build
          echo "Building sbt project..."
          sbtbuild.build()

          // set build description
          env.SERVICE_VERSION = sbtbuild.getServiceVersion()
          currentBuild.description = "${env.SERVICE}:${env.SERVICE_VERSION}_${env.BUILD_NUMBER}_${env.GIT_COMMIT.take(8)}"
        }
      }
	  }
    stage('Publish') {
      when {
        expression { publishStage }
      }
      steps {
        script {
          echo "Publishing external library"
          sbtbuild.setSubprojectName("sodaFountainExternal")
          sbtbuild.setPublish(true)
          sbtbuild.setBuildType("library")
          sbtbuild.build()
        }
      }
    }
    stage('Dockerize') {
      when {
        not { expression { isPr } }
      }
      steps {
        script {
          echo "Building docker container..."
          dockerize.docker_build(env.SERVICE_VERSION, env.GIT_COMMIT, "./docker", sbtbuild.getDockerArtifact())
          env.DOCKER_TAG = dockerize.getDeployTag()
        }
      }
      post {
        success {
          script {
            if (params.RELEASE_BUILD){
              echo env.DOCKER_TAG // For now, just print the deploy tag in the console output -- later, communicate to release metadata service
            }
          }
        }
      }
    }
    stage('Deploy') {
      when {
        not { expression { isPr } }
        not { expression { return params.RELEASE_BUILD } }
      }
      steps {
        script {
          // uses env.SERVICE and env.DOCKER_TAG, deploys to staging by default
          marathonDeploy()
        }
      }
    }
  }
}
