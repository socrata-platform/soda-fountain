// Set up the libraries
@Library('socrata-pipeline-library')

// set up service and project variables
def service = "soda-fountain"
def project_wd = "soda-fountain-jetty"
def deploy_service_pattern = "soda-fountain"
def deploy_environment = "staging"
def default_branch_specifier = "origin/main"

def service_sha = env.GIT_COMMIT

// variables that determine which stages we run based on what triggered the job
def boolean stage_cut = false
def boolean stage_build = false
def boolean stage_dockerize = false
def boolean stage_deploy = false
def boolean stage_publish = false

// instanciate libraries
def sbtbuild = new com.socrata.SBTBuild(steps, service, project_wd)
def dockerize = new com.socrata.Dockerize(steps, service, BUILD_NUMBER)
def deploy = new com.socrata.MarathonDeploy(steps)

pipeline {
  options {
    ansiColor('xterm')
  }
  parameters {
    booleanParam(name: 'RELEASE_CUT', defaultValue: false, description: 'Are we cutting a new release candidate?')
    booleanParam(name: 'FORCE_DOCKERIZE', defaultValue: false, description: 'Are we forcing a docker build?')
    string(name: 'AGENT', defaultValue: 'build-worker-pg13', description: 'Which build agent to use?')
    string(name: 'BRANCH_SPECIFIER', defaultValue: default_branch_specifier, description: 'Use this branch for building the artifact.')
  }
  agent {
    label params.AGENT
  }
  environment {
    PATH = "${WORKER_PATH}"
  }

  stages {
    stage('Setup') {
      steps {
        script {
          // check to see if we want to use a non-standard branch and check out the repo
          if (params.BRANCH_SPECIFIER == default_branch_specifier) {
            checkout scm
          } else {
            def scmRepoUrl = scm.getUserRemoteConfigs()[0].getUrl()
            checkout ([
              $class: 'GitSCM',
              branches: [[name: params.BRANCH_SPECIFIER ]],
              userRemoteConfigs: [[ url: scmRepoUrl ]]
            ])
          }

          // set the service sha to what was checked out (GIT_COMMIT isn't always set)
          service_sha = sh(returnStdout: true, script: "git rev-parse HEAD").trim()

          // if we need to force a docker build
          if (params.FORCE_DOCKERIZE) {
            stage_build = true
            stage_dockerize = true
          }

          // determine what triggered the build and what stages need to be run
          if (params.RELEASE_CUT) { // RELEASE_CUT parameter was set by a cut job
            stage_cut = true // stage_publish is turned on in the cut step as needed
            stage_build = true
            stage_dockerize = true
            stage_deploy = true
            deploy_environment = "rc"
          }
          else if (env.CHANGE_ID != null) { // we're running a PR builder
            stage_build = true
          }
          else if (BRANCH_NAME == "main") { // we're running a build on main branch to deploy to staging
            stage_build = true
            stage_dockerize = true
            stage_deploy = true
          }
          else {
            // we're not sure what we're doing...
            echo "Unknown build trigger - Exiting as Failure"
            currentBuild.result = 'FAILURE'
            return
          }
        }
      }
    }
    stage('Cut') {
      when { expression { stage_cut } }
      steps {
        script {
          // get a list of all files changes since the last tag
          files = sh(returnStdout: true, script: "git diff --name-only HEAD `git describe --match \"v*\" --abbrev=0`").trim()
          echo "Files changed:\n${files}"

          if (files == 'version.sbt') {
            echo 'No changes to the repo.  Rebuilding using the latest tag.'
            // For release cuts, we want to rebuild and deploy even if there are no changes to the repo to pick up base layer changes for aqua compliance
            stage_publish = false // artifactory does not let us publish without changes
          }
          else {
            echo 'Running sbt-release'

            // The git config setup required for your project prior to running 'sbt release with-defaults' may vary:
            sh(returnStdout: true, script: "git config remote.origin.fetch +refs/heads/*:refs/remotes/origin/*")
            sh(returnStdout: true, script: "git config branch.main.remote origin")
            sh(returnStdout: true, script: "git config branch.main.merge refs/heads/main")

            echo sh(returnStdout: true, script: "echo y | sbt \"release with-defaults\"")

            stage_publish = true
          }

          echo 'Getting release tag'
          release_tag = sh(returnStdout: true, script: "git describe --abbrev=0 --match \"v*\"").trim()
          branchSpecifier = "refs/tags/${release_tag}"
          echo branchSpecifier

          // checkout the tag so we're performing subsequent actions on it
          sh "git checkout ${branchSpecifier}"

          // set the service_sha to the current tag because it might not be the same as env.GIT_COMMIT
          service_sha = sh(returnStdout: true, script: "git rev-parse HEAD").trim()
        }
      }
    }
    stage('Build') {
      when { expression { stage_build } }
      steps {
        script {
          // perform any needed modifiers on the build parameters here
          sbtbuild.setNoSubproject(true)
          sbtbuild.setScalaVersion("2.12")
          sbtbuild.setSubprojectName("sodaFountainJetty")

          // build
          echo "Building sbt project..."
          sbtbuild.build()
        }
      }
	}
    stage('Publish') {
      when { expression { stage_publish } }
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
      when { expression { stage_dockerize } }
      steps {
        script {
          echo "Building docker container..."
          dockerize.docker_build(sbtbuild.getServiceVersion(), service_sha, "./docker", sbtbuild.getDockerArtifact())
        }
      }
    }
    stage('Deploy') {
      when { expression { stage_deploy } }
      steps {
        script {
          // Checkout and run bundle install in the apps-marathon repo
          deploy.checkoutAndInstall()

          // deploy the service to the specified environment
          deploy.deploy(deploy_service_pattern, deploy_environment, dockerize.getDeployTag())
        }
      }
    }
  }
}
