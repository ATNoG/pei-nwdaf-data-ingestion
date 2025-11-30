pipeline {
    agent any

    stages {
        stage('Prepare Environment') {
            steps {
                sh 'cp /var/lib/jenkins/.env .env'
                sh 'ls -la .env'
            }
        }

        stage('Deploy Infrastructure') {
            steps {
                sh '''
                    docker --context deploy compose down --remove-orphans || true
                    docker --context deploy compose --env-file .env build --no-cache
                    docker --context deploy compose --env-file .env up -d
                '''
            }
        }

        stage('Verify Deployment') {
            steps {
                sh 'docker --context deploy compose ps'
            }
        }
    }

    post {
        always {
            sh 'rm -f .env'
        }
        success {
            echo 'Deployment completed successfully!'
        }
        failure {
            echo 'Deployment failed. Check logs above.'
        }
    }
}
