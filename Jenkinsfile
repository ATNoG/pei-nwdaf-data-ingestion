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
                sh 'docker context use deploy'
                sh 'docker compose down --remove-orphans || true'
                sh 'docker compose --env-file .env build --no-cache'
                sh 'docker compose --env-file .env up -d'
                sh 'docker context use default'
            }
        }

        stage('Verify Deployment') {
            steps {
                sh 'docker context use deploy'
                sh 'docker compose ps'
                sh 'docker context use default'
            }
        }
    }

    post {
        always {
            sh 'rm -f .env'
            sh 'docker context use default || true'
        }
        success {
            echo 'Deployment completed successfully!'
        }
        failure {
            echo 'Deployment failed. Check logs above.'
            sh 'docker context use default || true'
        }
    }
}
