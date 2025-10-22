from flask import Flask, request, jsonify
import requests
import base64
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import os
import uuid
import threading
import time

app = Flask(__name__)

# Dictionnaire pour stocker les résultats des callbacks en attente
# Format: {correlation_id: {'status': None, 'data': None, 'event': threading.Event()}}
pending_callbacks = {}

# Configuration du logging
def setup_logging():
    """Configure le système de logging"""
    # Créer le dossier logs s'il n'existe pas
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    # Configuration du format de log
    log_format = logging.Formatter(
        '[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
    )
    
    # Handler pour fichier avec rotation
    file_handler = RotatingFileHandler(
        'logs/mvola_api.log',
        maxBytes=10240000,  # 10MB
        backupCount=10
    )
    file_handler.setFormatter(log_format)
    file_handler.setLevel(logging.INFO)
    
    # Handler pour la console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)
    console_handler.setLevel(logging.DEBUG)
    
    # Configuration du logger de l'application
    app.logger.addHandler(file_handler)
    app.logger.addHandler(console_handler)
    app.logger.setLevel(logging.INFO)
    
    # Réduire les logs des bibliothèques externes
    logging.getLogger('werkzeug').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

setup_logging()

@app.before_request
def log_request_info():
    """Log les informations de chaque requête"""
    app.logger.info(f'Request: {request.method} {request.path} from {request.remote_addr}')

@app.route('/mvola/token', methods=['POST'])
def get_mvola_token():
    """
    Endpoint pour obtenir un token Mvola
    Attend une authentification Basic Auth avec consumerKey:consumerSecret
    """
    app.logger.info('=== Nouvelle demande de token Mvola ===')
    
    # Récupérer les credentials depuis Basic Auth
    auth = request.authorization
    
    if not auth or not auth.username or not auth.password:
        app.logger.warning('Tentative d\'accès sans authentification')
        return jsonify({
            'error': 'Authentication required',
            'message': 'Please provide consumerKey and consumerSecret via Basic Auth'
        }), 401
    
    consumer_key = auth.username
    app.logger.info(f'Consumer Key reçu: {consumer_key[:10]}...')
    
    # Créer l'authorization header pour Mvola API
    credentials = f"{consumer_key}:{auth.password}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()
    
    # Préparer la requête vers l'API Mvola
    mvola_url = 'https://devapi.mvola.mg/token'
    
    headers = {
        'Cache-Control': 'no-cache',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': f'Basic {encoded_credentials}'
    }
    
    data = {
        'grant_type': 'client_credentials',
        'scope': 'EXT_INT_MVOLA_SCOPE'
    }
    
    try:
        app.logger.info(f'Envoi de la requête vers {mvola_url}')
        start_time = datetime.now()
        
        # Envoyer la requête à l'API Mvola
        response = requests.post(mvola_url, headers=headers, data=data, timeout=30)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        app.logger.info(f'Réponse reçue de Mvola - Status: {response.status_code} - Durée: {duration}s')
        
        if response.status_code == 200:
            app.logger.info('✓ Token obtenu avec succès')
            response_data = response.json()
            
            # Extraire seulement l'access_token
            if 'access_token' in response_data:
                access_token = response_data['access_token']
                app.logger.info(f'Access token extrait: {access_token[:20]}...')
                return jsonify({'access_token': access_token}), 200
            else:
                app.logger.error('access_token non trouvé dans la réponse')
                return jsonify({
                    'error': 'Invalid response',
                    'message': 'access_token non trouvé dans la réponse de Mvola'
                }), 500
        else:
            app.logger.warning(f'⚠ Échec de l\'obtention du token - Status: {response.status_code}')
            app.logger.debug(f'Réponse: {response.text}')
            return jsonify(response.json()), response.status_code
        
    except requests.exceptions.Timeout:
        app.logger.error('Timeout lors de la requête vers Mvola API')
        return jsonify({
            'error': 'Request timeout',
            'message': 'La requête vers Mvola a expiré'
        }), 504
        
    except requests.exceptions.RequestException as e:
        app.logger.error(f'Erreur lors de la requête: {str(e)}', exc_info=True)
        return jsonify({
            'error': 'Request failed',
            'message': str(e)
        }), 500
    
    except Exception as e:
        app.logger.error(f'Erreur inattendue: {str(e)}', exc_info=True)
        return jsonify({
            'error': 'Internal server error',
            'message': 'Une erreur inattendue s\'est produite'
        }), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Endpoint de vérification de santé"""
    app.logger.debug('Health check appelé')
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()}), 200

@app.route('/mvola/callback', methods=['PUT'])
def mvola_callback():
    """
    Endpoint pour recevoir les callbacks de Mvola
    Mvola envoie le statut final de la transaction via PUT
    """
    app.logger.info('=== Callback Mvola reçu ===')
    
    # Récupérer les headers importants
    correlation_id = request.headers.get('X-CorrelationID', 'N/A')
    content_type = request.headers.get('Content-Type', 'N/A')
    
    app.logger.info(f'X-CorrelationID: {correlation_id}')
    app.logger.info(f'Content-Type: {content_type}')
    
    # Récupérer le body de la requête
    try:
        callback_data = request.get_json()
        
        if not callback_data:
            app.logger.warning('Callback reçu sans données JSON')
            return jsonify({
                'status': 'error',
                'message': 'No JSON data received'
            }), 400
        
        # Logger toutes les données reçues
        app.logger.info(f'Données callback complètes: {callback_data}')
        
        # Extraire les informations importantes
        status = callback_data.get('status', 'UNKNOWN')
        transaction_ref = callback_data.get('serverCorrelationId', 
                                          callback_data.get('transactionReference', 'N/A'))
        
        app.logger.info(f'Status de la transaction: {status}')
        app.logger.info(f'Référence transaction: {transaction_ref}')
        
        # Logger selon le statut
        if status == 'SUCCESS' or status == 'success':
            app.logger.info('✅ TRANSACTION RÉUSSIE !')
            amount = callback_data.get('amount', 'N/A')
            app.logger.info(f'Montant: {amount}')
        elif status == 'FAILED' or status == 'failed':
            app.logger.error('❌ TRANSACTION ÉCHOUÉE !')
            error_msg = callback_data.get('errorMessage', 
                                         callback_data.get('message', 'Erreur inconnue'))
            app.logger.error(f'Raison: {error_msg}')
        elif status == 'PENDING' or status == 'pending':
            app.logger.info('⏳ Transaction en attente...')
        else:
            app.logger.warning(f'⚠️ Statut inconnu: {status}')
        
        # Sauvegarder dans un fichier de log spécifique pour les callbacks
        callback_log_file = 'logs/mvola_callbacks.log'
        with open(callback_log_file, 'a', encoding='utf-8') as f:
            log_entry = {
                'timestamp': datetime.now().isoformat(),
                'correlation_id': correlation_id,
                'status': status,
                'data': callback_data
            }
            f.write(f'{log_entry}\n')
        
        # Notifier la transaction en attente si elle existe
        if correlation_id in pending_callbacks:
            app.logger.info(f'Notification de la transaction en attente: {correlation_id}')
            pending_callbacks[correlation_id]['status'] = status
            pending_callbacks[correlation_id]['data'] = callback_data
            pending_callbacks[correlation_id]['event'].set()  # Débloquer l'attente
        else:
            app.logger.warning(f'Aucune transaction en attente trouvée pour: {correlation_id}')
        
        # Répondre à Mvola pour confirmer la réception
        return jsonify({
            'status': 'received',
            'message': 'Callback traité avec succès',
            'correlation_id': correlation_id
        }), 200
        
    except Exception as e:
        app.logger.error(f'Erreur lors du traitement du callback: {str(e)}', exc_info=True)
        return jsonify({
            'status': 'error',
            'message': 'Erreur lors du traitement du callback'
        }), 500

@app.route('/mvola/transaction', methods=['POST'])
def create_mvola_transaction():
    """
    Endpoint pour créer une transaction Mvola
    Attend un Bearer token et les données de transaction dans le body JSON
    """
    app.logger.info('=== Nouvelle demande de transaction Mvola ===')
    
    # Récupérer le token Bearer depuis le header Authorization
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        app.logger.warning('Tentative d\'accès sans token Bearer')
        return jsonify({
            'error': 'Authentication required',
            'message': 'Please provide Bearer token in Authorization header'
        }), 401
    
    access_token = auth_header.replace('Bearer ', '')
    app.logger.info(f'Token Bearer reçu: {access_token[:20]}...')
    
    # Récupérer les données JSON du body
    data = request.get_json()
    if not data:
        app.logger.warning('Aucune donnée JSON fournie')
        return jsonify({
            'error': 'Invalid request',
            'message': 'JSON body required'
        }), 400
    
    # Valider les champs obligatoires
    required_fields = ['amount', 'clientMsisdn', 'partnerMsisdn', 
                      'descriptionTransaction', 'referenceID', 'name']
    missing_fields = [field for field in required_fields if field not in data]
    
    if missing_fields:
        app.logger.warning(f'Champs manquants: {missing_fields}')
        return jsonify({
            'error': 'Missing required fields',
            'message': f'Les champs suivants sont obligatoires: {", ".join(missing_fields)}'
        }), 400
    
    # Générer X-CorrelationID et requestDate si non fournis
    x_correlation_id = data.get('xCorrelationID', datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3])
    request_date = data.get('requestDate', datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z')
    
    # Créer un Event pour attendre le callback
    callback_event = threading.Event()
    pending_callbacks[x_correlation_id] = {
        'status': None,
        'data': None,
        'event': callback_event
    }
    
    app.logger.info(f'Transaction - Amount: {data["amount"]} Ar, Client: {data["clientMsisdn"]}, Partner: {data["partnerMsisdn"]}')
    app.logger.info(f'X-CorrelationID: {x_correlation_id}')
    
    # Préparer la requête vers l'API Mvola
    mvola_url = 'https://devapi.mvola.mg/mvola/mm/transactions/type/merchantpay/1.0.0/'
    
    # Construire l'URL de callback (utiliser l'URL publique de votre serveur)
    # Pour le développement local, vous devrez utiliser ngrok ou un serveur public
    base_url = request.host_url.rstrip('/')
    callback_url = data.get('callbackUrl', f'{base_url}/mvola/callback')
    
    app.logger.info(f'Callback URL: {callback_url}')
    
    headers = {
        'version': '1.0',
        'UserLanguage': 'MG',
        'X-CorrelationID': x_correlation_id,
        'X-Callback-URL': callback_url,
        'Accept-Charset': 'utf-8',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }
    
    # Construire le payload pour Mvola
    payload = {
        "amount": str(data['amount']),
        "currency": "Ar",
        "descriptionText": data['descriptionTransaction'],
        "requestingOrganisationTransactionReference": data['referenceID'],
        "requestDate": request_date,
        "originalTransactionReference": f"APP_{x_correlation_id}",
        "debitParty": [
            {
                "key": "msisdn",
                "value": data['clientMsisdn']
            }
        ],
        "creditParty": [
            {
                "key": "msisdn",
                "value": data['partnerMsisdn']
            }
        ],
        "metadata": [
            {
                "key": "partnerName",
                "value": f"APP_{data['name']}"
            },
            {
                "key": "fc",
                "value": "Ar"
            },
            {
                "key": "amountFc",
                "value": "1"
            }
        ]
    }
    
    try:
        app.logger.info(f'Envoi de la transaction vers {mvola_url}')
        start_time = datetime.now()
        
        # Envoyer la requête à l'API Mvola
        response = requests.post(mvola_url, headers=headers, json=payload, timeout=30)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        app.logger.info(f'Réponse reçue de Mvola - Status: {response.status_code} - Durée: {duration}s')
        
        if response.status_code in [200, 201, 202]:
            if response.status_code == 202:
                app.logger.info('✓ Transaction acceptée (en cours de traitement asynchrone)')
                app.logger.info('⏳ Attente du callback de Mvola (timeout: 60 secondes)...')
                
                # Attendre le callback pendant 60 secondes maximum
                callback_received = callback_event.wait(timeout=60)
                
                if callback_received:
                    # Callback reçu
                    callback_result = pending_callbacks[x_correlation_id]
                    app.logger.info(f'✅ Callback reçu avec statut: {callback_result["status"]}')
                    
                    # Nettoyer le dictionnaire
                    del pending_callbacks[x_correlation_id]
                    
                    # Retourner le résultat du callback
                    return jsonify({
                        'status': callback_result['status'],
                        'correlation_id': x_correlation_id,
                        'callback_data': callback_result['data']
                    }), 200
                else:
                    # Timeout - callback non reçu
                    app.logger.warning('⚠️ Timeout: callback non reçu dans les 60 secondes')
                    
                    # Nettoyer le dictionnaire
                    del pending_callbacks[x_correlation_id]
                    
                    return jsonify({
                        'status': 'TIMEOUT',
                        'message': 'Transaction acceptée mais callback non reçu dans le délai imparti',
                        'correlation_id': x_correlation_id,
                        'initial_response': response.json()
                    }), 202
            else:
                app.logger.info('✓ Transaction créée avec succès')
                # Nettoyer si présent
                if x_correlation_id in pending_callbacks:
                    del pending_callbacks[x_correlation_id]
                return jsonify(response.json()), response.status_code
        else:
            app.logger.warning(f'⚠ Échec de la transaction - Status: {response.status_code}')
            app.logger.debug(f'Réponse: {response.text}')
            # Nettoyer si présent
            if x_correlation_id in pending_callbacks:
                del pending_callbacks[x_correlation_id]
            return jsonify(response.json()), response.status_code
        
    except requests.exceptions.Timeout:
        app.logger.error('Timeout lors de la requête vers Mvola API')
        return jsonify({
            'error': 'Request timeout',
            'message': 'La requête vers Mvola a expiré'
        }), 504
        
    except requests.exceptions.RequestException as e:
        app.logger.error(f'Erreur lors de la requête: {str(e)}', exc_info=True)
        return jsonify({
            'error': 'Request failed',
            'message': str(e)
        }), 500
    
    except Exception as e:
        app.logger.error(f'Erreur inattendue: {str(e)}', exc_info=True)
        return jsonify({
            'error': 'Internal server error',
            'message': 'Une erreur inattendue s\'est produite'
        }), 500

@app.errorhandler(404)
def not_found(error):
    app.logger.warning(f'Route non trouvée: {request.path}')
    return jsonify({'error': 'Not found', 'message': 'Route non trouvée'}), 404

@app.errorhandler(500)
def internal_error(error):
    app.logger.error(f'Erreur serveur: {str(error)}')
    return jsonify({'error': 'Internal server error'}), 500

if __name__ == '__main__':
    app.logger.info('🚀 Démarrage de l\'application Mvola Token API')
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=False, host='0.0.0.0', port=port)