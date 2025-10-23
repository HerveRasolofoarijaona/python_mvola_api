from flask import Flask, request, jsonify
import requests
import base64
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import os
import time
import threading

app = Flask(__name__)

# Configuration - URL de base Mvola depuis variable d'environnement
MVOLA_BASE_URL = os.environ.get('MVOLA_BASE_URL', 'https://devapi.mvola.mg')

# Dictionnaire pour stocker les résultats des callbacks
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
    mvola_url = f'{MVOLA_BASE_URL}/token'
    
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
        app.logger.debug(f'Réponse complète Mvola Token: {response.text}')
        
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
    """
    app.logger.info('=== Callback Mvola reçu ===')
    
    try:
        callback_data = request.get_json()
        
        if not callback_data:
            app.logger.warning('Callback reçu sans données JSON')
            return jsonify({
                'status': 'error',
                'message': 'No JSON data received'
            }), 400
        
        app.logger.info(f'Données callback complètes: {callback_data}')
        
        # Extraire le X-CorrelationID depuis les metadata
        correlation_id = 'N/A'
        metadata = callback_data.get('metadata', [])
        for meta in metadata:
            if meta.get('key') == 'XCorrelationId':
                correlation_id = meta.get('value', 'N/A')
                break
        
        app.logger.info(f'X-CorrelationID extrait: {correlation_id}')
        
        # Extraire le statut
        transaction_status = callback_data.get('transactionStatus', 'UNKNOWN')
        status_mapping = {
            'completed': 'SUCCESS',
            'failed': 'FAILED',
            'pending': 'PENDING'
        }
        status = status_mapping.get(transaction_status.lower(), transaction_status.upper())
        
        app.logger.info(f'Status de la transaction: {status}')
        
        # Notifier la transaction en attente si elle existe
        if correlation_id in pending_callbacks:
            app.logger.info(f'✅ Notification de la transaction en attente: {correlation_id}')
            pending_callbacks[correlation_id]['status'] = status
            pending_callbacks[correlation_id]['data'] = callback_data
            pending_callbacks[correlation_id]['event'].set()  # Débloquer l'attente
        else:
            app.logger.warning(f'Aucune transaction en attente trouvée pour: {correlation_id}')
        
        # Sauvegarder dans un fichier de log
        callback_log_file = 'logs/mvola_callbacks.log'
        with open(callback_log_file, 'a', encoding='utf-8') as f:
            log_entry = {
                'timestamp': datetime.now().isoformat(),
                'correlation_id': correlation_id,
                'status': status,
                'data': callback_data
            }
            f.write(f'{log_entry}\n')
        
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

def check_transaction_status(access_token, server_correlation_id, x_correlation_id, partner_msisdn, partner_name):
    """
    Vérifie le statut d'une transaction via l'API Mvola
    
    Args:
        access_token: Token d'authentification
        server_correlation_id: ID de corrélation serveur retourné par Mvola
        x_correlation_id: ID de corrélation original
        partner_msisdn: Numéro du partenaire
        partner_name: Nom du partenaire
        
    Returns:
        dict: Réponse contenant le statut ou None en cas d'erreur
    """
    status_url = f'{MVOLA_BASE_URL}/mvola/mm/transactions/type/merchantpay/1.0.0/status/{server_correlation_id}'
    
    headers = {
        'Version': '1.0',
        'X-CorrelationID': x_correlation_id,
        'UserLanguage': 'FR',
        'UserAccountIdentifier': f'msisdn;{partner_msisdn}',
        'partnerName': f'APP_{partner_name}',
        'Authorization': f'Bearer {access_token}'
    }
    
    try:
        app.logger.info(f'Vérification du statut: {status_url}')
        response = requests.get(status_url, headers=headers, timeout=10)
        
        app.logger.info(f'Status check - HTTP {response.status_code}')
        app.logger.debug(f'Réponse Status API: {response.text}')
        
        if response.status_code == 200:
            response_data = response.json()
            app.logger.info(f'Données Status reçues: {response_data}')
            return response_data
        else:
            app.logger.warning(f'Status check failed: {response.text}')
            return None
            
    except Exception as e:
        app.logger.error(f'Erreur lors de la vérification du statut: {str(e)}')
        return None

def get_transaction_details(access_token, object_reference, x_correlation_id, partner_msisdn, partner_name):
    """
    Récupère les détails d'une transaction via l'API Mvola
    
    Args:
        access_token: Token d'authentification
        object_reference: Référence de l'objet transaction
        x_correlation_id: ID de corrélation
        partner_msisdn: Numéro du partenaire
        partner_name: Nom du partenaire
        
    Returns:
        dict: Détails de la transaction ou None en cas d'erreur
    """
    details_url = f'{MVOLA_BASE_URL}/mvola/mm/transactions/type/merchantpay/1.0.0/{object_reference}'
    
    headers = {
        'Version': '1.0',
        'X-CorrelationID': x_correlation_id,
        'UserLanguage': 'FR',
        'UserAccountIdentifier': f'msisdn;{partner_msisdn}',
        'partnerName': f'APP_{partner_name}',
        'Authorization': f'Bearer {access_token}'
    }
    
    try:
        app.logger.info(f'Récupération des détails: {details_url}')
        response = requests.get(details_url, headers=headers, timeout=10)
        
        app.logger.info(f'Details fetch - HTTP {response.status_code}')
        app.logger.debug(f'Réponse Details API: {response.text}')
        
        if response.status_code == 200:
            response_data = response.json()
            app.logger.info(f'Détails transaction reçus: {response_data}')
            return response_data
        else:
            app.logger.warning(f'Details fetch failed: {response.text}')
            return None
            
    except Exception as e:
        app.logger.error(f'Erreur lors de la récupération des détails: {str(e)}')
        return None

@app.route('/mvola/transaction', methods=['POST'])
def create_mvola_transaction():
    """
    Endpoint pour créer une transaction Mvola avec polling du statut
    """
    app.logger.info('=== Nouvelle demande de transaction Mvola ===')
    
    # Récupérer le token Bearer
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        app.logger.warning('Tentative d\'accès sans token Bearer')
        return jsonify({
            'error': 'Authentication required',
            'message': 'Please provide Bearer token in Authorization header'
        }), 401
    
    access_token = auth_header.replace('Bearer ', '')
    app.logger.info(f'Token Bearer reçu: {access_token[:20]}...')
    
    # Récupérer les données JSON
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
    
    # Générer X-CorrelationID et requestDate
    x_correlation_id = data.get('xCorrelationID', datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3])
    request_date = data.get('requestDate', datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z')
    
    app.logger.info(f'Transaction - Amount: {data["amount"]} Ar, Client: {data["clientMsisdn"]}, Partner: {data["partnerMsisdn"]}')
    app.logger.info(f'X-CorrelationID: {x_correlation_id}')
    
    # Préparer la requête vers l'API Mvola
    mvola_url = f'{MVOLA_BASE_URL}/mvola/mm/transactions/type/merchantpay/1.0.0/'
    
    base_url = request.host_url.rstrip('/')
    callback_url = data.get('callbackUrl', f'{base_url}/mvola/callback')
    
    headers = {
        'version': '1.0',
        'UserLanguage': 'MG',
        'X-CorrelationID': x_correlation_id,
        'X-Callback-URL': callback_url,
        'Accept-Charset': 'utf-8',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }
    
    # Construire le payload
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
        app.logger.debug(f'Réponse complète Mvola Transaction: {response.text}')
        
        if response.status_code in [200, 201, 202]:
            response_data = response.json()
            app.logger.info(f'Données transaction initiales: {response_data}')
            
            server_correlation_id = response_data.get('serverCorrelationId')
            
            if not server_correlation_id:
                app.logger.error('serverCorrelationId manquant dans la réponse')
                return jsonify(response_data), response.status_code
            
            app.logger.info(f'✓ Transaction acceptée - Server Correlation ID: {server_correlation_id}')
            
            # Créer un Event pour attendre le callback
            callback_event = threading.Event()
            pending_callbacks[x_correlation_id] = {
                'status': None,
                'data': None,
                'event': callback_event
            }
            
            app.logger.info('⏳ Attente du callback (max 20 secondes)...')
            
            # Attendre le callback pendant 20 secondes maximum
            callback_received = callback_event.wait(timeout=20)
            
            if callback_received:
                # Callback reçu avant 20 secondes !
                callback_result = pending_callbacks[x_correlation_id]
                app.logger.info(f'✅ Callback reçu avec statut: {callback_result["status"]}')
                
                # Nettoyer le dictionnaire
                del pending_callbacks[x_correlation_id]
                
                # Extraire les informations du callback
                callback_data = callback_result['data']
                
                # Construire la réponse depuis le callback
                response_data = {
                    'status': callback_result['status'],
                    'transactionReference': callback_data.get('transactionReference'),
                    'serverCorrelationId': callback_data.get('serverCorrelationId'),
                    'requestDate': callback_data.get('requestDate'),
                    'debitParty': callback_data.get('debitParty', []),
                    'creditParty': callback_data.get('creditParty', []),
                    'fees': callback_data.get('fees', []),
                    'amount': callback_data.get('amount'),
                    'xCorrelationId': x_correlation_id,
                    'source': 'callback'
                }
                
                return jsonify(response_data), 200
            
            else:
                # Pas de callback après 20 secondes, on vérifie via l'API
                app.logger.info('⏱️ Pas de callback après 20 secondes, vérification via API Status...')
                
                # Nettoyer le dictionnaire
                if x_correlation_id in pending_callbacks:
                    del pending_callbacks[x_correlation_id]
                
                # Vérifier le statut via l'API
                status_response = check_transaction_status(
                    access_token,
                    server_correlation_id,
                    x_correlation_id,
                    data['partnerMsisdn'],
                    data['name']
                )
                
                if status_response:
                    object_reference = status_response.get('objectReference')
                    
                    if object_reference:
                        app.logger.info(f'✅ Object Reference trouvé: {object_reference}')
                        
                        # Récupérer les détails de la transaction
                        details = get_transaction_details(
                            access_token,
                            object_reference,
                            x_correlation_id,
                            data['partnerMsisdn'],
                            data['name']
                        )
                        
                        if details:
                            app.logger.info('✅ Détails de la transaction récupérés avec succès')
                            
                            # Extraire le statut depuis transactionStatus
                            transaction_status = details.get('transactionStatus', 'UNKNOWN')
                            
                            # Mapper le statut Mvola vers nos statuts
                            status_mapping = {
                                'completed': 'SUCCESS',
                                'failed': 'FAILED',
                                'pending': 'PENDING'
                            }
                            status = status_mapping.get(transaction_status.lower(), transaction_status.upper())
                            
                            app.logger.info(f'Statut final: {status} (original: {transaction_status})')
                            
                            # Construire la réponse depuis l'API (même format que callback)
                            response_data = {
                                'status': status,
                                'transactionReference': details.get('transactionReference'),
                                'serverCorrelationId': server_correlation_id,
                                'requestDate': details.get('requestDate'),
                                'creationDate': details.get('creationDate'),
                                'debitParty': details.get('debitParty', []),
                                'creditParty': details.get('creditParty', []),
                                'fees': details.get('fees', []),
                                'amount': details.get('amount'),
                                'currency': details.get('currency', 'Ar'),
                                'xCorrelationId': x_correlation_id,
                                'source': 'api_polling'
                            }
                            
                            return jsonify(response_data), 200
                        else:
                            app.logger.warning('Échec de la récupération des détails')
                            return jsonify({
                                'status': 'ERROR',
                                'message': 'Impossible de récupérer les détails de la transaction',
                                'serverCorrelationId': server_correlation_id,
                                'xCorrelationId': x_correlation_id
                            }), 500
                    else:
                        app.logger.warning('objectReference non disponible')
                        return jsonify({
                            'status': 'PENDING',
                            'message': 'Transaction en cours de traitement. objectReference non encore disponible.',
                            'serverCorrelationId': server_correlation_id,
                            'xCorrelationId': x_correlation_id
                        }), 202
                else:
                    app.logger.warning('Échec de la vérification du statut')
                    return jsonify({
                        'status': 'PENDING',
                        'message': 'Transaction en cours de traitement. Impossible de vérifier le statut.',
                        'serverCorrelationId': server_correlation_id,
                        'xCorrelationId': x_correlation_id
                    }), 202
            
        else:
            app.logger.warning(f'⚠ Échec de la transaction - Status: {response.status_code}')
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

@app.errorhandler(404)
def not_found(error):
    app.logger.warning(f'Route non trouvée: {request.path}')
    return jsonify({'error': 'Not found', 'message': 'Route non trouvée'}), 404

@app.errorhandler(500)
def internal_error(error):
    app.logger.error(f'Erreur serveur: {str(error)}')
    return jsonify({'error': 'Internal server error'}), 500

if __name__ == '__main__':
    app.logger.info('🚀 Démarrage de l\'application Mvola Token API avec Polling')
    app.run(debug=True, host='0.0.0.0', port=5000)