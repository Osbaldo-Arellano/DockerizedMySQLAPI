from __future__ import annotations
from flask import Flask, request, jsonify, url_for
from connect_connector import connect_with_connector

import logging
import os
import sqlalchemy

app = Flask(__name__)

logger = logging.getLogger()

# Sets up connection pool for the app
def init_connection_pool() -> sqlalchemy.engine.base.Engine:
    if os.environ.get('INSTANCE_CONNECTION_NAME'):
        return connect_with_connector()
        
    raise ValueError(
        'Missing database connection type. Please define INSTANCE_CONNECTION_NAME'
    )

# This global variable is declared with a value of `None`
db = None

# Initiates connection to database
def init_db():
    global db
    db = init_connection_pool()

def create_business_table(db: sqlalchemy.engine.base.Engine) -> None:
    with db.connect() as conn:
        conn.execute(
            sqlalchemy.text(
                '''
                CREATE TABLE IF NOT EXISTS business (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    owner_id INT NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    street_address VARCHAR(255) NOT NULL,
                    city VARCHAR(255) NOT NULL,
                    state VARCHAR(255) NOT NULL,
                    zip_code VARCHAR(10) NOT NULL
                );
                '''
            )
        )
        conn.commit()

def create_review_table(db: sqlalchemy.engine.base.Engine) -> None:
    with db.connect() as conn:
        conn.execute(
            sqlalchemy.text(
                '''
                CREATE TABLE IF NOT EXISTS review (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    user_id INT NOT NULL,
                    business_id INT NOT NULL,
                    stars INT NOT NULL,
                    review_text TEXT,
                    FOREIGN KEY (business_id) REFERENCES business(id) ON DELETE CASCADE
                );
                '''
            )
        )
        conn.commit()

# Response Formatting
def format_business_response(business_row):
    return {
        "id": int(business_row["id"]),
        "owner_id": int(business_row["owner_id"]), 
        "name": business_row["name"],
        "street_address": business_row["street_address"],
        "city": business_row["city"],
        "state": business_row["state"],
        "zip_code": int(business_row["zip_code"]), 
    }


def format_review_response(review):
    return {
        'id': review.id,
        'user_id': review.user_id,
        'business_id': review.business_id,
        'stars': review.stars,
        'review_text': review.review_text
    }

@app.route('/', methods=['GET'])
def index():
    routes = {
        'POST /businesses': 'Create a new business',
        'GET /businesses': 'List all businesses',
        'GET /businesses/<business_id>': 'Get a business by ID',
        'PUT /businesses/<business_id>': 'Update a business by ID',
        'DELETE /businesses/<business_id>': 'Delete a business and its reviews',
        'GET /owners/<owner_id>/businesses': 'List all businesses for an owner',
        'POST /reviews': 'Create a new review',
        'GET /reviews/<review_id>': 'Get a review by ID',
        'PUT /reviews/<review_id>': 'Update a review by ID',
        'DELETE /reviews/<review_id>': 'Delete a review by ID',
        'GET /users/<user_id>/reviews': 'List all reviews made by a user'
    }
    return jsonify({'available_routes': routes}), 200

@app.route('/businesses', methods=['POST'])
def create_business():
    data = request.get_json()
    required_fields = ['owner_id', 'name', 'street_address', 'city', 'state', 'zip_code']
    
    if not all(field in data for field in required_fields):
        return jsonify({'Error': 'The request body is missing at least one of the required attributes'}), 400

    insert = sqlalchemy.text(
        '''
        INSERT INTO business (owner_id, name, street_address, city, state, zip_code)
        VALUES (:owner_id, :name, :street_address, :city, :state, :zip_code)
        '''
        )

    select = sqlalchemy.text(
        '''
        SELECT * FROM business WHERE id = :id
        '''
        )

    with db.connect() as conn:
        result = conn.execute(insert, data)
        conn.commit()
        business_id = result.lastrowid

        row_result = conn.execute(select, {'id': business_id})
        business_result = row_result.mappings().fetchone()


    if business_result is None:
        return jsonify({'Error': 'Failed to fetch created business'}), 500
    
    response = format_business_response(business_result)

    response["self"] = f"{request.host_url}businesses/{business_result['id']}"
    
    return jsonify(response), 201

@app.route('/businesses/<int:business_id>', methods=['GET'])
def get_business_by_id(business_id):
    select = sqlalchemy.text('SELECT * FROM business WHERE id = :id')
    
    with db.connect() as conn:
        result = conn.execute(select, {'id': business_id})
        business = result.mappings().fetchone()

    if business is None:
        return jsonify({'Error': 'No business with this business_id exists'}), 404

    response = format_business_response(business)
    response["self"] = f"{request.host_url.rstrip('/')}/businesses/{business['id']}"
    return jsonify(response), 200

if __name__ == '__main__':
    init_db()
    create_business_table(db)
    create_review_table(db)
    app.run(host='127.0.0.1', port=8080, debug=True)