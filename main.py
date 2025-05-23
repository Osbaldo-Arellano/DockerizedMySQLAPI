from __future__ import annotations
from flask import Flask, request, jsonify, url_for
from connect_connector import connect_with_connector
from sqlalchemy.exc import IntegrityError

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
        "name": str(business_row["name"]),
        "street_address": str(business_row["street_address"]),
        "city": str(business_row["city"]),
        "state": str(business_row["state"]),
        "zip_code": int(business_row["zip_code"]), 
    }
def format_review_response(review):
    base_url = request.host_url.rstrip('/')
    return {
        'id': int(review['id']),
        'user_id': int(review['user_id']),
        'business': f"{base_url}/businesses/{review['business_id']}",
        'stars': int(review['stars']),
        'review_text': review.get('review_text'),
        'self': f"{base_url}/reviews/{review['id']}"
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

@app.route('/businesses', methods=['GET'])
def get_businesses():
    try:
        limit = int(request.args.get('limit', 3))
        offset = int(request.args.get('offset', 0))
    except ValueError:
        return jsonify({'Error': 'Invalid limit or offset'}), 400

    select = sqlalchemy.text('SELECT * FROM business LIMIT :limit OFFSET :offset')
    count_query = sqlalchemy.text('SELECT COUNT(*) FROM business')

    with db.connect() as conn:
        result = conn.execute(select, {'limit': limit, 'offset': offset})
        businesses = result.mappings().fetchall()
        total_count = conn.execute(count_query).scalar()  # get total number of businesses

    entries = []
    for business in businesses:
        formatted = format_business_response(business)
        formatted["self"] = f"{request.host_url}businesses/{business['id']}"
        entries.append(formatted)

    response = {'entries': entries}

    # Add 'next' only if there are more records to fetch
    if offset + limit < total_count:
        next_offset = offset + limit
        response['next'] = f"{request.host_url}businesses?offset={next_offset}&limit={limit}"

    return jsonify(response), 200

@app.route('/businesses/<int:business_id>', methods=['PUT'])
def edit_business(business_id):
    data = request.get_json()
    required_fields = ['owner_id', 'name', 'street_address', 'city', 'state', 'zip_code']

    if not data or not all(field in data for field in required_fields):
        return jsonify({'Error': 'The request body is missing at least one of the required attributes'}), 400

    select_query = sqlalchemy.text('SELECT * FROM business WHERE id = :id')
    update_query = sqlalchemy.text('''
        UPDATE business
        SET owner_id = :owner_id,
            name = :name,
            street_address = :street_address,
            city = :city,
            state = :state,
            zip_code = :zip_code
        WHERE id = :id
    ''')

    with db.connect() as conn:
        # Check if business exists
        existing = conn.execute(select_query, {'id': business_id}).mappings().fetchone()
        if not existing:
            return jsonify({'Error': 'No business with this business_id exists'}), 404

        # update
        conn.execute(update_query, {**data, 'id': business_id})
        conn.commit()

        # Get update
        updated = conn.execute(select_query, {'id': business_id}).mappings().fetchone()

    response = format_business_response(updated)
    response["self"] = f"{request.host_url.rstrip('/')}/businesses/{updated['id']}"

    return jsonify(response), 200

@app.route('/businesses/<int:business_id>', methods=['DELETE'])
def delete_business(business_id):
    select_query = sqlalchemy.text('SELECT * FROM business WHERE id = :id')
    delete_reviews_query = sqlalchemy.text('DELETE FROM review WHERE business_id = :business_id')
    delete_business_query = sqlalchemy.text('DELETE FROM business WHERE id = :id')

    with db.connect() as conn:
        # Check if the business exists
        result = conn.execute(select_query, {'id': business_id}).mappings().fetchone()
        if not result:
            return jsonify({'Error': 'No business with this business_id exists'}), 404

        # Delete reviews and the business
        conn.execute(delete_reviews_query, {'business_id': business_id})
        conn.execute(delete_business_query, {'id': business_id})
        conn.commit()

    return '', 204

@app.route('/owners/<int:owner_id>/businesses', methods=['GET'])
def list_owner_businesses(owner_id):
    query = sqlalchemy.text('SELECT * FROM business WHERE owner_id = :owner_id')

    with db.connect() as conn:
        result = conn.execute(query, {'owner_id': owner_id})
        businesses = result.mappings().fetchall()

    business_list = []
    for business in businesses:
        business_dict = dict(business)
        business_dict["self"] = f"{request.host_url.rstrip('/')}/businesses/{business_dict['id']}"
        business_list.append(business_dict)

    return jsonify(business_list), 200

@app.route('/reviews', methods=['POST'])
def create_review():
    data = request.get_json()
    required_fields = ['user_id', 'business_id', 'stars']
    if not all(field in data for field in required_fields):
        return jsonify({'Error': 'The request body is missing at least one of the required attributes'}), 400

    business_check = sqlalchemy.text('SELECT * FROM business WHERE id = :bid')
    with db.connect() as conn:
        business = conn.execute(business_check, {'bid': data['business_id']}).fetchone()
        if not business:
            return jsonify({'Error': 'No business with this business_id exists'}), 404

        review_check = sqlalchemy.text(
            'SELECT * FROM review WHERE user_id = :uid AND business_id = :bid'
        )
        existing = conn.execute(review_check, {
            'uid': data['user_id'],
            'bid': data['business_id']
        }).fetchone()

        if existing:
            return jsonify({
                'Error': 'You have already submitted a review for this business. You can update your previous review, or delete it and submit a new review'
            }), 409

        insert = sqlalchemy.text(
            '''
            INSERT INTO review (user_id, business_id, stars, review_text)
            VALUES (:uid, :bid, :stars, :review_text)
            '''
            )
        conn.execute(insert, {
            'uid': data['user_id'],
            'bid': data['business_id'],
            'stars': data['stars'],
            'review_text': data.get('review_text')
        })

        conn.commit()

        review_id = conn.execute(sqlalchemy.text('SELECT LAST_INSERT_ID()')).scalar()

        new_review = conn.execute(
            sqlalchemy.text('SELECT * FROM review WHERE id = :id'),
            {'id': review_id}
        ).mappings().fetchone()

        response = format_review_response(new_review)

        return jsonify(response), 201


@app.route('/reviews/<review_id>', methods=['GET'])
def get_review(review_id):
    try:
        review_id = int(review_id)
    except ValueError:
        return jsonify({'Error': 'review_id must be an integer'}), 200

    query = sqlalchemy.text('SELECT * FROM review WHERE id = :id')
    with db.connect() as conn:
        review = conn.execute(query, {'id': review_id}).mappings().fetchone()

    if not review:
        return jsonify({'Error': 'No review with this review_id exists'}), 404

    return jsonify(format_review_response(review)), 200


@app.route('/reviews/<int:review_id>', methods=['PUT'])
def edit_review(review_id):
    data = request.get_json()
    if 'stars' not in data and 'review_text' not in data:
        return jsonify({'Error': 'The request body is missing at least one of the required attributes'}), 400

    with db.connect() as conn:
        check = sqlalchemy.text('SELECT * FROM review WHERE id = :id')
        review = conn.execute(check, {'id': review_id}).mappings().fetchone()

        if not review:
            return jsonify({'Error': 'No review with this review_id exists'}), 404

        update = sqlalchemy.text('''
            UPDATE review SET
                stars = COALESCE(:stars, stars),
                review_text = COALESCE(:review_text, review_text)
            WHERE id = :id
        ''')
        conn.execute(update, {
            'stars': data.get('stars'),
            'review_text': data.get('review_text'),
            'id': review_id
        })

        updated_review = conn.execute(
            sqlalchemy.text('SELECT * FROM review WHERE id = :id'),
            {'id': review_id}
        ).mappings().fetchone()

        return jsonify(format_review_response(updated_review)), 200


@app.route('/reviews/<int:review_id>', methods=['DELETE'])
def delete_review(review_id):
    with db.connect() as conn:
        check = sqlalchemy.text('SELECT * FROM review WHERE id = :id')
        review = conn.execute(check, {'id': review_id}).fetchone()

        if not review:
            return jsonify({'Error': 'No review with this review_id exists'}), 404

        conn.execute(sqlalchemy.text('DELETE FROM review WHERE id = :id'), {'id': review_id})
        return '', 204


@app.route('/users/<int:user_id>/reviews', methods=['GET'])
def list_user_reviews(user_id):
    query = sqlalchemy.text('SELECT * FROM review WHERE user_id = :uid')
    with db.connect() as conn:
        result = conn.execute(query, {'uid': user_id}).mappings().fetchall()

    return jsonify([format_review_response(r) for r in result]), 200

if __name__ == '__main__':
    init_db()
    create_business_table(db)
    create_review_table(db)
    app.run(host='127.0.0.1', port=8080, debug=True)