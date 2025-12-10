import os
from flask import Flask, render_template, request
import psycopg2
from psycopg2.extras import RealDictCursor


app = Flask(__name__)

db_config = os.environ.get('DATABASE_URL')

available_locales = [
    ('pl_PL', 'Polski (Polska)'),
    ('en_US', 'English (USA)'),
    ('de_DE', 'Deutsch (Deutschland)'),
    ('fr_FR', 'Français (France)'),
    ('it_IT', 'Italiano (Italia)')
]
def get_db_connection():
    try:
        connection = psycopg2.connect(db_config)
        return connection
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

@app.route('/')
def index():
    seed = request.args.get('seed', 'start_seed')
    current_locales = request.args.getlist('locale')
    if not current_locales:
        current_locales = ['en_US']
    try:
        batch_id = int(request.args.get('batch_id', '1'))
    except ValueError:
        batch_id = 1

    if batch_id < 1:
        batch_id = 1

    user_count = 20
    users = []
    error = None

    try:
        connection = get_db_connection()
        if connection:
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            query = "SELECT * FROM task_6.generate_users_batch(%s, %s, %s, %s)"
            cursor.execute(query, (seed, current_locales, batch_id, user_count))

            users = cursor.fetchall()
            cursor.close()
            connection.close()
        else:
            error = "Database connection failed"
    except Exception as e:
        error = f"Error in database operation: {e}"

    return render_template(
        'index.html',
        users=users,
        seed=seed,
        current_locales=current_locales,
        batch_id=batch_id,
        user_count=user_count,
        locales=available_locales,
        error=error
    )

if __name__ == '__main__':
    app.run(debug=True, port=5000)
