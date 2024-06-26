import sqlite3
import os
from datetime import datetime, timedelta
from flask import Flask, request, g
from flask_cors import CORS 
app = Flask(__name__)
from flask import jsonify
CORS(app)
current_file_path = os.path.abspath(__file__)

# Lấy thư mục chứa file hiện tại
current_dir = os.path.dirname(current_file_path)

# Tạo đường dẫn tới file cơ sở dữ liệu trong cùng thư mục với file script
DATABASE = os.path.join(current_dir, 'demo_arbeitsagentur.db')

def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()


@app.route('/provinces', methods=['GET'])
def get_provinces():
    db = get_db()
    cursor = db.cursor()
    query = "SELECT DISTINCT region FROM data WHERE region IS NOT NULL AND region != '' ORDER BY region ASC"
    cursor.execute(query)
    regions = cursor.fetchall()
    provinces = [{'id': index + 1, 'name': region[0]} for index, region in enumerate(regions) if region[0]]
    return jsonify(provinces)

@app.route('/type', methods=['GET'])
def get_categories():
    db = get_db()
    cursor = db.cursor()
    query = "SELECT DISTINCT type FROM data ORDER BY type ASC"
    cursor.execute(query)
    categories = cursor.fetchall()
    # Bỏ qua kết quả rỗng và thêm số thứ tự vào mỗi type không rỗng
    types = [{'id': index + 1, 'name': category[0]} for index, category in enumerate(categories) if category[0]]
    return jsonify(types)
@app.route('/api', methods=['GET'])
def get_jobs():
    search = request.args.get('search', default='', type=str)
    filter = request.args.get('type', default='', type=str)
    sort = request.args.get('sort', default='', type=str)
    min_s = request.args.get('min_s', default=None, type=int)
    max_s = request.args.get('max_s', default=None, type=int)
    region = request.args.get('region', default='', type=str)
    limit = request.args.get('limit', default=10, type=int)
    offset = request.args.get('offset', default=0, type=int)
    db = get_db()
    cursor = db.cursor()

    base_query = " FROM data WHERE 1=1"
    params = []

    if search:
        base_query += " AND title LIKE ?"
        params.append('%' + search + '%')
    if filter:
        base_query += " AND type = ?"
        params.append(filter)
    if region:
        base_query += " AND region = ?"
        params.append(region)
    if min_s is not None:
        base_query += " AND salary >= ?"
        params.append(min_s)
    if max_s is not None:
        base_query += " AND salary <= ?"
        params.append(max_s)

    total_query = "SELECT COUNT(*)" + base_query
    cursor.execute(total_query, params)
    total_results = cursor.fetchone()[0]

    data_query = "SELECT *" + base_query
    if sort == 'new':
        data_query += " ORDER BY time_up DESC"
    elif sort == 'old':
        data_query += " ORDER BY time_up ASC"
    else:
        data_query += " ORDER BY time_up DESC"
    data_query += " LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    cursor.execute(data_query, params)
    jobs = cursor.fetchall()
    column_names = [column[0] for column in cursor.description]
    jobs_dict = [dict(zip(column_names, row)) for row in jobs]

    return jsonify({'total': total_results, 'jobs': jobs_dict})
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)