from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import os
import tempfile
from werkzeug.utils import secure_filename
from extract_parquet_metadata import process_parquet_files

app = Flask(__name__)
CORS(app)

UPLOAD_FOLDER = tempfile.mkdtemp()
ALLOWED_EXTENSIONS = {'parquet'}

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB max

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/upload', methods=['POST'])
def upload_files():
    if 'files' not in request.files:
        return jsonify({'error': 'Nenhum arquivo enviado'}), 400
    
    files = request.files.getlist('files')
    
    if not files or files[0].filename == '':
        return jsonify({'error': 'Nenhum arquivo selecionado'}), 400
    
    uploaded_files = []
    
    for file in files:
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)
            uploaded_files.append(file_path)
    
    if not uploaded_files:
        return jsonify({'error': 'Nenhum arquivo Parquet válido enviado'}), 400
    
    try:
        output_excel = os.path.join(app.config['UPLOAD_FOLDER'], 'parquet_metadata.xlsx')
        process_parquet_files(uploaded_files, output_excel)
        
        return jsonify({
            'message': 'Arquivos processados com sucesso',
            'excel_url': '/download'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/download', methods=['GET'])
def download_excel():
    excel_path = os.path.join(app.config['UPLOAD_FOLDER'], 'parquet_metadata.xlsx')
    
    if not os.path.exists(excel_path):
        return jsonify({'error': 'Arquivo Excel não encontrado'}), 404
    
    return send_file(
        excel_path,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name='parquet_metadata.xlsx'
    )

if __name__ == '__main__':
    app.run(debug=True, port=5000)