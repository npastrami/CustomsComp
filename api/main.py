from quart import Quart, request, jsonify, Response
from quart_cors import cors
from werkzeug.utils import secure_filename
from uploader import Uploader
from extractor import Extractor 
from database import Database
from sorter import Sorter
from form_mapping_utils import upload_bucket_mapping
from table_builder import TableBuilder
import xml.etree.ElementTree as ET
import re
import asyncio
import io

app = Quart(__name__)
app = cors(app)

@app.route('/process_doc', methods=['POST'])
async def process_doc():
    form = await request.form
    client_id = form['clientID']
    version_id = form['versionID']
    form_types = form.getlist('formTypes[]')
    uploaded_files = (await request.files).getlist('files[]')

    tasks = []

    for uploaded_file, form_type in zip(uploaded_files, form_types):
        tasks.append(handle_file(client_id, uploaded_file, form_type, version_id))

    responses = await asyncio.gather(*tasks)

    return jsonify(responses)

async def handle_file(client_id, uploaded_file, form_type, version_id):
    filename = secure_filename(uploaded_file.filename)
    blob_url = await upload_file(client_id, uploaded_file, form_type, version_id)

    if not blob_url:
        return {"status": "Error", "error": "Upload failed"}

    if form_type != 'None':
        xml_str = await extract_data(client_id, filename, upload_bucket_mapping[form_type], form_type, version_id)
        if xml_str:
            return {"status": "Extract Completed", "xml": xml_str}
        else:
            return {"status": "Empty Extraction"}
    else:
        return {"status": "Upload Completed", "uploaded_file": blob_url}

async def upload_file(client_id, uploaded_file, form_type, version_id):
    bucket_name = upload_bucket_mapping.get(form_type, 'unsorted')
    uploader = Uploader(bucket_name)
    blob_url = await uploader.upload(client_id, uploaded_file)
    if blob_url:
        database = Database(client_id, blob_url)
        await database.post2postgres_upload(client_id, blob_url, 'uploaded', form_type, bucket_name, version_id)
    return blob_url

async def extract_data(client_id, filename, bucket_name, form_type, version_id):
    sanitized_blob_name = sanitize_blob_name(filename)
    extractor = Extractor(bucket_name)
    extracted_values, blob_sas_url = await extractor.extract(client_id, sanitized_blob_name, form_type)
    await extractor.update_database(client_id, blob_sas_url, filename, form_type, extracted_values, version_id)
    root = ET.Element("W2s")
    for extracted_value in extracted_values:
        w2_element = ET.SubElement(root, "W2")
        for key, value in extracted_value.items():
            if key == 'confidence':
                w2_element.set('confidence', str(value))
            else:
                ET.SubElement(w2_element, key).text = str(value) if value is not None else None
    xml_str = ET.tostring(root, encoding='utf-8').decode('utf-8')
    
    return ({"message": "W2 extraction successful", "xml": xml_str})

def sanitize_blob_name(blob_name):
    # Your existing sanitize function
    sanitized = re.sub(r'[()\[\]{}]', '', blob_name)
    sanitized = sanitized.replace(' ', '_')
    return sanitized

@app.route('/download_csv/<document_id>', methods=['GET', 'POST'])
async def download_csv(document_id):
    print("Entered download_csv function")
    json_data = await request.json
    client_id = json_data['clientID']
    db = Database(None, None)
    sanitized_doc_id = sanitize_blob_name(document_id)
    print(sanitized_doc_id)  
    csv_content = await db.generate_csv(sanitized_doc_id, client_id)

    await db.close()

    response = Response(csv_content, mimetype='text/csv')
    response.headers["Content-Disposition"] = f"attachment; filename={document_id}.csv" 
    
    return response

@app.route('/get_client_data', methods=['POST'])
async def get_client_data():
    json_data = await request.json
    client_id = json_data['clientID']

    # Use async with to ensure proper initialization and cleanup of TableBuilder
    async with TableBuilder() as table_builder:
        client_data = await table_builder.fetch_client_data(client_id)

    return jsonify({"data": client_data})

async def process_sort(file):
    filename = secure_filename(file.filename)
    print(f'Processing file: {filename}')
    file_stream = io.BytesIO(file.read())

    sorter = Sorter()
    result = await sorter.sort(file_stream)
    file_stream.close()

    return {**result, 'file_name': filename}

@app.route('/sort', methods=['POST'])
async def sort():
    files = await request.files
    files_to_sort = files.getlist('files[]')

    # Schedule all file processing tasks to run concurrently
    sorted_files = await asyncio.gather(*(process_sort(file) for file in files_to_sort))
    
    print(f'sorted_files: {sorted_files}')
    return {'sorted_files': sorted_files}


if __name__ == "__main__":
    app.run(debug=True)
    
    #main branch