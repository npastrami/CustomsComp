import asyncpg
import os
import csv
from io import StringIO

class Database:
    def __init__(self, client_id, doc_url):
        self.client_id = client_id
        self.doc_url = doc_url
        self.conn = None
        
    async def ensure_connected(self):
        if self.conn is None:
            await self.connect()

    async def connect(self):
        self.conn = await asyncpg.connect(
            database="postgres",
            user="postgres",
            password="kr3310",
            host="localhost",
            port="5432"
        )
        await self.create_table()

    async def create_table(self):
        await self.ensure_connected()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS client_docs (
            id SERIAL PRIMARY KEY,
            client_id TEXT,
            doc_url TEXT,
            doc_name TEXT,
            doc_status TEXT,
            doc_type TEXT,
            container_name TEXT,
            access_id TEXT
        );
        """
        await self.conn.execute(create_table_query)
        
        create_extracted_fields_table_query = """
        CREATE TABLE IF NOT EXISTS extracted_fields (
            id SERIAL PRIMARY KEY,
            client_id TEXT,
            doc_url TEXT,
            doc_name TEXT,
            doc_status TEXT,
            doc_type TEXT,
            field_name TEXT,
            field_value TEXT,
            confidence REAL,
            access_id TEXT
        );
        """
        await self.conn.execute(create_extracted_fields_table_query)

    async def post2postgres_upload(self, client_id, doc_url, doc_status, doc_type, container_name, access_id):
        await self.ensure_connected()
        doc_name = os.path.basename(doc_url)  
        insert_query = """
        INSERT INTO client_docs (client_id, doc_url, doc_name, doc_status, doc_type, container_name, access_id)  
        VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id;
        """
        last_inserted_id = await self.conn.fetchval(insert_query, client_id, doc_url, doc_name, doc_status, doc_type, container_name, access_id)
        return last_inserted_id
    
    async def post2postgres_extract(self, client_id, doc_url, doc_name, doc_status, doc_type, field_name, field_value, confidence, access_id):
        await self.ensure_connected()
        insert_query = """
        INSERT INTO extracted_fields (client_id, doc_url, doc_name, doc_status, doc_type, field_name, field_value, confidence, access_id) 
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING id;
        """
        # Use fetchval to execute the query and get the returned value
        last_inserted_id = await self.conn.fetchval(insert_query, client_id, doc_url, doc_name, doc_status, doc_type, field_name, field_value, confidence, access_id)

        update_status_query = """
        UPDATE client_docs SET doc_status = 'extracted' WHERE client_id = $1 AND doc_url = $2;
        """
        # Use execute for the update query as it does not return a value
        await self.conn.execute(update_status_query, client_id, doc_url)
        
        return last_inserted_id
    
    async def generate_csv(self, document_id, client_id):
        await self.ensure_connected()
        # Query to fetch all fields and values for a specific document and client
        query = """ SELECT field_name, field_value, confidence FROM extracted_fields WHERE doc_name = $1 AND client_id = $2"""
        rows = await self.conn.fetch(query, document_id, client_id)
        # Initialize CSV output in memory
        output = StringIO()
        csv_writer = csv.writer(output)

        # Write the document name in cell A1
        csv_writer.writerow([f"Document Name: {document_id}"])

        # Write "Field Names" in cell A2 and "Field Values" in cell B2
        csv_writer.writerow(["Field Names", "Field Values", "Confidence"])

        # Populate the rest of the columns with field names and their values
        for row in rows:
            csv_writer.writerow([row[0], row[1], row[2]])

        # Get the CSV content and reset the pointer
        csv_content = output.getvalue()
        output.seek(0)

        return csv_content

    async def close(self):
        if self.conn is not None:
            await self.conn.close()