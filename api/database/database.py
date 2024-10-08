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
            password="newpassword",
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
    
    async def get_field_values(self, client_id, doc_type):
        await self.ensure_connected()
        query = """
        SELECT field_name, field_value
        FROM extracted_fields
        WHERE client_id = $1 AND doc_type = $2;
        """
        rows = await self.conn.fetch(query, client_id, doc_type)
        return rows
    
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
    
    async def generate_sheet_data(self, document_id, client_id):
        await self.ensure_connected()
        query = """SELECT field_name, field_value, confidence FROM extracted_fields WHERE doc_name = $1 AND client_id = $2"""
        rows = await self.conn.fetch(query, document_id, client_id)

        original_sheet_data = [
            ["Document Name: {}".format(document_id)],
            ["Field Names", "Field Values", "Confidence"]
        ]

        keywords_data = {}

        for field_name, field_value, confidence in rows:
            original_sheet_data.append([field_name, field_value, confidence])

            # Extract item codes from code keywords
            if '[code' in field_name:
                # print(field_name)
                keyword, code = field_name.split(' [')
                # print(keyword)
                code = code.rstrip(']')
                # print(code)
                
                # Extracting the number from the code
                code_number = code.split(' ')[-1]
                modified_keyword = f"{keyword} {code_number}"
                # print(f"mod keyword: {modified_keyword}")
                # Store code and confidence in the dictionary for modified_keyword
                if modified_keyword not in keywords_data:
                    keywords_data[modified_keyword] = {'code': field_value, 'code_confidence': confidence}

                # Update code for the original keyword if it exists - adds code to keyword in keyword_data before amount or confidence
                if keyword in keywords_data:
                    keywords_data[keyword].update({'code': field_value})
            else:
                keyword = field_name  # bring in amount-keywords
                # add amount and confidence to keywords data
                if keyword not in keywords_data:
                    keywords_data[keyword] = {'amount': field_value, 'amount_confidence': confidence}
                else:
                    # Update in case of append cause GPT said to
                    keywords_data[keyword].update({'amount': field_value, 'amount_confidence': confidence})

        # print(f"keyword data:{keywords_data}") 
        # Construct the FOF sheet data
        fof_sheet_data = [
            ["Document Name: FOF_{}".format(document_id)],
            ["Keyword", "Item Codes", "Amount", "Confidence"]
        ]

        for keyword, data in keywords_data.items():
            row = [
                keyword,
                data.get('code', ''),
                data.get('amount', ''),
                data.get('amount_confidence', 0)
            ]
            fof_sheet_data.append(row)

        return original_sheet_data, fof_sheet_data

    async def close(self):
        if self.conn is not None:
            await self.conn.close()