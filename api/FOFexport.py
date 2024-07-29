import openpyxl
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment, PatternFill
import numpy as np
import pandas as pd
import re
from io import BytesIO
from itemcodeoffsets import keyword_to_offset_dict

def process_FOF(workbook, fof_sheets):   
        mappings = {
            "shippername": 7,
            "shipperaddress": 8,
            "shipperphone": 9,
            "invoicenum": 10,
            "countrycode": 11,
            "date": 12,
            "mawbnum": 13,
            "hawbnum": 14,
            "airlineandflightnum": 15,
            "freightforwarder": 16,
            "rucnum": 17,
            "daenum": 18,
            "incoterm": 19,
            "consigneename": 20,
            "consigneeaddress": 21,
            "consigneecityc": 22,
            "consigneephone": 23,
            "consigneecontact": 24,
            "consigneepostalcode": 25,
            "fixedprice": 26,
            "consignment": 27,
            "piecestype1": 28,
            "totalpices1": 29,
            "eqfullboxes1": 30,
            "product1": 31,
            "hits#1": 32,
            "nandina1": 33,
            "totalunits1": 34,
            "stemsbunch1": 35,
            "unitprice1": 36,
            "totalvalue1": 37,
            "samples": 38,
            "billto": 39,
            "shippercontact": 40,
            "shipperfax": 41,
            "consigneefax": 42,
        }
        
        target_worksheet = workbook["Sheet1"]
        starting_row = 5

        for sheet_index, fof_sheet in enumerate(fof_sheets, start=starting_row):
            # Extracting the sheet name and removing the 'FOF_' prefix
            sheet_title = fof_sheet.title.replace("FOF_", "")
            row_number = sheet_index
            target_worksheet[f'A{row_number}'] = sheet_title
            
            #list of lists to store items with footnotes
            items_with_footnotes = []
            items_with_target_col = []

            for row in fof_sheet.iter_rows(min_row=1):
                keyword_cell = row[0].value
                if keyword_cell and any(keyword in keyword_cell for keyword in mappings.keys()):
                    # match base keyword
                    base_keyword = re.match(r'([^\d]+)\s*(\d*)', keyword_cell).groups()[0]
                    base_keyword = base_keyword.strip()
                    # fetch item code associated with keyword instance
                    item_code = row[1].value

                    if item_code and '*' in item_code:
                        item_code = item_code.replace('*', '')
                        items_with_footnotes.append([base_keyword, item_code, int(sheet_index)])
                    if base_keyword in keyword_to_offset_dict:
                        offset_dict = keyword_to_offset_dict[base_keyword]

                        if item_code in offset_dict:
                            offset = offset_dict[item_code]
                            target_col = int(mappings[base_keyword]) + offset + 1  # Convert to int
                            print(f'Target col for {base_keyword} with item code {item_code} is {target_col}')
                        else:
                            invalid_item_codes = {base_keyword: item_code}
                            print(f'This Keyword-Item Code pairing is invalid: {invalid_item_codes}')
                            continue
                    else:
                        # For keywords without offsets
                        target_col = int(mappings[base_keyword]) + 1  # Convert to int
                    print(f'Final target col: {target_col}, for {base_keyword} with item code {item_code}')
                    # Append target col to items_with_target_col matched with base keyword and item code
                    for item in items_with_footnotes:
                        if item[0] == base_keyword and item[1] == item_code:
                            items_with_target_col.append(item + [target_col])  # Create a new list with target_col

                    amount_cell = row[2].value
                    target_worksheet.cell(row=row_number, column=target_col, value=amount_cell)
            print(f'All items with footnotes: {items_with_footnotes}')
            
            for base_keyword, item_code, sheet_index, target_col in items_with_target_col:
                # Ensure target_row and sheet_index are integers before calling the cell method
                target_col = int(target_col) if isinstance(target_col, str) else target_col
                sheet_index = int(sheet_index) if isinstance(sheet_index, str) else sheet_index
                cell_to_highlight = target_worksheet.cell(row=row_number, column=target_col)
                cell_to_highlight.fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
                        
        for row in range(1, target_worksheet.max_row + 1):
            row_letter = get_column_letter(row)
            target_worksheet.row_dimensions[row].height = 25  # Approximate conversion
                
        # Make Totals Column for Rows 5-300 in Column A
        for col_num in range(5, 300):  # End range is exclusive, so 300 to include column 299
            sum_value = 0  # Initialize sum for the current column
            for row_num in range(1, target_worksheet.max_row + 1):  # Start from row 1
                cell = target_worksheet.cell(row=row_num, column=col_num)
                cell_value = cell.value
                # Check if the cell contains a string with parentheses or a negative sign
                if cell_value is not None:
                    # Remove commas, parentheses, and negative signs for numeric values
                    if isinstance(cell_value, str):
                        cell_value_cleaned = cell_value.replace(',', '').replace('(', '').replace(')', '').replace('-', '')
                        if cell_value_cleaned.isdigit():
                            numeric_value = int(cell_value_cleaned)
                            if '(' in cell_value or '-' in cell_value:
                                sum_value -= numeric_value
                            else:
                                sum_value += numeric_value

            # Update Row 1 with the calculated sum for the current column, converting sum to string, make 0 totals blank
            if sum_value != 0:
                target_worksheet.cell(row=1, column=col_num, value=str(sum_value))  # Row 1 is the 1st row
            else:
                target_worksheet.cell(row=1, column=col_num, value="")

        # Set text wrap for column A
        for row in range(1, target_worksheet.max_row + 1):
            cell = target_worksheet.cell(row=row, column=1)
            cell.alignment = Alignment(wrap_text=True, horizontal='center', vertical='center')

        # Rename the worksheet
        target_worksheet.title = "FOF_Data"