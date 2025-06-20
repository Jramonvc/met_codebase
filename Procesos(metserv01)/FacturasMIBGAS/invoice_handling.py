import xml.etree.ElementTree as ET
import pyodbc
from datetime import datetime
import os
import shutil
import time

start_time = time.time()

#path to files + processed folder
base_folder = r'M:\MET EE\Operaciones\MIBGAS\CentroDescargasGAS'
processed_folder = os.path.join(base_folder, 'Procesados')

#database connection data
server = 'met-esp-prod.database.windows.net'
db = 'Risk_MGMT_Spain'
username = 'sqluser'
password = 'cwD9KVms4Qdv4mLy'
table1 = 'METDB.MET_FACTMIBGASCAB'
table2 = 'METDB.MET_FACTMIBGASLIN'

conn_string = (
     f'DRIVER={{ODBC Driver 17 for SQL Server}};'
    f'SERVER={server};'
    f'DATABASE={db};'
    f'UID={username};'
    f'PWD={password};'
    f'Encrypt=yes;'
    f'TrustServerCertificate=no;'
    f'Connection Timeout=30;'
)

def parse_date(el):
    return datetime.strptime(el.text, "%Y-%m-%d").date() if el is not None and el.text else None


def parse_xml (file_path):
    try:

        #xml reader
        tree = ET.parse(file_path)
        root = tree.getroot()

        ns = {'fe': 'http://www.facturae.es/Facturae/2014/v3.2.1/Facturae'}

        #seller tax id
        seller_tax_id = root.find('.//SellerParty/TaxIdentification/TaxIdentificationNumber', ns)

        #buyer tax id
        buyer_tax_id = root.find('.//BuyerParty/TaxIdentification/TaxIdentificationNumber', ns)

        #invoice num
        invoice_num = root.find('.//Invoice/InvoiceHeader/InvoiceNumber', ns)

        #invoice series code
        invoice_series_code = root.find('.//Invoice/InvoiceHeader/InvoiceSeriesCode', ns)

        #invoice issuer type
        invoice_issuer_type = root.find('.//InvoiceIssuerType', ns)

        #invoice_type document
        invoice_type_doc = root.find('.//Invoice/InvoiceHeader/InvoiceDocumentType', ns)

        #invoice issue date
        invoice_issue_date = root.find('.//Invoice/InvoiceIssueData/IssueDate', ns)
            
        #invoice period start
        invoice_period_start = root.find('.//Invoice/InvoiceIssueData/InvoicingPeriod/StartDate', ns)

        #invoice period end
        invoice_period_end = root.find('.//Invoice/InvoiceIssueData/InvoicingPeriod/EndDate', ns)

        #installment due date
        installment_due_date = root.find('.//Invoice/PaymentDetails/Installment/InstallmentDueDate', ns)

        #total gross amount
        total_gross_amount = root.find('.//Invoice/InvoiceTotals/TotalGrossAmount', ns)

        #total tax outputs
        total_tax_outputs = root.find('.//Invoice/InvoiceTotals/TotalTaxOutputs', ns)

        #total taxes withheld
        total_tax_withheld = root.find('.//Invoice/InvoiceTotals/TotalTaxesWithheld', ns)
            
        #invoice total
        invoice_total = root.find('.//Invoice/InvoiceTotals/InvoiceTotal', ns)
        
        #insert invoice into db table
        with pyodbc.connect(conn_string) as conn:
            cursor = conn.cursor()
            
            if None in [seller_tax_id, buyer_tax_id, invoice_num, invoice_issue_date, total_gross_amount, total_tax_outputs, total_tax_withheld, invoice_total]:
                raise ValueError("One of the required fields is missing")
            else:
                values1 = (seller_tax_id.text,
                    buyer_tax_id.text,
                    invoice_num.text,
                    invoice_series_code.text if invoice_series_code is not None else None,
                    invoice_issuer_type.text,
                    invoice_type_doc.text,
                    parse_date(invoice_issue_date),
                    parse_date(invoice_period_start) if invoice_period_start is not None else None,
                    parse_date(invoice_period_end) if invoice_period_end is not None else None,
                    parse_date(installment_due_date) if installment_due_date is not None else None,
                    float(total_gross_amount.text),
                    float(total_tax_outputs.text),
                    float(total_tax_withheld.text),
                    float(invoice_total.text)
                )

                query1 = f"""
                    INSERT INTO {table1} 
                    (FMC_SELLER, FMC_BUYER, FMC_INVNUM, FMC_INVCOD, FMC_TIPFACT, FMC_TIPDOC, FMC_FECINV, FMC_FECINI, FMC_FECFIN, FMC_FECVEN, FMC_IMPBASE, FMC_IMPTAX, FMC_IMPTOTTACD, FMC_IMPTOTAL) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    SELECT SCOPE_IDENTITY() AS NewID;
                """

                cursor.execute(query1, values1)
                cursor.nextset()
                generated_id = cursor.fetchone()[0]
                print("Inserted row ID:", generated_id)

                i = 0
                for line in root.findall('.//Invoice/Items/InvoiceLine', ns):

                    #item desc
                    item_desc = line.find('./ItemDescription', ns)

                    #quantity
                    quantity = line.find('./Quantity', ns)

                    #unit price
                    unit_price = line.find('./UnitPriceWithoutTax', ns)

                    #total cost
                    total_cost = line.find('./TotalCost', ns)

                    #tax rate
                    tax_rate = line.find('./TaxesOutputs/Tax/TaxRate', ns)

                    #article code
                    article_code = line.find('./ArticleCode', ns)

                    #insert line into db table
                    if None in [total_cost]:
                        raise ValueError("One of the required fields is missing")
                    else:
                        values2 = (generated_id,
                            int(i),
                            item_desc.text if item_desc is not None else None,
                            float(quantity.text) if quantity is not None else None,
                            float(unit_price.text) if unit_price is not None else None,
                            float(total_cost.text),
                            float(tax_rate.text) if tax_rate is not None else None,
                            article_code.text if article_code is not None else None
                        )

                        query2 = f"""
                            INSERT INTO {table2} 
                            (FML_FMC_ID, FML_LINEA, FML_DESCRIP, FML_CANTIDAD, FML_UNITP, FML_TOTCOST, FML_TAXRATE, FML_ARTCODE) 
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                        """

                        cursor.execute(query2, values2)

                    i = i + 1
            cursor.close()
            conn.commit()
            conn.close()
    except Exception as e:
        print(f"Error: {str(e)}")


for filename in os.listdir(base_folder):
    if filename.lower().endswith('.xml'):

        #select xml file path and process it
        file_path = os.path.join(base_folder, filename)
        parse_xml(file_path)

        #move xml file to other folder (so it doesnt get accessed again)
        dest_path = os.path.join(processed_folder, filename)
        shutil.move(file_path, dest_path)

end_time = time.time()
elapsed_time = end_time - start_time
print(f"Finished processing in {elapsed_time:.2f} seconds.")