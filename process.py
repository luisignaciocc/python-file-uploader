import logging
import sys
import os
import  pandas as pd
import mysql.connector
import datetime
from settings.database import mysql_conn
from utils.database import search_by_name
from utils.validator import Validator

class UploadBudget():

    def __init__(self, filepath='files/budget.xlsx'):
        logging.basicConfig(
            level=logging.INFO,
            filename="./logs/uploader_{:%Y_%m_%d_%H_%M}.log".format(datetime.datetime.now()),
            datefmt="%Y-%m-%d %H:%M:%S",
            format='%(asctime)s %(levelname)-8s %(message)s'
        )
        
        self.logger = logging.getLogger("upload")
        
        self.mysql = mysql.connector.connect(
            host=mysql_conn["host"],
            user=mysql_conn["user"],
            password=mysql_conn["pass"],
            database=mysql_conn["db"]
        )
        
        self.cursor = self.mysql.cursor(dictionary=True,buffered=True)
        
        self.filepath = filepath
        data = pd.read_excel (self.filepath, engine='openpyxl')
        self.dataframe_columns = [
            'id',
            'year',
            'month',
            'type',
            'classification',
            'code',
            'descriptor',
            'lab_code',
            'lab',
            'business_name',
            'business_unit',
            'category_set',
            'macrocategory',
            'category_1',
            'category_2',
            'client_type',
            'channel',
            'sale',
            'contribution',
            'units',
            'price'
        ]
        self.df = pd.DataFrame(data)
        
        self.validator = Validator(self.logger)


    def execute(self):
        try:
            with open('lockfile.txt', 'r') as f:
                lock = f.readline().strip().split()
                if lock[0] == 'locked':
                    self.logger.info(f'Process already running!')
                    sys.exit(0)
        except FileNotFoundError:
            with open('lockfile.txt', 'w') as f:
                f.write('locked')

        if set(self.dataframe_columns).issubset(self.df.columns) is False:
            self.logger.error('Invalid file')
            os.remove("lockfile.txt")
            os.remove(self.filepath)
            sys.exit(0)

        self.logger.info(f'Process started')
        
        for index, row in self.df.iterrows():
            
            id = row["id"]
            year = self.validator.validate_data_number(row["year"], id, 'year')
            month = self.validator.validate_data_number(row["month"], id, 'month')
            type = self.validator.validate_data_number(row["type"], id, 'type')
            classification = self.validator.validate_data_string(row['classification'], id, 'classification')
            code = self.validator.validate_data_number(row["code"], id, 'code')
            descriptor = self.validator.validate_data_string(row["descriptor"], id, 'descriptor')
            lab_code = self.validator.validate_data_number(row['lab_code'], id, 'lab_code')
            lab = self.validator.validate_data_string(row['lab'], id, 'lab')
            business_name = self.validator.validate_data_string(row['business_name'], id, 'business_name')
            business_unit = self.validator.validate_data_string(row['business_unit'], id, 'business_unit')
            category_set = self.validator.validate_data_string(row['category_set'], id, 'category_set')
            macrocategory = self.validator.validate_data_string(row['macrocategory'], id, 'macrocategory')
            category_1 = self.validator.validate_data_string(row['category_1'], id, 'category_1')
            category_2 = self.validator.validate_data_string(row['category_2'], id, 'category_2')
            client_type = self.validator.validate_data_string(row['client_type'], id, 'client_type')
            channel = self.validator.validate_data_string(row['channel'], id, 'channel')
            sale = self.validator.validate_data_number(row['sale'], id, 'sale')
            contribution = self.validator.validate_data_number(row['contribution'], id, 'contribution')
            units = self.validator.validate_data_number(row['units'], id, 'units')
            price = self.validator.validate_data_number(row['price'], id, 'price')
            
            classification_id = search_by_name(self.cursor, 'classification', classification)
            if classification_id == 0:
                self.cursor.execute('INSERT INTO classification (name) VALUES (%s)' % (classification))
                self.mysql.commit()
                classification_id = self.cursor.lastrowid
            
            lab_id = search_by_name(self.cursor, 'laboratory', lab)
            if lab_id == 0:
                self.cursor.execute('INSERT INTO laboratory (name, code) VALUES (%s, %s)' % (lab, lab_code))
                self.mysql.commit()
                lab_id = self.cursor.lastrowid
            
            business_id = search_by_name(self.cursor, 'business', business_name)
            if business_id == 0:
                self.cursor.execute('INSERT INTO business (name) VALUES (%s)' % (business_name))
                self.mysql.commit()
                business_id = self.cursor.lastrowid
            
            business_unit_id = search_by_name(self.cursor, 'business_unit', business_unit)
            if business_unit_id == 0:
                self.cursor.execute('INSERT INTO business_unit (name) VALUES (%s)' % (business_unit))
                self.mysql.commit()
                business_unit_id = self.cursor.lastrowid
            
            category_set_id = search_by_name(self.cursor, 'category_set', category_set)
            if category_set_id == 0:
                self.cursor.execute('INSERT INTO category_set (name, businessUnitId) VALUES (%s, %s)' % (category_set, business_unit_id))
                self.mysql.commit()
                category_set_id = self.cursor.lastrowid
            
            macrocategory_id = search_by_name(self.cursor, 'macrocategory', macrocategory)
            if macrocategory_id == 0:
                self.cursor.execute('INSERT INTO macrocategory (name, categorySetId) VALUES (%s, %s)' % (macrocategory, category_set_id))
                self.mysql.commit()
                macrocategory_id = self.cursor.lastrowid
            
            category_1_id = search_by_name(self.cursor, 'category_1', category_1)
            if category_1_id == 0:
                self.cursor.execute('INSERT INTO category_1 (name, macrocategoryId) VALUES (%s, %s)' % (category_1, macrocategory_id))
                self.mysql.commit()
                category_1_id = self.cursor.lastrowid
            
            category_2_id = search_by_name(self.cursor, 'category_2', category_2)
            if category_2_id == 0:
                self.cursor.execute('INSERT INTO category_2 (name, category1Id) VALUES (%s, %s)' % (category_2, category_1_id))
                self.mysql.commit()
                category_2_id = self.cursor.lastrowid
            
            client_type_id = search_by_name(self.cursor, 'client_type', client_type)
            if client_type_id == 0:
                self.cursor.execute('INSERT INTO client_type (name) VALUES (%s)' % (client_type))
                self.mysql.commit()
                client_type_id = self.cursor.lastrowid

            channel_id = search_by_name(self.cursor, 'channel', channel)
            if channel_id == 0:
                self.cursor.execute('INSERT INTO channel (name) VALUES (%s)' % (channel))
                self.mysql.commit()
                channel_id = self.cursor.lastrowid

            try:
                self.cursor.execute("""
                    INSERT INTO budget (
                        id, year, month, type, code, descriptor, sale, contribution, units, price, classificationId, laboratoryId, businessId, businessUnitId, categorySetId, macrocategoryId, category1Id, category2Id, clientTypeId, channelId
                    ) VALUES (
                        %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                    )
                """ 
                % (
                    id, year, month, type, code, descriptor, sale, contribution, units, price, classification_id, lab_id, business_id, business_unit_id, category_set_id, macrocategory_id, category_1_id, category_2_id, client_type_id, channel_id
                ))
            except Exception as e:
                self.logger.error(f'Error inserting row, id: {row["id"]}')
                self.logger.exception(e)
                continue
            else:
                self.mysql.commit()

        self.logger.info(f'Process finished')
        os.remove("lockfile.txt")
        os.remove(self.filepath)

if __name__ == '__main__':
    UploadBudget().execute()