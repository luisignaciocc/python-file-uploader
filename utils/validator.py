import math
import numbers

class Validator():
    
    def __init__(self, logger):
        self.logger = logger

    def validate_data_string(self, value, id, column):
        if isinstance(value, str):
            if value == 'falta dato' or value == 'NaN' or value == 'SIN DATOS' or value == '#N/D' or value == 'NO APLICA':
                return "'" + "-" + "'"
            else:
                return "'" + value + "'"
        else:
            self.logger.warn(f'Type error: {column} - id: {id}')
            return "'" + "-" + "'"

    def validate_data_number(self, value, id, column):
        if isinstance(value, numbers.Number) and not math.isnan(abs(value)) and math.isfinite(abs(value)):
            return value
        else:
            self.logger.warn(f'Type error: {column} - id: {id}')
            return 0