import logging
from configparser import ConfigParser
import json
import os
import re

logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')

config = ConfigParser(interpolation=None)
config.read('config.ini')

INPUT_FILE_NAME = config['IN']['INPUT_FILE_NAME']
INPUT_FILE_CONTAINS_FILEHEADER = bool(config['IN']['INPUT_FILE_CONTAINS_FILEHEADER'])
INPUT_FILE_CONTAINS_COLHEADERS = bool(config['IN']['INPUT_FILE_CONTAINS_COLHEADERS'])
INPUT_COLS = dict(json.loads(config['IN']['INPUT_COLS']))
FILE_HEADER_PATTERN = config['IN']['FILE_HEADER_PATTERN']
OUTPUT_DT_PATTERN = config['OUT']['OUTPUT_DT_PATTERN']
OUTPUT_COL_HEADERS = list(config['OUT']['OUTPUT_COL_HEADERS'].split(','))
OUTPUT_FILE_PATH = config['OUT']['OUTPUT_FILE_PATH']
OUTPUT_ORDER_BY = config['OUT']['OUTPUT_ORDER_BY']

ERR_MSG_NO_IN_FILE = 'Missing input file "{}"!'.format(INPUT_FILE_NAME)

if os.path.exists(INPUT_FILE_NAME) == False:
    raise Exception(ERR_MSG_NO_IN_FILE)

with open(INPUT_FILE_NAME, "r") as input_file:
    file_header = input_file.readline()
    records_in_file = len(input_file.readlines()) - 2 # 1 row as header + 1 row as the last empty row. The rest is data set
if re.match(FILE_HEADER_PATTERN, file_header):
    control_numbers = re.findall(r'[0-9]+/[0-9]+', file_header)[0]
    records_to_check = int(control_numbers.split('/')[0])
    if (records_to_check != records_in_file):
        input_file.close()
        raise Exception('Control numbers don''t match!')
else:
    raise Exception('Wrong file header format!')

