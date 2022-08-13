import logging
from configparser import ConfigParser
import json
import os
import re
import math
from datetime import datetime
import csv


def read_config(conf_file):
    config = ConfigParser(interpolation=None)
    if not os.path.exists(conf_file):
        raise FileNotFoundError(conf_file, " not found")
    else:
        config.read(conf_file)
        input_file_name = config['IN']['INPUT_FILE_NAME']
        input_file_contains_fileheader = bool(
            config['IN']['INPUT_FILE_CONTAINS_FILEHEADER'])
        input_file_contains_colheaders = bool(
            config['IN']['INPUT_FILE_CONTAINS_COLHEADERS'])
        input_cols = dict(json.loads(config['IN']['INPUT_COLS']))
        file_header_pattern = config['IN']['FILE_HEADER_PATTERN']
        output_dt_pattern = config['OUT']['OUTPUT_DT_PATTERN']
        output_col_headers = list(config['OUT']['OUTPUT_COL_HEADERS'].split(','))
        output_file_path = config['OUT']['OUTPUT_FILE_PATH']
        output_order_by = config['OUT']['OUTPUT_ORDER_BY']
        return input_file_name, input_file_contains_fileheader, input_file_contains_colheaders, input_cols \
                , file_header_pattern, output_dt_pattern, output_col_headers, output_file_path, output_order_by


def validate_input_file_control_num(in_file_name, file_hdr_pttrn):
    err_msg_no_in_file = 'Missing input file "{}"!'.format(in_file_name)
    if os.path.exists(in_file_name) == False:
        raise Exception(err_msg_no_in_file)
    else:
        with open(in_file_name, "r") as input_file:
            file_header = input_file.readline()
            records_in_file = len(input_file.readlines()) - 1
        if re.match(file_hdr_pttrn, file_header):
            control_numbers = re.findall(r'[0-9]+/[0-9]+', file_header)[0]
            records_to_check = int(control_numbers.split('/')[0])
            if (records_to_check != records_in_file):
                raise Exception('Record to check: ' + str(records_to_check) + '\n' \
                                + 'Records in file: ' + str(records_in_file) + '\n' \
                                + 'Control numbers not matching!')
            else:
                return True
        else:
            raise Exception('Wrong file header format!')


def read_ra_param():
    msg = "Enter right ascension in degrees (RA) [between 0 and 360 non-inclusive] (or # to exit): "
    ra_prm = input(msg)
    if ra_prm != '#':
        try:
            while not (float(ra_prm) > 0 and float(ra_prm) < 360):
                ra_prm = input(msg)
                if ra_prm == '#':
                    exit()
        except ValueError as e:
            raise Exception("Expecting numeric value!") from e
    else:
        exit()
    return ra_prm


def read_decl_param():
    msg = "Enter observation declination in degrees (Decl) [between -90 and 90 inclusive] (or # to exit): "
    decl_prm = input(msg)
    if decl_prm != '#':
        try:
            while not (float(decl_prm) >= -90 and float(decl_prm) <= 90):
                decl_prm = input(msg)
                if decl_prm == '#':
                    exit()
        except ValueError as e:
            raise Exception("Expecting numeric value!") from e
    return decl_prm


def read_fov_h_param():
    msg = "Enter observation horizontal field of view (fov_h) [between 0 and 360 non-inclusive] (or # to exit): "
    fov_h_prm = input(msg)
    if fov_h_prm != '#':
        try:
            while not (float(fov_h_prm) > 0 and float(fov_h_prm) < 360):
                fov_h_prm = input(msg)
                if fov_h_prm == '#':
                    exit()
        except ValueError as e:
            raise Exception("Expecting numeric value!") from e
    return fov_h_prm


def read_fov_v_param():
    msg = "Enter observation vertical field of view (fov_v) [between -90 and 90 inclusive] (or # to exit): "
    fov_v_prm = input(msg)
    if fov_v_prm != '#':
        try:
            while not (float(fov_v_prm) >= -90 and float(fov_v_prm) <= 90):
                fov_v_prm = input(msg)
                if fov_v_prm == '#':
                    exit()
        except ValueError as e:
            raise Exception("Expecting numeric value!") from e
    return fov_v_prm


def read_top_N_param():
    msg = "Enter the number of the brightest objects to have in the output (top N) [whole positive number]: "
    top_N_prm = input(msg)
    if top_N_prm != '#':
        try:
            while not (int(top_N_prm) > 0):
                top_N_prm = input(msg)
                if top_N_prm == '#':
                    exit()
        except ValueError as e:
            raise Exception("Expecting numeric value!") from e
        return top_N_prm


def read_input_file(input_file, input_file_contains_filehdr):
    in_ds = {}
    with open(input_file, "r") as input_file:
        if input_file_contains_filehdr == True:
            input_file_content = input_file.read().splitlines()[1:]  # skip file header
        else:
            input_file_content = input_file.read().splitlines()
        for row_num in range(len(input_file_content)):
            row = dict(enumerate(input_file_content[row_num].split('\t')))
            in_ds.update({row_num: row})
    return in_ds


def min_ras(ras0, fovh):
    return (ras0 - (fovh / 2))


def max_ras(ras0, fovh):
    return (ras0 + (fovh / 2))


def min_dcl(dcl0, fovv):
    return (dcl0 - (fovv / 2))


def max_dcl(dcl0, fovv):
    return (dcl0 + (fovv / 2))


def check_object_in_fov(ras, dcl, minras, maxras, mindecl, maxdecl):
    if ras >= minras and ras <= maxras and dcl >= mindecl and dcl <= maxdecl:
        return True
    else:
        return False


def calc_dist(ra_1, ra_2, decl_1, decl_2):
    return math.sqrt((ra_2 - ra_1)**2 + (decl_2 - decl_1)**2)


def process_dataset(in_ds, contains_colheaders, ra_param, decl_param, fov_h_param, fov_v_param, in_cols):
    staging_ds = {}
    row_dict = {}
    staging_ds_row_id = 0
    max_bright = -999
    if contains_colheaders:
        del in_ds[0]
    for id, row in in_ds.items():
        if row.get(in_cols.get('1')) != '' \
                and row.get(in_cols.get('2')) != '' \
                and row.get(in_cols.get('3')) != '' \
                and row.get(in_cols.get('4')) != '':
            col_id = int(row.get(in_cols.get('1')))
            col_ra = float(row.get(in_cols.get('2')))
            col_decl = float(row.get(in_cols.get('3')))
            col_bright = float(row.get(in_cols.get('4')))
            if check_object_in_fov(float(col_ra), float(col_decl) \
                    , min_ras(float(ra_param), float(fov_h_param)) \
                    , max_ras(float(ra_param), float(fov_h_param)) \
                    , min_dcl(float(decl_param), float(fov_v_param)) \
                    , max_dcl(float(decl_param), float(fov_v_param))):
                col_dist = calc_dist(ra_param, col_ra, decl_param, col_decl)
                row_dict.update({'ID':col_id})
                row_dict.update({'RA':col_ra})
                row_dict.update({'DEC':col_decl})
                row_dict.update({'BRI':col_bright})
                row_dict.update({'DIST': col_dist})
                staging_ds.update({staging_ds_row_id:row_dict})
            staging_ds_row_id += 1
        else:
            raise Exception('Missing value!')
        row_dict = {}
    return staging_ds


def prep_final_dataset(input_ds, topN, col):
    pivot_ds = {}
    list_keys_to_return = []
    dict_to_return = {}
    for k, v in input_ds.items():
        key_value = {k:v.get(col)}
        pivot_ds.update(key_value)
    if topN > 1:
        max_k = max(pivot_ds, key=pivot_ds.get)
        list_keys_to_return.append(max_k)
        del pivot_ds[max_k]
        topN -= 1
        while len(pivot_ds) > 0 and topN != 0:
            max_k = max(pivot_ds, key=pivot_ds.get)
            list_keys_to_return.append(max_k)
            del pivot_ds[max_k]
            topN -= 1
    k_dict_to_return = 1
    for i in list_keys_to_return:
        dict_to_return.update({k_dict_to_return: list(input_ds.get(i).values())})
        k_dict_to_return += 1
    return dict_to_return


def write_output_file(final_ds, outfile, out_col_hdrs):
    with open(outfile, "w", encoding='UTF8', newline='') as out_file:
        writer = csv.DictWriter(out_file, fieldnames=out_col_hdrs)
        writer.writeheader()
        for key, value_as_list in final_ds.items():
            # out_file.write(','.join([str(elem) for elem in list(row_dict.values())]))
            out_file.write(','.join([str(elem) for elem in value_as_list]))
            out_file.write('\n')


def main():
    config_file = 'config.ini'

    logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')

    INPUT_FILE_NAME, INPUT_FILE_CONTAINS_FILEHEADER, INPUT_FILE_CONTAINS_COLHEADERS, INPUT_COLS, FILE_HEADER_PATTERN \
        , OUTPUT_DT_PATTERN, OUTPUT_COL_HEADERS, OUTPUT_FILE_PATH, OUTPUT_ORDER_BY = read_config(config_file)

    validate_input_file_control_num(INPUT_FILE_NAME, FILE_HEADER_PATTERN)

    ra_param = read_ra_param()
    decl_param = read_decl_param()
    fov_h_param = read_fov_h_param()
    fov_v_param = read_fov_v_param()
    top_N_param = read_top_N_param()

    in_dataset = read_input_file(INPUT_FILE_NAME, INPUT_FILE_CONTAINS_FILEHEADER)

    stg_dataset = process_dataset(in_dataset, INPUT_FILE_CONTAINS_COLHEADERS \
                                    , float(ra_param), float(decl_param), float(fov_h_param), float(fov_v_param) \
                                    , INPUT_COLS)

    final_dataset = prep_final_dataset(stg_dataset, int(top_N_param), OUTPUT_ORDER_BY)

    datetime_stamp = datetime.now().strftime(OUTPUT_DT_PATTERN)
    output_file = os.path.join(OUTPUT_FILE_PATH, '{}.csv'.format(datetime_stamp))

    write_output_file(final_dataset, output_file, OUTPUT_COL_HEADERS)

    print('\nCompleted! Check "{}"'.format(output_file))


if __name__ == '__main__':
    main()

