import csv
import argparse
import pandas as pd
from pathlib import Path

DAS_SERVER = 'DASMBTCP'

MODE_HEADER = [':mode=update']

ALARM_GROUP_HEADER = [':AlarmGroup', 'Group', 'Comment', 'EventLogged', 'EventLoggingPriority', 'LoLoAlarmDisable',
                      'LoAlarmDisable', 'HiAlarmDisable', 'HiHiAlarmDisable', 'MinDevAlarmDisable',
                      'MajDevAlarmDisable', 'RocAlarmDisable', 'DSCAlarmDisable', 'LoLoAlarmInhibitor',
                      'LoAlarmInhibitor', 'HiAlarmInhibitor', 'HiHiAlarmInhibitor', 'MinDevAlarmInhibitor',
                      'MajDevAlarmInhibitor', 'RocAlarmInhibitor', 'DSCAlarmInhibitor']

IO_ACCESS_HEADER = [':IOAccess', 'Application', 'Topic', 'AdviseActive', 'DDEProtocol', 'SecApplication', 'SecTopic',
                    'SecAdviseActive', 'SecDDEProtocol', 'FailoverExpression', 'FailoverDeadband', 'DFOFlag',
                    'FBDFlag', 'FailbackDeadband']

IO_DISCRETE_HEADER = [':IODisc', 'Group', 'Comment', 'Logged', 'EventLogged', 'EventLoggingPriority', 'RetentiveValue',
                      'InitialDisc', 'OffMsg', 'OnMsg', 'AlarmState', 'AlarmPri', 'DConversion', 'AccessName',
                      'ItemUseTagname', 'ItemName', 'ReadOnly', 'AlarmComment', 'AlarmAckModel', 'DSCAlarmDisable',
                      'DSCAlarmInhibitor', 'SymbolicName']

IO_INTEGER_HEADER = [':IOInt', 'Group', 'Comment', 'Logged', 'EventLogged', 'EventLoggingPriority', 'RetentiveValue',
                     'RetentiveAlarmParameters', 'AlarmValueDeadband', 'AlarmDevDeadband',
                     'EngUnits', 'InitialValue', 'MinEU', 'MaxEU', 'Deadband', 'LogDeadband', 'LoLoAlarmState',
                     'LoLoAlarmValue', 'LoLoAlarmPri', 'LoAlarmState', 'LoAlarmValue', 'LoAlarmPri',
                     'HiAlarmState', 'HiAlarmValue', 'HiAlarmPri', 'HiHiAlarmState', 'HiHiAlarmValue', 'HiHiAlarmPri',
                     'MinorDevAlarmState', 'MinorDevAlarmValue', 'MinorDevAlarmPri',
                     'MajorDevAlarmState', 'MajorDevAlarmValue', 'MajorDevAlarmPri', 'DevTarget',
                     'ROCAlarmState', 'ROCAlarmValue', 'ROCAlarmPri', 'ROCTimeBase', 'MinRaw', 'MaxRaw', 'Conversion',
                     'AccessName',
                     'ItemUseTagname', 'ItemName', 'ReadOnly', 'AlarmComment', 'AlarmAckModel', 'LoLoAlarmDisable',
                     'LoAlarmDisable', 'HiAlarmDisable', 'HiHiAlarmDisable',
                     'MinDevAlarmDisable', 'MajDevAlarmDisable', 'RocAlarmDisable', 'LoLoAlarmInhibitor',
                     'LoAlarmInhibitor', 'HiAlarmInhibitor', 'HiHiAlarmInhibitor', 'MinDevAlarmInhibitor',
                     'MajDevAlarmInhibitor', 'RocAlarmInhibitor', 'SymbolicName']

IO_REAL_HEADER = [':IOReal', 'Group', 'Comment', 'Logged', 'EventLogged', 'EventLoggingPriority', 'RetentiveValue',
                  'RetentiveAlarmParameters', 'AlarmValueDeadband', 'AlarmDevDeadband', 'EngUnits', 'InitialValue',
                  'MinEU', 'MaxEU',
                  'Deadband', 'LogDeadband', 'LoLoAlarmState', 'LoLoAlarmValue', 'LoLoAlarmPri', 'LoAlarmState',
                  'LoAlarmValue', 'LoAlarmPri', 'HiAlarmState', 'HiAlarmValue', 'HiAlarmPri', 'HiHiAlarmState',
                  'HiHiAlarmValue', 'HiHiAlarmPri', 'MinorDevAlarmState', 'MinorDevAlarmValue',
                  'MinorDevAlarmPri', 'MajorDevAlarmState', 'MajorDevAlarmValue', 'MajorDevAlarmPri', 'DevTarget',
                  'ROCAlarmState', 'ROCAlarmValue', 'ROCAlarmPri',
                  'ROCTimeBase', 'MinRaw', 'MaxRaw', 'Conversion', 'AccessName', 'ItemUseTagname', 'ItemName',
                  'ReadOnly', 'AlarmComment', 'AlarmAckModel', 'LoLoAlarmDisable', 'LoAlarmDisable', 'HiAlarmDisable',
                  'HiHiAlarmDisable', 'MinDevAlarmDisable', 'MajDevAlarmDisable', 'RocAlarmDisable',
                  'LoLoAlarmInhibitor', 'LoAlarmInhibitor', 'HiAlarmInhibitor', 'HiHiAlarmInhibitor',
                  'MinDevAlarmInhibitor', 'MajDevAlarmInhibitor', 'RocAlarmInhibitor', 'SymbolicName']

CSV_HEADER = ['CODE', 'PLC_TAG', 'SCADA_TAG', 'PLC_ADDRESS', 'SCADA_ADDRESS', 'PARENT_OBJECT', 'PARENT_OBJECT_TYPE',
              'DESCRIPTION', 'RANGE_UNITS', 'TAG_LEN', 'DESCRIPTION_LEN', 'EXTRA', 'COMMENT', 'REVISION',
              'REVISION_COMMENT', 'COLOR_CODE']


class Tag(object):
    def __init__(self, name, description, address, data_type, units, data_range):
        self.name = name
        self.description = description
        self.address = address
        self.data_type = data_type
        self.units = units
        self.lower_range, self.upper_range = data_range
        self.alarm = False


def get_plc_data_from_xls(filename):
    path = Path(filename)
    xls_file = pd.ExcelFile(path)
    data_frame = xls_file.parse('SCADA_DB')

    tag_list = []

    for index, row in data_frame.iterrows():
        tag_name = row['SCADA_TAG']
        address = row['SCADA_ADDRESS']
        description = row['DESCRIPTION']
        data_type = row['DATA_TYPE']
        range_units = str(row['RANGE_UNITS'])
        alarm = row['ALARM']

        lower_range = ''
        upper_range = ''
        units = ''

        if range_units and data_type != 'BOOL':
            if len(range_units.split()) > 1:
                data_range, units = range_units.split()
                lower_range, upper_range = data_range.split('-')
            else:
                units = range_units
                lower_range = '-32768'
                upper_range = '32767'

        tag = Tag(tag_name, description, address, data_type, units, (lower_range, upper_range))
        if alarm == 'Y':
            tag.alarm = True
        tag_list.append(tag)

    return tag_list


def main(args):
    topic = args.topic
    input_file = Path(args.input_file)
    output_file = input_file.parent / '{}_DB.csv'.format(topic)
    booleans = []
    integers = []
    floats = []

    tags = get_plc_data_from_xls(input_file)
    for tag in tags:
        if tag.data_type == 'BOOL':
            booleans.append(tag)
        elif tag.data_type == 'DINT' or tag.data_type == 'INT':
            integers.append(tag)
        elif tag.data_type == 'REAL':
            floats.append(tag)

    with open(output_file, 'w', newline='') as csv_output_file:
        writer = csv.DictWriter(csv_output_file, fieldnames=MODE_HEADER)
        writer.writeheader()

        # Alarm Group
        writer.fieldnames = ALARM_GROUP_HEADER
        writer.writeheader()
        writer.writerow(
            {
                ':AlarmGroup': topic,
                'Group': '$System',
                'Comment': '',
                'EventLogged': 'Yes',
                'EventLoggingPriority': '999',
                'LoLoAlarmDisable': '0',
                'LoAlarmDisable': '0',
                'HiAlarmDisable': '0',
                'HiHiAlarmDisable': '0',
                'MinDevAlarmDisable': '0',
                'MajDevAlarmDisable': '0',
                'RocAlarmDisable': '0',
                'DSCAlarmDisable': '0'
            }
        )

        # Access Topic
        writer.fieldnames = IO_ACCESS_HEADER
        writer.writeheader()
        writer.writerow({
            ':IOAccess': topic,
            'Application': DAS_SERVER,
            'Topic': topic,
            'AdviseActive': 'Yes',
            'DDEProtocol': 'No'})

        # Discrete Tags
        writer.fieldnames = IO_DISCRETE_HEADER
        writer.writeheader()
        for tag in booleans:
            writer.writerow({
                ':IODisc': tag.name,
                'Group': topic,
                'Comment': tag.description,
                'Logged': 'No',
                'EventLogged': 'No',
                'RetentiveValue': 'No',
                'EventLoggingPriority': '0',
                'InitialDisc': 'Off',
                'AlarmState': 'On' if tag.alarm else 'Off',
                'AlarmPri': '1',
                'DConversion': 'Direct',
                'AccessName': topic,
                'ItemUseTagname': 'No',
                'ReadOnly': 'No',
                'AlarmComment': tag.description if tag.alarm else '',
                'AlarmAckModel': '0',
                'DSCAlarmDisable': '0',
                'ItemName': str(tag.address).zfill(6),
                'OffMsg': 'OK' if tag.alarm else 'Off',
                'OnMsg': 'In Alarm' if tag.alarm else 'On'
            })

        # Integer Tags
        writer.fieldnames = IO_INTEGER_HEADER
        writer.writeheader()
        for tag in integers:
            writer.writerow({
                ':IOInt': tag.name,
                'Group': topic,
                'Comment': tag.description,
                'Logged': 'No',
                'EventLogged': 'No',
                'EventLoggingPriority': '0',
                'RetentiveValue': 'No',
                'RetentiveAlarmParameters': 'No',
                'AlarmValueDeadband': '0',
                'AlarmDevDeadband': '0',
                'EngUnits': tag.units,
                'InitialValue': '0',
                'MinEU': '0',
                'MaxEU': '32767',
                'Deadband': '0',
                'LogDeadband': '0',
                'LoLoAlarmState': 'Off',
                'LoLoAlarmValue': '0',
                'LoLoAlarmPri': '1',
                'LoAlarmState': 'Off',
                'LoAlarmValue': '0',
                'LoAlarmPri': '1',
                'HiAlarmState': 'Off',
                'HiAlarmValue': '0',
                'HiAlarmPri': '1',
                'HiHiAlarmState': 'Off',
                'HiHiAlarmValue': '0',
                'HiHiAlarmPri': '1',
                'MinorDevAlarmState': 'Off',
                'MinorDevAlarmValue': '0',
                'MinorDevAlarmPri': '1',
                'MajorDevAlarmState': 'Off',
                'MajorDevAlarmValue': '0',
                'MajorDevAlarmPri': '1',
                'DevTarget': '0',
                'ROCAlarmState': 'Off',
                'ROCAlarmValue': '0',
                'ROCAlarmPri': '1',
                'ROCTimeBase': 'Min',
                'MinRaw': '0',
                'MaxRaw': '32767',
                'Conversion': 'Linear',
                'AccessName': topic,
                'ItemUseTagname': 'No',
                'ItemName': '{} I'.format(tag.address),
                'ReadOnly': 'No',
                'AlarmComment': '',
                'AlarmAckModel': '0',
                'LoLoAlarmDisable': '0',
                'LoAlarmDisable': '0',
                'HiAlarmDisable': '0',
                'HiHiAlarmDisable': '0',
                'MinDevAlarmDisable': '0',
                'MajDevAlarmDisable': '0',
                'RocAlarmDisable': '0'})

        # Real Tags
        writer.fieldnames = IO_REAL_HEADER
        writer.writeheader()
        for tag in floats:
            writer.writerow({
                ':IOReal': tag.name,
                'Group': topic,
                'Comment': tag.description,
                'Logged': 'No',
                'EventLogged': 'No',
                'EventLoggingPriority': '0',
                'RetentiveValue': 'No',
                'RetentiveAlarmParameters': 'No',
                'AlarmValueDeadband': '0',
                'AlarmDevDeadband': '0',
                'EngUnits': tag.units,
                'InitialValue': tag.lower_range,
                'MinEU': tag.lower_range,
                'MaxEU': tag.upper_range,
                'Deadband': '0',
                'LogDeadband': '0',
                'LoLoAlarmState': 'Off',
                'LoLoAlarmValue': '0',
                'LoLoAlarmPri': '1',
                'LoAlarmState': 'Off',
                'LoAlarmValue': '0',
                'LoAlarmPri': '1',
                'HiAlarmState': 'Off',
                'HiAlarmValue': '0',
                'HiAlarmPri': '1',
                'HiHiAlarmState': 'Off',
                'HiHiAlarmValue': '0',
                'HiHiAlarmPri': '1',
                'MinorDevAlarmState': 'Off',
                'MinorDevAlarmValue': '0',
                'MinorDevAlarmPri': '1',
                'MajorDevAlarmState': 'Off',
                'MajorDevAlarmValue': '0',
                'MajorDevAlarmPri': '1',
                'DevTarget': '0',
                'ROCAlarmState': 'Off',
                'ROCAlarmValue': '0',
                'ROCAlarmPri': '1',
                'ROCTimeBase': 'Min',
                'MinRaw': tag.lower_range,
                'MaxRaw': tag.upper_range,
                'Conversion': 'Linear',
                'AccessName': topic,
                'ItemUseTagname': 'No',
                'ItemName': '{} F'.format(tag.address),
                'ReadOnly': 'No',
                'AlarmComment': '',
                'AlarmAckModel': '0',
                'LoLoAlarmDisable': '0',
                'LoAlarmDisable': '0',
                'HiAlarmDisable': '0',
                'HiHiAlarmDisable': '0',
                'MinDevAlarmDisable': '0',
                'MajDevAlarmDisable': '0',
                'RocAlarmDisable': '0',
                'LoLoAlarmInhibitor': '',
                'LoAlarmInhibitor': '',
                'HiAlarmInhibitor': '',
                'HiHiAlarmInhibitor': '',
                'MinDevAlarmInhibitor': '',
                'MajDevAlarmInhibitor': '',
                'RocAlarmInhibitor': '',
                'SymbolicName': ''})


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input-file', type=str, action='store', dest='input_file')
    parser.add_argument('-t', '--topic', type=str, action='store', dest='topic')
    main(parser.parse_args())
