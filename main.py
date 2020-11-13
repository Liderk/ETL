import csv
import json
from datetime import datetime


class Etl:
    config = {
        'amo_city_id': 512318,
        'drupal_utm_id': 632884,
        'tilda_utm_source_id': 648158,
        'tilda_utm_medium_id': 648160,
        'tilda_utm_campaign_id': 648310,
        'tilda_utm_content_id': 648312,
        'tilda_utm_term_id': 648314,
        'ct_utm_source_id': 648256,
        'ct_utm_medium_id': 648258,
        'ct_utm_campaign_id': 648260,
        'ct_utm_content_id': 648262,
        'ct_utm_term_id': 648264,
        'ct_type_communication_id': 648220,
        'ct_device_id': 648276,
        'ct_os_id': 648278,
        'ct_browser_id': 648280,

    }

    def __init__(self, json_file, config=None):
        # self.config = {}
        # if config:
        #     self.config = config
        self.file = json_file
        self.data = []

    def extract(self, data):
        with open(self.file, 'r') as read_j:
            data = json.load(read_j)
        return data

    def _parse_utm_source(self, parse_row, ct_utm_source, tilda_utm_source):
        index_utm_source = parse_row.find('utm_source=')
        if index_utm_source > 0:
            if parse_row.__contains__(
                    'utm_source=yandex') or parse_row.__contains__(
                'utm_medium=yandex'):
                return 'yandex'
            index_and = parse_row[index_utm_source + len('utm_source=')].find(
                '&')
            if index_and > 0:
                return parse_row[
                       index_utm_source + len('utm_source='):index_and]
            return parse_row[index_utm_source + len('utm_source='):]
        elif ct_utm_source != 'None':
            return ct_utm_source
        else:
            return tilda_utm_source

    def _parse_utm_medium(self, parse_row, ct_utm_medium, tilda_utm_medium):
        index_utm_source = parse_row.find('utm_medium=')
        if index_utm_source > 0:
            if parse_row.__contains__(
                    'utm_source=context') or parse_row.__contains__(
                'utm_medium=context'):
                return 'context'
            index_and = parse_row[index_utm_source + len('utm_medium=')].find(
                '&')
            if index_and > 0:
                return parse_row[
                       index_utm_source + len('utm_medium='):index_and]
            return parse_row[index_utm_source + len('utm_medium='):]
        elif ct_utm_medium != 'None':
            return ct_utm_medium
        else:
            return tilda_utm_medium

    def _parse_utm_campaign(
            self, parse_row,
            ct_utm_campaign,
            tilda_utm_campaign):
        index_utm_source = parse_row.find('utm_campaign=')
        index_and = parse_row[index_utm_source + len('utm_campaign=')].find('&')
        if index_utm_source > 0:
            return parse_row[index_utm_source + len('utm_campaign='): index_and]
        elif ct_utm_campaign != 'None':
            return ct_utm_campaign
        else:
            return tilda_utm_campaign

    def _get_sub_field_by_id(self, row, field_id):
        for field in row:
            if field['field_id'] == field_id:
                return field["values"][0]['value']
        return 'None'

    def transform_row(self, row):
        amo_datetime = datetime.fromtimestamp(row['created_at'])

        result = {
            'id': row['id'],
            'created_at': row['created_at'],
            'amo_trashed_at': (
                row['trashed_at'] if 'trashed_at' in row else None),
            'amo_pipeline_id': (
                row['pipeline_id'] if 'pipeline_id' in row else None),
            'amo_status_id': (
                row['status_id'] if 'status_id' in row else None),
            'amo_updated_at': (
                row['updated_at'] if 'updated_at' in row else None),
            'amo_closed_at': (
                row['closed_at'] if 'closed_at' in row else None),
            'amo_city': self._get_sub_field_by_id(
                row["custom_fields_values"],
                self.config['amo_city_id']),
            'drupal_utm': self._get_sub_field_by_id(
                row['custom_fields_values'],
                self.config['drupal_utm_id']),
            'tilda_utm_source': self._get_sub_field_by_id(
                row['custom_fields_values'],
                self.config['tilda_utm_source_id']),
            'tilda_utm_medium': self._get_sub_field_by_id(
                row['custom_fields_values'],
                self.config['tilda_utm_medium_id']),
            'tilda_utm_campaign': self._get_sub_field_by_id(
                row['custom_fields_values'],
                self.config['tilda_utm_campaign_id']),
            'tilda_utm_content': self._get_sub_field_by_id(
                row['custom_fields_values'],
                self.config['tilda_utm_content_id']),
            'tilda_utm_term': self._get_sub_field_by_id(
                row['custom_fields_values'],
                self.config['tilda_utm_term_id']),
            'ct_utm_source': self._get_sub_field_by_id(
                row['custom_fields_values'],
                self.config['ct_utm_source_id']),
            'ct_utm_medium': self._get_sub_field_by_id(
                row['custom_fields_values'],
                self.config['ct_utm_medium_id']),
            'ct_utm_campaign': self._get_sub_field_by_id(
                row['custom_fields_values'],
                self.config['ct_utm_campaign_id']),
            'ct_utm_content': self._get_sub_field_by_id(
                row['custom_fields_values'],
                self.config['ct_utm_content_id']),
            'ct_utm_term': self._get_sub_field_by_id(
                row['custom_fields_values'],
                self.config['ct_utm_term_id']),
            'ct_type_communication':
                self._get_sub_field_by_id(
                    row['custom_fields_values'],
                    self.config['ct_type_communication_id']),
            'ct_device': self._get_sub_field_by_id(
                row['custom_fields_values'],
                self.config['ct_device_id']),
            'ct_os': self._get_sub_field_by_id(
                row['custom_fields_values'],
                self.config['ct_os_id']),
            'ct_browser': self._get_sub_field_by_id(
                row['custom_fields_values'],
                self.config['ct_browser_id']),
            'created_at_year_id': amo_datetime.year,
            'created_at_month_id': amo_datetime.month,

        }

        parsing_fields = {
            'lead_utm_source': self._parse_utm_source(
                result['drupal_utm'],
                result['ct_utm_source'],
                result['tilda_utm_source']),
            'lead_utm_medium': self._parse_utm_medium(
                result['drupal_utm'],
                result['ct_utm_medium'],
                result['tilda_utm_medium']),
            'lead_utm_campaign': self._parse_utm_medium(
                result['drupal_utm'],
                result['ct_utm_campaign'],
                result['tilda_utm_campaign']),

        }
        result.update(parsing_fields)
        return result

    def transform(self):
        data = self.extract(self.file)
        for row in data:
            self.data.append(self.transform_row(row))
        return self.data

    def load(self):
        result = self.transform()
        tsv_headers = result[0].keys()
        with open('results_2.tsv', 'w', newline='') as tsvfile:
            writer = csv.DictWriter(
                tsvfile,
                fieldnames=list(tsv_headers),
                extrasaction='ignore',
                delimiter="\t"
            )
            writer.writeheader()
            for row in result:
                writer.writerow(row)


a = Etl('amo_json_2020_40.json')
a.load()
