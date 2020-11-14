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

    def _parse_utm_medium_and_sourse(
            self,
            parse_row,
            ct_utm,
            tilda_utm,
            **kwargs
    ):
        # определение начального индекса нахождения поля, значение которого
        # надо спарсить
        index_utm_source = parse_row.find(kwargs['parse_word'])
        if index_utm_source > 0:
            if parse_row.__contains__(
                    f'{kwargs["source"]}={kwargs["context"]}') or \
                    parse_row.__contains__(
                        f'{kwargs["medium"]}={kwargs["context"]}'):
                return kwargs["context"]
            # определение индекса &, до котрого надо забрать информацию,
            # если он есть
            index_and = parse_row[
                index_utm_source + len(kwargs['parse_word'])].find(
                '&')
            if index_and > 0:
                return parse_row[
                       index_utm_source + len(kwargs['parse_word']):index_and]
            # если & в строке отсутствует, то берем все до конца строки
            return parse_row[index_utm_source + len(kwargs['parse_word']):]
        elif ct_utm != 'None':
            return ct_utm
        else:
            return tilda_utm

    def _parse_utm_campaign_content_term(
            self, parse_row,
            ct_utm,
            tilda_utm,
            parsing_field):
        index_utm_source = parse_row.find(parsing_field)

        if index_utm_source > 0:
            index_and = parse_row[
                index_utm_source + len(parsing_field)].find(
                '&')
            return parse_row[
                   index_utm_source + len(parsing_field): index_and]
        elif ct_utm != 'None':
            return ct_utm
        else:
            return tilda_utm

    def _get_sub_field_by_id(self, row, field_id):
        for field in row:
            if field['field_id'] == field_id:
                return field["values"][0]['value']
        return 'None'

    def _get_parsing_row(self, result):
        parsing_fields = {
            'lead_utm_source': self._parse_utm_medium_and_sourse(
                result['drupal_utm'],
                result['ct_utm_source'],
                result['tilda_utm_source'],
                **{
                    'parse_word': 'utm_source=',
                    'source': 'utm_source=',
                    'medium': 'utm_medium=',
                    'context': 'yandex'
                },
            ),
            'lead_utm_medium': self._parse_utm_medium_and_sourse(
                result['drupal_utm'],
                result['ct_utm_medium'],
                result['tilda_utm_medium'],
                **{
                    'parse_word': 'utm_medium=',
                    'source': 'utm_source=',
                    'medium': 'utm_medium=',
                    'context': 'context'
                }),
            'lead_utm_campaign': self._parse_utm_campaign_content_term(
                result['drupal_utm'],
                result['ct_utm_campaign'],
                result['tilda_utm_campaign'],
                parsing_field='utm_campaign='
            ),
            'lead_utm_content': self._parse_utm_campaign_content_term(
                result['drupal_utm'],
                result['ct_utm_content'],
                result['tilda_utm_content'],
                parsing_field='utm_content='
            ),
            'lead_utm_term': self._parse_utm_campaign_content_term(
                result['drupal_utm'],
                result['ct_utm_term'],
                result['tilda_utm_term'],
                parsing_field='utm_term='
            ),

        }
        return parsing_fields

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
        result.update(self._get_parsing_row(result))
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
