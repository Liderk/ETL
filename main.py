import csv
import json
from datetime import date, datetime


class Etl:
    config = {
        'time_format': '%Y-%m-%d %H:%M:%S',
        'week_offset': {'day0': 5, 'hour0': 18},

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

        'amo_items_2019': 648028,
        'amo_items_2020': 562024,

    }

    def __init__(self, json_file, config=None):
        if config:
            self.config = config
        self.file = json_file
        self.data = []

    def extract(self, data):
        with open(self.file, 'r') as read_j:
            data = json.load(read_j)
        return data

    def _validate_write_logfile(self, error):
        with open(f'{self.file[:-5]}.log', 'a') as logfile:
            logfile.write(error + '\n')

    def _get_error(self, **kwargs):

        if kwargs['field1']:
            if kwargs['field1'] != kwargs['field3']:
                self._validate_write_logfile(
                            f"Конфликт {kwargs['error_source']} "
                            f"в сделке {kwargs['id']}")

        if kwargs['field2']:
            if kwargs['field2'] != kwargs['field3']:
                self._validate_write_logfile(
                            f"Конфликт {kwargs['error_source']} "
                            f"в сделке {kwargs['id']}")

    def validate(self, result):
        self._get_error(**{
            'id': result['id'],
            'field1': result.get('ct_utm_source'),
            'field2': result.get('tilda_utm_source'),
            'field3': result.get('lead_utm_source'),
            'error_source': 'utm_source'
        }
                        )
        self._get_error(**{
            'id': result['id'],
            'field1': result.get('ct_utm_medium'),
            'field2': result.get('tilda_utm_medium'),
            'field3': result.get('lead_utm_medium'),
            'error_source': 'utm_medium'
        }
                        )
        self._get_error(**{
            'id': result['id'],
            'field1': result.get('ct_utm_campaign'),
            'field2': result.get('tilda_utm_campaign'),
            'field3': result.get('lead_utm_campaign'),
            'error_source': 'utm_campaign'
        }
                        )
        self._get_error(**{
            'id': result['id'],
            'field1': result.get('ct_utm_content'),
            'field2': result.get('tilda_utm_content'),
            'field3': result.get('lead_utm_content'),
            'error_source': 'utm_content'
        }
                        )
        self._get_error(**{
            'id': result['id'],
            'field1': result.get('ct_utm_term'),
            'field2': result.get('tilda_utm_term'),
            'field3': result.get('lead_utm_term'),
            'error_source': 'utm_term'
        }
                        )

    def _parse_utm_sourse(self, result_row):
        parse_words = ['yandex', 'google']

        if result_row['drupal_utm']:
            drupal_utm_list = result_row['drupal_utm'].split(', ')
            drupal_utm_dict = dict([
                item.split('=') for item in drupal_utm_list])

            if 'source' not in drupal_utm_dict and result_row.get(
                    'ct_utm_source'):
                return result_row['ct_utm_source']

            if 'source' in drupal_utm_dict:
                if drupal_utm_dict['source'] in parse_words:
                    return drupal_utm_dict['source']

            if 'medium' in drupal_utm_dict:
                if drupal_utm_dict['medium'] in parse_words:
                    return drupal_utm_dict['medium']
            return drupal_utm_dict['source']

        return result_row['tilda_utm_source']

    def _parse_utm_medium(self, result_row):
        parse_words = ['context']

        if result_row['drupal_utm']:
            drupal_utm_list = result_row['drupal_utm'].split(', ')
            drupal_utm_dict = dict([
                item.split('=') for item in drupal_utm_list])

            if 'source' not in drupal_utm_dict and result_row.get(
                    'ct_utm_medium'):
                return result_row['ct_utm_medium']

            if 'source' in drupal_utm_dict:
                if drupal_utm_dict['source'] in parse_words:
                    return drupal_utm_dict['source']

            if 'medium' in drupal_utm_dict:
                if drupal_utm_dict['medium'] in parse_words:
                    return drupal_utm_dict['medium']
            return drupal_utm_dict['medium']

        return result_row['tilda_utm_medium']

    def _parse_utm_campaign_content(self, result_row, parse_sourse):

        if result_row['drupal_utm']:
            drupal_utm_list = result_row['drupal_utm'].split(', ')
            drupal_utm_dict = dict([
                item.split('=') for item in drupal_utm_list])

            if parse_sourse in drupal_utm_dict:
                return drupal_utm_dict[parse_sourse]

            if result_row.get(f'ct_utm_{parse_sourse}'):
                return result_row[f'ct_utm_{parse_sourse}']

        return result_row.get(f'tilda_utm_{parse_sourse}')

    def _parse_utm_term(self, result_row, parse_sourse):

        if result_row['drupal_utm']:
            drupal_utm_list = result_row['drupal_utm'].split(', ')
            drupal_utm_dict = dict([
                item.split('=') for item in drupal_utm_list])

            if parse_sourse in drupal_utm_dict:
                return drupal_utm_dict[parse_sourse]

            if result_row.get('ct_utm_term'):
                return result_row['ct_utm_term']

        return result_row.get('tilda_utm_term')

    def _get_sub_field_by_id(self, row, field_id):
        if row:
            for field in row:
                if field['field_id'] == field_id:
                    return field["values"][0]['value']
        return None

    def _get_parsing_row(self, result):
        parsing_fields = {
            'lead_utm_source': self._parse_utm_sourse(result),
            'lead_utm_medium': self._parse_utm_sourse(result),

            'lead_utm_campaign': self._parse_utm_campaign_content(
                result, 'campaign'),
            'lead_utm_content': self._parse_utm_campaign_content(
                result, 'content'),
            'lead_utm_term': self._parse_utm_term(
                result, 'keyword'),
        }
        return parsing_fields

    def _get_week_number(self, amo_datetime):
        day = amo_datetime.day
        hour = amo_datetime.hour
        month = amo_datetime.month
        year = amo_datetime.year
        week_raw = date(year, month, day).isocalendar()
        if week_raw[2] > self.config['week_offset']['day0']:
            return week_raw[1] + 1
        elif week_raw[2] == self.config['week_offset']['day0'] and hour >= \
                self.config['week_offset']['hour0']:
            return week_raw[1] + 1
        else:
            return week_raw[1]

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

            'ct_type_communication': self._get_sub_field_by_id(
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
            'created_at_bq_timestamp': amo_datetime.strftime(
                self.config['time_format']),

            'created_at_year': amo_datetime.year,
            'created_at_month': amo_datetime.month,
            'created_at_week': self._get_week_number(amo_datetime),

            'amo_items_2019': self._get_sub_field_by_id(
                row['custom_fields_values'],
                self.config['amo_items_2019']),

            'amo_items_2020': self._get_sub_field_by_id(
                row['custom_fields_values'],
                self.config['amo_items_2020']),


        }
        result.update(self._get_parsing_row(result))
        self.validate(result)
        return result

    def transform(self):
        data = self.extract(self.file)
        for row in data:
            self.data.append(self.transform_row(row))
        return self.data

    def get_tsv(self):
        result = self.transform()
        tsv_headers = result[0].keys()
        with open(f'{self.file[:-5]}.tsv', 'w', newline='') as tsvfile:
            writer = csv.DictWriter(
                tsvfile,
                fieldnames=list(tsv_headers),
                extrasaction='ignore',
                delimiter="\t"
            )
            writer.writeheader()
            for row in result:
                writer.writerow(row)


def main():
    # amo_json_2020_40.json - название json файла
    file_json = str(input('Название файла: '))
    config = str(input('Конфиг файла: '))
    a = Etl(file_json, config)
    a.get_tsv()


if __name__ == "__main__":
    main()
