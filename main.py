import csv
import json
from datetime import datetime

# список полей, по которым нужно создать заголовки tsv файла.
# эти данные находятся внтури custom_fields_values
custom_fields_values = {
    512318: 'amo_city',
    632884: 'drupal_utm',
    648158: 'tilda_utm_source',
    648160: 'tilda_utm_medium',
    648310: 'tilda_utm_campaign',
    648312: 'tilda_utm_content',
    648314: 'tilda_utm_term',
    648256: 'ct_utm_source',
    648258: 'ct_utm_medium',
    648260: 'ct_utm_campaign',
    648262: 'ct_utm_content',
    648264: 'ct_utm_term',
    648220:'ct_type_communication',
    648276: 'ct_device',
    648278: 'ct_os',
    648280: 'ct_browser',
}


def read_json(file):
    with open(file, 'r') as read_j:
        data = json.load(read_j)
    return data


def get_time_data(timestamp):
    data = {}
    value = datetime.fromtimestamp(timestamp)
    data['created_at_year'] = value.strftime('%Y')
    data['created_a_month'] = value.strftime('%m')
    data['created_at_week'] = value.strftime("%V")
    return data


def get_sub_data(data):
    temp = {}
    if data["field_id"] in custom_fields_values.keys():
        temp[custom_fields_values[data["field_id"]]] = data["values"][0][
            'value']
    return temp


def transform_data(data):
    result = []
    for item in data:
        temp = {}
        # для начала выписал вот так вот все поля, которые получаются
        # напрямую тут тоже потом уберу это или в функцию или в класс пока
        # делаю все напрямую, без функций, так как пытаюсь понять структуру
        # программы. Не опыта видеть в голове сразу весь будущий код
        temp['id'] = item.get('id', None)
        temp["created_at"] = item.get("created_at", None)
        temp["amo_pipeline_id"] = item.get("pipeline_id", None)
        temp["amo_status_id"] = item.get("status_id", None)
        temp["amo_updated_at"] = item.get("updated_at", None)
        temp["amo_trashed_at"] = item.get("trashed_at", None)
        temp["amo_closed_at"] = item.get("closed_at", None)
        for sub_item in item['custom_fields_values']:
            temp.update(get_sub_data(sub_item))

        time_data = get_time_data(item.get("created_at"))
        temp.update(time_data)
        result.append(temp.copy())
    return result


def write_file(data, heading_list):
    with open('results.tsv', 'w', newline='') as tsvfile:
        writer = csv.DictWriter(
            tsvfile,
            fieldnames=heading_list,
            extrasaction='ignore',
            delimiter="\t"
        )
        writer.writeheader()
        for row in data:
            writer.writerow(row)


def main():
    data_read = read_json('amo_json_2020_40.json')
    data = transform_data(data_read)
    headers = [
        'id',
        'created_at',
        'amo_updated_at',
        'amo_trashed_at',
        'amo_pipeline_id',
        'amo_status_id',
        'amo_updated_at',
        'amo_city',
        'drupal_utm',
        'tilda_utm_source',
        'tilda_utm_medium',
        'tilda_utm_campaign',
        'tilda_utm_content',
        'tilda_utm_term',
        'ct_utm_source',
        'ct_utm_medium',
        'ct_utm_campaign',
        'ct_utm_content',
        'ct_utm_term'
        'ct_type_communication',
        'ct_device',
        'ct_os',
        'ct_browser',
        'created_at_year',
        'created_a_month',
        'created_at_week',
        # 'amo_closed_at',
        # 'amo_items_2019',
        # 'amo_items_2020',

    ]
    write_file(data, headers)


if __name__ == "__main__":
    main()
