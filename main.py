import csv
import json
from datetime import datetime

import csv
import json
from datetime import datetime

tsv_headers = {
    'id': 'id',
    'created_at': 'created_at',
    'trashed_at': 'amo_trashed_at',
    'pipeline_id': 'amo_pipeline_id',
    'status_id': 'amo_status_id',
    'updated_at': 'amo_updated_at',
    'closed_at': 'amo_closed_at',
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
    648220: 'ct_type_communication',
    648276: 'ct_device',
    648278: 'ct_os',
    648280: 'ct_browser',
    '%Y': 'created_at_year',
    '%m': 'created_a_month',
    '%V': 'created_at_week'
}


# 'amo_closed_at',
# 'amo_items_2019',
# 'amo_items_2020',


class Etl:
    def __init__(self, headers, file):
        self.headers = headers
        self.file = file
        self.data = []

    def extract(self):
        with open(self.file, 'r') as read_j:
            data = json.load(read_j)
        return data

    def transform_row(self, data):
        temp = {}
        for hed in data.keys():
            if hed in self.headers.keys():
                temp[self.headers[hed]] = data[hed]

        for items in data["custom_fields_values"]:
            if items["field_id"] in self.headers.keys():
                temp[self.headers[items["field_id"]]] = items["values"][0][
                    'value']
        return temp

    def transform(self):
        data = self.extract()
        for item in data:
            self.data.append(self.transform_row(item))
        return self.data

    def load(self):
        with open('results_2.tsv', 'w', newline='') as tsvfile:
            writer = csv.DictWriter(
                tsvfile,
                fieldnames=list(self.headers.values()),
                extrasaction='ignore',
                delimiter="\t"
            )
            writer.writeheader()
            for row in self.transform():
                writer.writerow(row)


def main():
    data = Etl(tsv_headers, 'amo_json_2020_40.json')
    data.load()


if __name__ == "__main__":
    main()
