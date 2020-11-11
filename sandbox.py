temp = {
    'tilda_utm_source': 648158,
    'tilda_utm_medium': 648160,
    'tilda_utm_campaign': 648310,
    'tilda_utm_content': 648312,
    'tilda_utm_term': 648314,
    'ct_utm_source': 648256,
    'ct_utm_medium': 648258,
    'ct_utm_campaign': 648260,
    'ct_utm_content': 648262,
    'ct_utm_term': 648264,
}
custom_fields_values = {}
for key, value in temp.items():
    custom_fields_values[value] = key
print(custom_fields_values)
