# Пример выполнения простого ETL по требованию заказчика
# Содержание 
  1. [Дано](#Дано)
  2. [Поля из раздела 'custom_fields_values'](#sub_field)
  3. [Поля даты и времени](#time_field)

## Дано <a name="Дано"></a>
Есть json, выгрузка из CRM, необходимо на основе его создать датафрейм с колонками?

'id' - id из JSON

'created_at' - created_at из JSON

'amo_pipeline_id' - pipeline_id из JSON

'amo_status_id' - status_id из JSON

'amo_updated_at' - updated_at из JSON

'amo_trashed_at' - trashed_at из JSON

'amo_closed_at' - closed_at из JSON

#### Поля из раздела 'custom_fields_values' JSON-файла <a name="sub_field"></a>

amo_city' - первое значение параметра Values поля field_id = 512318

'drupal_utm' - первое значение параметра Values поля field_id = 632884

'tilda_utm_source': 648158
 
 'tilda_utm_medium': 648160
  
  'tilda_utm_campaign': 648310
   
  'tilda_utm_content': 648312
    
  'tilda_utm_term': 648314 
    
  'ct_utm_source': 648256 
  
  'ct_utm_medium': 648258 
  
  'ct_utm_campaign': 648260 
  
  'ct_utm_content': 648262 
  
  'ct_utm_term': 648264
  
'ct_type_communication': 648220

'ct_device': 648276 

'ct_os': 648278 

'ct_browser': 648280

#### Поля вычисляемые из даты (формат UNIX timestamp) поля created_at: <a name="time_field"></a>
'created_at_bq_timestamp' - дата и время, приведённые к формату DATETIME Google BigQuery
created_at_year - год в формате YYYY, к которому относится дата в created_at
created_a_month - месяц в формате MM
created_at_week - номер недели в году. Неделя начинается в пятницу в 18:00 по московскому времени. (Хорошо бы вынести в конфиг время разделения недели, чтобы можно было это быстро менять). Первая неделя года - та, к которой относится первый четверг года.

