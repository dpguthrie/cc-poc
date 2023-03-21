{% macro create_fake() %}

create or replace function {{ target.schema }}.fake(locale varchar,provider varchar,parameters variant)
returns variant
language python
runtime_version = '3.8'
packages = ('faker', 'simplejson')
handler = 'fake'
as
$$
import simplejson as json
from faker import Faker
def fake(locale, provider, parameters):
    if type(parameters).__name__ == 'sqlNullWrapper':
        parameters = {}
    fake = Faker(locale=locale)
    return json.loads(json.dumps(fake.format(formatter=provider, **parameters), default=str))
$$;

{% endmacro %}