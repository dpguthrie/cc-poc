{% macro unpack_json(model, variant_column_name) %}

	{% set sql %}

	with

	cte as (
		select
			object_keys({{ variant_column_name }}) as keys
		from {{ model }}
	)

	select distinct k.value::string
	from cte,
	lateral flatten(input => cte.keys) k

	{% endset %}

	{% if execute %}

		{% set keys = run_query(sql).columns[0].values() %}

		{% for key in keys %}

			get({{ variant_column_name }}, '{{ key }}') as {{ key }}{% if not loop.last %},{% endif %}

		{% endfor %}

	{% else %}

		{{ return('NULL') }}

	{% endif %}


{% endmacro %}