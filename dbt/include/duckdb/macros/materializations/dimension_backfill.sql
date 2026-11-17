{#
    dimension_backfill: SCD2 dimensions for local DuckDB SQL models, fed by
    (possibly late arriving) attribute observations.

    A model materialized with `dimension_backfill` selects *complete attribute
    observations*: one row per (business key, effective time) describing the
    dimension record as of that instant.

        {{ config(
            materialized='dimension_backfill',
            unique_key='customer_id',
            updated_at='effective_at',
            dimension_attributes=['name', 'segment', 'region'],
            fact_tables=[{
                'relation': 'sales',
                'unique_key': 'customer_id',
                'occurred_at': 'sold_at',
                'version_column': 'customer_version_id',
            }],
        ) }}

        select customer_id, effective_at, name, segment, region
        from {{ ref('customer_observations') }}

    Each run upserts the batch into a persistent observation store
    (`<model>__dbt_observations`), so late observations are kept rather than
    flattened into whatever the source happens to return today:

      * an observation is identified by its business key and effective time, so a
        re-sent observation with the same start corrects ("upserts") the stored
        one instead of opening a second version,
      * observations whose attributes repeat an earlier observation are kept in
        the store but do not open a new SCD2 version,
      * two different attribute tuples for the same (key, effective time) in the
        same batch contradict each other and abort the run with an error.

    The model relation itself is then rebuilt, inside the same transaction, as
    SCD2 history over every stored observation: one row per *change point*, valid
    over the half-open interval [valid_from, next change point). A change point
    that already existed keeps its `version_column` value from the previous
    build; only a newly appearing change point gets a new one.

    Finally, every declared fact table has its version column re-linked: the
    version is the history row covering (business key, occurrence time), so a
    fact that falls exactly on a boundary gets the *new* version, a fact that no
    version covers gets NULL, and no other fact column is touched.

    Observation store, history and fact re-links are written in the model's dbt
    transaction. An in-transaction failure (including a post-hook) rolls all of
    them back, leaving the previous observation store, history and fact versions
    in place.
#}

{% macro duckdb__dimension_backfill_config() %}
  {#-- Read and validate the model config. Everything the macros below need is
       returned in a single mapping so the column names are resolved once. --#}

  {%- set unique_key = normalize_string_or_list(config.get('unique_key'), 'unique_key') -%}
  {%- set updated_at = config.get('updated_at') -%}
  {%- set attributes = normalize_string_or_list(
        config.get('dimension_attributes'), 'dimension_attributes') -%}
  {%- set version_column = config.get('version_column') or 'dimension_version_id' -%}
  {%- set history_start_column = config.get('history_start_column') or 'valid_from' -%}
  {%- set history_end_column = config.get('history_end_column') or 'valid_to' -%}

  {%- if not unique_key -%}
    {% do exceptions.raise_compiler_error(
      "dimension_backfill models must set `unique_key` to the business key column(s) "
      ~ "of the dimension, e.g. unique_key='customer_id'.") %}
  {%- endif -%}
  {%- if updated_at is not string or not updated_at -%}
    {% do exceptions.raise_compiler_error(
      "dimension_backfill models must set `updated_at` to the effective time column "
      ~ "of the observations, e.g. updated_at='effective_at'.") %}
  {%- endif -%}
  {%- if not attributes -%}
    {% do exceptions.raise_compiler_error(
      "dimension_backfill models must set `dimension_attributes` to the attribute "
      ~ "columns of the observations.") %}
  {%- endif -%}

  {%- set reserved = [] -%}
  {%- for name in unique_key + [updated_at, version_column, history_start_column, history_end_column] -%}
    {%- if name in reserved -%}
      {% do exceptions.raise_compiler_error(
        "dimension_backfill config uses the column name '" ~ name ~ "' twice.") %}
    {%- endif -%}
    {%- do reserved.append(name) -%}
  {%- endfor -%}
  {%- for name in attributes -%}
    {%- if name in reserved -%}
      {% do exceptions.raise_compiler_error(
        "dimension_backfill attribute '" ~ name ~ "' collides with a business key, "
        ~ "effective time or dbt-managed column name.") %}
    {%- endif -%}
    {%- do reserved.append(name) -%}
  {%- endfor -%}

  {#-- Fact tables: relation name plus the columns that link a fact to its version.
       The business key and occurrence time default to this model's own settings. --#}
  {%- set raw_fact_tables = config.get('fact_tables') or [] -%}
  {%- if raw_fact_tables is mapping -%}
    {%- set raw_fact_tables = [raw_fact_tables] -%}
  {%- endif -%}
  {%- if raw_fact_tables is not sequence or raw_fact_tables is string -%}
    {% do exceptions.raise_compiler_error(
      "dimension_backfill `fact_tables` must be a list of mappings with a `relation` key.") %}
  {%- endif -%}

  {%- set fact_tables = [] -%}
  {%- for raw in raw_fact_tables -%}
    {%- if raw is not mapping or not raw.get('relation') -%}
      {% do exceptions.raise_compiler_error(
        "dimension_backfill `fact_tables` entries must be mappings with a `relation` key.") %}
    {%- endif -%}
    {%- set fact_key = normalize_string_or_list(raw.get('unique_key'), 'fact_tables.unique_key') -%}
    {%- if fact_key | length == 0 -%}
      {%- set fact_key = unique_key -%}
    {%- elif fact_key | length != unique_key | length -%}
      {% do exceptions.raise_compiler_error(
        "dimension_backfill fact table '" ~ raw.get('relation') ~ "' declares "
        ~ (fact_key | length) ~ " business key column(s) where the dimension has "
        ~ (unique_key | length) ~ ".") %}
    {%- endif -%}
    {%- do fact_tables.append({
      'relation': raw.get('relation'),
      'unique_key': fact_key,
      'occurred_at': raw.get('occurred_at') or updated_at,
      'version_column': raw.get('version_column') or version_column,
    }) -%}
  {%- endfor -%}

  {{ return({
    'unique_key': unique_key,
    'updated_at': updated_at,
    'attributes': attributes,
    'version_column': version_column,
    'history_start_column': history_start_column,
    'history_end_column': history_end_column,
    'fact_tables': fact_tables,
  }) }}
{%- endmacro %}


{% macro duckdb__dimension_backfill_quote_columns(columns) %}
  {%- set quoted = [] -%}
  {%- for column in columns -%}
    {%- do quoted.append(adapter.quote(column)) -%}
  {%- endfor -%}
  {{ return(quoted) }}
{%- endmacro %}


{% macro duckdb__dimension_backfill_relation_exists(relation) %}
  {#-- A non-throwing existence check. `get_columns_in_relation` on a missing
       relation makes the connection manager roll back the model's transaction,
       which would discard the observation batch and everything built so far. --#}
  {%- set database = adapter.escape_sql_string(relation.database or '') -%}
  {%- set schema = adapter.escape_sql_string(relation.schema or '') -%}
  {%- set identifier = adapter.escape_sql_string(relation.identifier) -%}
  {%- set found = run_query(
    "select 1 as dbt_found from duckdb_tables()
     where lower(database_name) = lower('" ~ database ~ "')
       and lower(schema_name) = lower('" ~ schema ~ "')
       and lower(table_name) = lower('" ~ identifier ~ "')
     union all
     select 1 from duckdb_views()
     where lower(database_name) = lower('" ~ database ~ "')
       and lower(schema_name) = lower('" ~ schema ~ "')
       and lower(view_name) = lower('" ~ identifier ~ "')") -%}
  {{ return(found | length > 0) }}
{%- endmacro %}


{% macro duckdb__dimension_backfill_observations_sql(compiled_code) %}
  {#-- The observation batch holds every column the model selects, so the column
       set can be compared with the config before anything is stored. Duplicate
       observations inside the batch collapse with `select distinct`; same-start
       corrections are applied when the batch is upserted into the store. --#}
  select distinct *
  from (
    {{ compiled_code }}
  ) as dbt_dimension_observation
{%- endmacro %}


{% macro duckdb__dimension_backfill_contradictions_sql(relation, settings) %}
  {#-- More than one distinct attribute tuple for the same (business key,
       effective time) inside a single batch cannot be resolved: the observations
       contradict each other, so the run must fail rather than pick one. --#}
  {%- set keys = duckdb__dimension_backfill_quote_columns(settings.unique_key) -%}
  {%- set attributes = duckdb__dimension_backfill_quote_columns(settings.attributes) -%}
  {%- set updated_at = adapter.quote(settings.updated_at) -%}
  select
    {{ keys | join(', ') }},
    {{ updated_at }} as dbt_effective_at,
    count(distinct row({{ attributes | join(', ') }})) as dbt_attribute_count
  from {{ relation }}
  group by {{ keys | join(', ') }}, {{ updated_at }}
  having count(distinct row({{ attributes | join(', ') }})) > 1
  order by {{ updated_at }}
{%- endmacro %}


{% macro duckdb__dimension_backfill_upsert_sql(observation_store, relation, settings) %}
  {#-- Same-start observations are corrections: drop the stored rows for every
       (business key, effective time) the batch touches, then add the batch back.
       Rows for other start points are untouched. --#}
  {%- set keys = duckdb__dimension_backfill_quote_columns(settings.unique_key) -%}
  {%- set attributes = duckdb__dimension_backfill_quote_columns(settings.attributes) -%}
  {%- set updated_at = adapter.quote(settings.updated_at) -%}
  {%- set match = namespace(parts=[]) -%}
  {%- for key in keys -%}
    {%- do match.parts.append(
      'stored.' ~ key ~ ' is not distinct from batch.' ~ key) -%}
  {%- endfor -%}
  delete from {{ observation_store }} as stored
  where exists (
    select 1
    from {{ relation }} as batch
    where {{ match.parts | join(' and ') }}
      and stored.{{ updated_at }} is not distinct from batch.{{ updated_at }}
  );
  insert into {{ observation_store }} (
    {{ keys | join(', ') }}, {{ updated_at }}, {{ attributes | join(', ') }}
  )
  select distinct
    {{ keys | join(', ') }}, {{ updated_at }}, {{ attributes | join(', ') }}
  from {{ relation }}
{%- endmacro %}


{% macro duckdb__dimension_backfill_validate(batch_relation, settings) %}
  {#-- Reject a batch that cannot be turned into SCD2 history: missing columns,
       an observation without a business key or effective time, or two different
       attribute tuples for the same (business key, effective time). --#}
  {%- set available = adapter.get_columns_in_relation(batch_relation) -%}
  {%- set available_names = [] -%}
  {%- for column in available -%}
    {%- do available_names.append(column.name | lower) -%}
  {%- endfor -%}

  {%- set required = settings.unique_key + [settings.updated_at] + settings.attributes -%}
  {%- set missing = [] -%}
  {%- for name in required -%}
    {%- if name | lower not in available_names -%}
      {%- do missing.append(name) -%}
    {%- endif -%}
  {%- endfor -%}
  {%- if missing -%}
    {% do exceptions.raise_compiler_error(
      "dimension_backfill model must select the configured columns, but "
      ~ missing | join(', ') ~ " is missing from the observations. Available columns: "
      ~ available_names | join(', ') ~ ".") %}
  {%- endif -%}

  {%- set keys = duckdb__dimension_backfill_quote_columns(settings.unique_key) -%}
  {%- set null_checks = [] -%}
  {%- for key in keys -%}
    {%- do null_checks.append(key ~ ' is null') -%}
  {%- endfor -%}
  {%- do null_checks.append(adapter.quote(settings.updated_at) ~ ' is null') -%}
  {%- set null_result = run_query(
    'select count(*) as dbt_null_count from ' ~ batch_relation ~ ' where '
    ~ null_checks | join(' or ')) -%}
  {%- set null_count = null_result.rows[0][0] -%}
  {%- if null_count > 0 -%}
    {% do exceptions.raise_compiler_error(
      "dimension_backfill observations must have a business key and an effective time, "
      ~ "but " ~ null_count ~ " observation(s) have a NULL in "
      ~ (settings.unique_key | join(', ')) ~ " or " ~ settings.updated_at
      ~ ". Versions cannot be ordered or matched without them.") %}
  {%- endif -%}

  {%- set contradictions = run_query(
    duckdb__dimension_backfill_contradictions_sql(batch_relation, settings)) -%}
  {%- if contradictions | length > 0 -%}
    {%- set key_count = settings.unique_key | length -%}
    {%- set problems = [] -%}
    {%- for row in contradictions.rows -%}
      {%- set values = [] -%}
      {%- for index in range(key_count) -%}
        {%- do values.append(row[index] | string) -%}
      {%- endfor -%}
      {%- do problems.append(
        '(' ~ values | join(', ') ~ ') at ' ~ (row[key_count] | string)
        ~ ' has ' ~ (row[key_count + 1] | string) ~ ' different attribute observations') -%}
    {%- endfor -%}
    {% do exceptions.raise_compiler_error(
      "dimension_backfill found contradictory observations in this batch for the same "
      ~ "business key and effective time, so no version can be chosen: "
      ~ problems | join('; ')
      ~ ". Deduplicate the source or send a single corrected observation per start point.") %}
  {%- endif -%}
{%- endmacro %}


{% macro duckdb__dimension_backfill_history_sql(settings, observation_store, prior_relation) %}
  {#-- Rebuild SCD2 history from the whole observation store.

       A change point is an observation whose attributes differ from the
       observation before it for the same business key, compared NULL-safely.
       Consecutive observations with the same attributes therefore collapse into
       the version opened by the first of them: they stay in the store but do not
       open a version. Each version is valid over [start, next change point).

       The version identifier is derived from the business key and the start
       point, and a start point that already existed keeps the identifier it had
       in the previous history, so reruns do not renumber unchanged versions. --#}
  {%- set keys = duckdb__dimension_backfill_quote_columns(settings.unique_key) -%}
  {%- set attributes = duckdb__dimension_backfill_quote_columns(settings.attributes) -%}
  {%- set key_list = keys | join(', ') -%}
  {%- set attribute_list = attributes | join(', ') -%}
  {%- set current_keys = [] -%}
  {%- set current_attributes = [] -%}
  {%- for name in keys -%}
    {%- do current_keys.append('dbt_current.' ~ name) -%}
  {%- endfor -%}
  {%- for name in attributes -%}
    {%- do current_attributes.append('dbt_current.' ~ name) -%}
  {%- endfor -%}
  {%- set updated_at = adapter.quote(settings.updated_at) -%}
  {%- set start_column = adapter.quote(settings.history_start_column) -%}
  {%- set end_column = adapter.quote(settings.history_end_column) -%}
  {%- set version_column = adapter.quote(settings.version_column) -%}
  {%- set id_parts = [] -%}
  {%- for key in keys -%}
    {%- do id_parts.append(
      'coalesce(cast(dbt_current.' ~ key ~ ' as varchar), chr(30))') -%}
  {%- endfor -%}
  {%- do id_parts.append('cast(dbt_current.' ~ start_column ~ ' as varchar)') -%}
  {%- set new_version = 'md5(concat_ws(chr(31), ' ~ id_parts | join(', ') ~ '))' -%}
  with observations as (
    select {{ key_list }}, {{ updated_at }}, {{ attribute_list }}
    from {{ observation_store }}
  ),
  change_points as (
    select
      {{ key_list }},
      {{ attribute_list }},
      {{ updated_at }} as {{ start_column }},
      row({{ attribute_list }}) is distinct from
        lag(row({{ attribute_list }})) over (
          partition by {{ key_list }} order by {{ updated_at }}
        ) as dbt_is_change_point
    from observations
  ),
  versions as (
    select
      {{ key_list }},
      {{ attribute_list }},
      {{ start_column }},
      lead({{ start_column }}) over (
        partition by {{ key_list }} order by {{ start_column }}
      ) as {{ end_column }}
    from change_points
    where dbt_is_change_point
  )
  select
    {{ current_keys | join(', ') }},
    {{ current_attributes | join(', ') }},
    dbt_current.{{ start_column }},
    dbt_current.{{ end_column }},
    {%- if prior_relation is not none %}
    coalesce(prior.{{ version_column }}, {{ new_version }}) as {{ version_column }}
    {%- else %}
    {{ new_version }} as {{ version_column }}
    {%- endif %}
  from versions as dbt_current
  {%- if prior_relation is not none %}
  left join {{ prior_relation }} as prior
    on prior.{{ start_column }} is not distinct from dbt_current.{{ start_column }}
    {%- for key in keys %}
    and prior.{{ key }} is not distinct from dbt_current.{{ key }}
    {%- endfor %}
  {%- endif %}
{%- endmacro %}


{% macro duckdb__dimension_backfill_fact_relation(raw, target_relation) %}
  {#-- A fact table is named relative to the model: "sales", "schema.sales" or
       "database.schema.sales". --#}
  {%- if raw is not string -%}
    {% do exceptions.raise_compiler_error(
      "dimension_backfill `fact_tables` entries must name the fact table as a string, "
      ~ "relative to the model's schema. Got: " ~ raw) %}
  {%- endif -%}
  {%- set parts = [] -%}
  {%- for part in raw.split('.') -%}
    {%- do parts.append(part | trim) -%}
  {%- endfor -%}
  {%- if parts | length == 1 -%}
    {%- set database = target_relation.database -%}
    {%- set schema = target_relation.schema -%}
    {%- set identifier = parts[0] -%}
  {%- elif parts | length == 2 -%}
    {%- set database = target_relation.database -%}
    {%- set schema = parts[0] -%}
    {%- set identifier = parts[1] -%}
  {%- elif parts | length == 3 -%}
    {%- set database = parts[0] -%}
    {%- set schema = parts[1] -%}
    {%- set identifier = parts[2] -%}
  {%- else -%}
    {% do exceptions.raise_compiler_error(
      "dimension_backfill fact table '" ~ raw ~ "' must be 'table', 'schema.table' "
      ~ "or 'database.schema.table'.") %}
  {%- endif -%}
  {{ return(api.Relation.create(
    database=database, schema=schema, identifier=identifier, type='table')) }}
{%- endmacro %}


{% macro duckdb__dimension_backfill_check_fact(fact, fact_relation, settings) %}
  {%- if not duckdb__dimension_backfill_relation_exists(fact_relation) -%}
    {% do exceptions.raise_compiler_error(
      "dimension_backfill fact table " ~ fact_relation ~ " does not exist. Create it, or "
      ~ "remove it from the model's `fact_tables` config.") %}
  {%- endif -%}
  {%- set columns = adapter.get_columns_in_relation(fact_relation) -%}
  {%- set types = {} -%}
  {%- for column in columns -%}
    {%- do types.update({column.name | lower: column.dtype}) -%}
  {%- endfor -%}

  {%- set required = fact.unique_key + [fact.occurred_at, fact.version_column] -%}
  {%- set missing = [] -%}
  {%- for name in required -%}
    {%- if name | lower not in types and name != fact.version_column -%}
      {%- do missing.append(name) -%}
    {%- endif -%}
  {%- endfor -%}
  {%- if missing -%}
    {% do exceptions.raise_compiler_error(
      "dimension_backfill fact table " ~ fact_relation ~ " is missing the columns "
      ~ missing | join(', ') ~ " needed to link rows to a dimension version.") %}
  {%- endif -%}

  {%- set version_type = types.get(fact.version_column | lower) -%}
  {%- if version_type is not none and not version_type.lower().startswith('varchar') -%}
    {% do exceptions.raise_compiler_error(
      "dimension_backfill fact table " ~ fact_relation ~ " already has a version column "
      ~ adapter.quote(fact.version_column) ~ " of type " ~ version_type ~ "; it must be a "
      ~ "text column so it can hold the version identifier.") %}
  {%- endif -%}
  {{ return(types) }}
{%- endmacro %}


{% macro duckdb__dimension_backfill_relink_fact_sql(fact, settings, history_relation, fact_relation) %}
  {#-- Re-link the fact's version column to the history row covering its business key
       and occurrence time. Only that column is written: a fact exactly on a change
       point belongs to the new version (the intervals are half-open), a fact that no
       version covers becomes NULL. --#}
  {%- set history_keys = duckdb__dimension_backfill_quote_columns(settings.unique_key) -%}
  {%- set fact_keys = duckdb__dimension_backfill_quote_columns(fact.unique_key) -%}
  {%- set matches = namespace(parts=[]) -%}
  {%- for index in range(history_keys | length) -%}
    {%- do matches.parts.append(
      'dbt_version.' ~ history_keys[index] ~ ' is not distinct from dbt_fact.'
      ~ fact_keys[index]) -%}
  {%- endfor -%}
  {%- set occurred_at = adapter.quote(fact.occurred_at) -%}
  {%- set version_column = adapter.quote(fact.version_column) -%}
  {%- set start_column = adapter.quote(settings.history_start_column) -%}
  {%- set end_column = adapter.quote(settings.history_end_column) -%}
  update {{ fact_relation }} as dbt_fact
  set {{ version_column }} = (
    select dbt_version.{{ adapter.quote(settings.version_column) }}
    from {{ history_relation }} as dbt_version
    where {{ matches.parts | join(' and ') }}
      and dbt_fact.{{ occurred_at }} >= dbt_version.{{ start_column }}
      and (dbt_version.{{ end_column }} is null
           or dbt_fact.{{ occurred_at }} < dbt_version.{{ end_column }})
  )
{%- endmacro %}


{% macro duckdb__dimension_backfill_relink_fact(fact, settings, history_relation) %}
  {%- set fact_relation = duckdb__dimension_backfill_fact_relation(fact.relation, this) -%}
  {%- do duckdb__dimension_backfill_check_fact(fact, fact_relation, settings) -%}
  {% call statement('add_version_column_' ~ fact_relation.identifier) -%}
    alter table {{ fact_relation }}
    add column if not exists {{ adapter.quote(fact.version_column) }} varchar
  {%- endcall %}
  {% call statement('relink_' ~ fact_relation.identifier) -%}
    {{ duckdb__dimension_backfill_relink_fact_sql(fact, settings, history_relation, fact_relation) }}
  {%- endcall %}
{%- endmacro %}


{% materialization dimension_backfill, adapter="duckdb", supported_languages=['sql'] %}

  {%- if adapter.is_motherduck() -%}
    {% do exceptions.raise_compiler_error(
      "The dimension_backfill materialization keeps an observation store, the dimension "
      ~ "history and the declared fact tables consistent in a single transaction, which "
      ~ "a MotherDuck connection cannot guarantee. Use a local DuckDB database.") %}
  {%- endif -%}

  {%- set settings = duckdb__dimension_backfill_config() -%}
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set intermediate_relation = make_intermediate_relation(target_relation) -%}
  -- the intermediate/backup relations should not already exist in the database; if
  -- they do, we get a relation that we can drop, before BEGIN, and reuse the name
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
  {%- set observation_store = target_relation.incorporate(
        path={'identifier': target_relation.identifier ~ '__dbt_observations'}, type='table') -%}
  {%- set build_in_place = adapter.is_ducklake(target_relation)
        or has_foreign_key_constraints(existing_relation) -%}
  {%- set full_refresh_mode = should_full_refresh() -%}
  {%- set dropped_dependents = [] -%}
  {%- set intermediate_is_cleanup = false -%}
  {% set grant_config = config.get('grants') %}

  -- drop the temp relations if they exist already in the database
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {#-- 1. The batch of observations this run brings in. --#}
  {%- set batch_relation = make_temp_relation(target_relation) -%}
  {% call statement('create_observation_batch') -%}
    create temporary table {{ batch_relation }} as (
      {{ duckdb__dimension_backfill_observations_sql(compiled_code) }}
    )
  {%- endcall %}
  {%- do duckdb__dimension_backfill_validate(batch_relation, settings) -%}

  {#-- 2. Upsert the batch into the observation store, keeping every observation
       ever accepted. The store holds exactly the configured columns, so columns a
       model happens to select for its own purposes are not persisted. --#}
  {%- set store_exists = duckdb__dimension_backfill_relation_exists(observation_store) -%}
  {%- if full_refresh_mode and store_exists -%}
    {% do adapter.drop_relation(observation_store) %}
    {%- set store_exists = false -%}
  {%- endif -%}
  {%- set store_key_columns = duckdb__dimension_backfill_quote_columns(
        settings.unique_key + [settings.updated_at] + settings.attributes) -%}
  {%- if not store_exists -%}
    {% call statement('create_observation_store') -%}
      create table {{ observation_store }} as (
        select distinct {{ store_key_columns | join(', ') }} from {{ batch_relation }}
      )
    {%- endcall %}
  {%- else -%}
    {#-- A newly configured attribute needs a column in the existing store. Only
         configured columns are added, never everything the model selects. --#}
    {%- set store_column_names = [] -%}
    {%- for column in adapter.get_columns_in_relation(observation_store) -%}
      {%- do store_column_names.append(column.name | lower) -%}
    {%- endfor -%}
    {%- set configured_names = [] -%}
    {%- for name in settings.unique_key + [settings.updated_at] + settings.attributes -%}
      {%- do configured_names.append(name | lower) -%}
    {%- endfor -%}
    {%- for column in adapter.get_columns_in_relation(batch_relation) -%}
      {%- if column.name | lower in configured_names
            and column.name | lower not in store_column_names -%}
        {% call statement('add_observation_column_' ~ loop.index) -%}
          alter table {{ observation_store }}
          add column if not exists {{ adapter.quote(column.name) }} {{ column.dtype }}
        {%- endcall %}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}
  {% call statement('upsert_observations') -%}
    {{ duckdb__dimension_backfill_upsert_sql(observation_store, batch_relation, settings) }}
  {%- endcall %}

  {#-- 3. Rebuild the SCD2 history from the store, reusing the previous version
       identifiers for start points that did not move. --#}
  {%- set prior_relation = none -%}
  {%- if existing_relation is not none and existing_relation.is_table -%}
    {%- set existing_columns = [] -%}
    {%- for column in adapter.get_columns_in_relation(existing_relation) -%}
      {%- do existing_columns.append(column.name | lower) -%}
    {%- endfor -%}
    {%- if settings.version_column | lower in existing_columns
          and settings.history_start_column | lower in existing_columns -%}
      {%- set prior_relation = existing_relation -%}
    {%- endif -%}
  {%- endif -%}

  {%- set history_sql = duckdb__dimension_backfill_history_sql(
        settings, observation_store, prior_relation) -%}
  {%- if config.get('contract') and config.get('contract').enforced -%}
    {#-- The contract describes the history this model produces (business key,
         attributes, the interval and the version id), not the observations. --#}
    {% do get_assert_columns_equivalent(history_sql) %}
  {%- endif -%}

  {% if existing_relation is none %}
    {#-- Nothing to swap or replace: build the history under its final name. --#}
    {% call statement('main') -%}
      create table {{ target_relation }} as ({{ history_sql }})
    {%- endcall %}
  {% else %}
    {% call statement('main') -%}
      create table {{ intermediate_relation }} as ({{ history_sql }})
    {%- endcall %}

    {% if build_in_place %}
      {#-- Tables with foreign keys must not be renamed (see has_foreign_key_constraints),
           and DuckLake tables must be created under their final name so their physical
           paths do not retain intermediate suffixes. A view can be replaced in place
           only after it is dropped. --#}
      {% if existing_relation.is_view
            or has_foreign_key_constraints(existing_relation) %}
        {% set dropped_dependents = drop_relation_for_rebuild(existing_relation) %}
        {% call statement('copy_history_to_target') -%}
          create table {{ target_relation }} as (select * from {{ intermediate_relation }})
        {%- endcall %}
      {% else %}
        {% call statement('copy_history_to_target') -%}
          create or replace table {{ target_relation }} as (
            select * from {{ intermediate_relation }}
          )
        {%- endcall %}
      {% endif %}
      {% set intermediate_is_cleanup = true %}
      {% do restore_foreign_key_dependents(dropped_dependents) %}
    {% else %}
      {#-- Drop indexes and foreign key dependents on the existing relation before
           renaming it out of the way, to avoid dependency errors. --#}
      {% do drop_indexes_on_relation(existing_relation) %}
      {% if existing_relation.is_table and may_be_foreign_key_target(existing_relation) %}
        {% set dropped_dependents = drop_foreign_key_dependents(existing_relation) %}
      {% endif %}
      {{ adapter.rename_relation(existing_relation, backup_relation) }}
      {{ adapter.rename_relation(intermediate_relation, target_relation) }}
      {% do restore_foreign_key_dependents(dropped_dependents) %}
    {% endif %}
  {% endif %}

  {% do create_indexes(target_relation) %}

  {#-- 4. Re-link the declared fact tables to the rebuilt versions. --#}
  {% for fact in settings.fact_tables %}
    {% do duckdb__dimension_backfill_relink_fact(fact, settings, target_relation) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}
  {% do persist_docs(target_relation, model) %}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  -- finally, drop the existing/backup relation after the commit
  {{ drop_relation_if_exists(backup_relation) }}
  {% if intermediate_is_cleanup
        and duckdb__dimension_backfill_relation_exists(intermediate_relation) %}
    {% do adapter.drop_relation(intermediate_relation) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
