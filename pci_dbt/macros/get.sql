{% macro sqlserver__alter_column_comment(relation, column_dict) -%}
    {%- set existing_columns = adapter.get_columns_in_relation(relation) | map(attribute="name") | list %}
    {%- for column_name in column_dict if (column_name in existing_columns) %}
        {%- do log('Alter extended property "MS_Description" to "' ~ column_dict[column_name]['description'] ~ '" for ' ~ relation ~ ' column "' ~ column_name ~ '"', info=false) %}
        IF NOT EXISTS (
            SELECT NULL 
            FROM SYS.EXTENDED_PROPERTIES AS ep

                INNER JOIN SYS.ALL_COLUMNS AS cols
                    ON cols.object_id = ep.major_id
                    AND cols.column_id = ep.minor_id

            WHERE   ep.major_id = OBJECT_ID('{{ relation }}') 
            AND     ep.name = N'MS_Description'
            AND		cols.name = N'{{ column_name }}'
        )
            EXECUTE sp_addextendedproperty      @name = N'MS_Description', @value = N'{{ column_dict[column_name]['description'] }}'
                                                , @level0type = N'SCHEMA', @level0name = N'{{ relation.schema }}'
                                                , @level1type = N'{{ relation.type }}', @level1name = N'{{ relation.identifier }}'
                                                , @level2type = N'COLUMN', @level2name = N'{{ column_name }}';
        ELSE
            EXECUTE sp_updateextendedproperty   @name = N'MS_Description', @value = N'{{ column_dict[column_name]['description'] }}'
                                                , @level0type = N'SCHEMA', @level0name = N'{{ relation.schema }}'
                                                , @level1type = N'{{ relation.type }}', @level1name = N'{{ relation.identifier }}'
                                                , @level2type = N'COLUMN', @level2name = N'{{ column_name }}';
    {%- endfor %}
{%- endmacro %}

{% macro sqlserver__alter_relation_comment(relation, relation_comment) -%}
   {% do log('Alter extended property "MS_Description" to "' ~ relation_comment ~ '" for ' ~ relation, info=false) %}

    IF NOT EXISTS (
        SELECT NULL 
        FROM SYS.EXTENDED_PROPERTIES AS ep
        WHERE   ep.major_id = OBJECT_ID('{{ relation }}')
        AND     ep.name = N'MS_Description'
        AND     ep.minor_id = 0
    )
        EXECUTE sp_addextendedproperty      @name = N'MS_Description', @value = N'{{ relation_comment }}'
                                            , @level0type = N'SCHEMA', @level0name = N'{{ relation.schema }}'
                                            , @level1type = N'{{ relation.type }}', @level1name = N'{{ relation.identifier }}';
    ELSE
        EXECUTE sp_updateextendedproperty   @name = N'MS_Description', @value = N'{{ relation_comment }}'
                                            , @level0type = N'SCHEMA', @level0name = N'{{ relation.schema }}'
                                            , @level1type = N'{{ relation.type }}', @level1name = N'{{ relation.identifier }}';
{% endmacro %}