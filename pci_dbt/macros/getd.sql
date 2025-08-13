{% set source_table = source('dbinterface', 'gc_CONVERSATIONS_SEGMENT_FACT') %}
{% set column = source_table.columns | selectattr('name', 'equalto', 'CONVERSATION_ID') | first %}
{{ column.description }}