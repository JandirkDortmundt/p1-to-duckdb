{% macro sqlite_source(table_name) %}
    (
        -- Creates a read-only connection to the SQLite database
        -- Safe to use while P1 monitor app is writing to the database
        SELECT * FROM sqlite_scan('/home/jd/p1monitor/alldata/mnt/ramdisk/e_historie.db', '{{ table_name }}')
    )
{% endmacro %}
