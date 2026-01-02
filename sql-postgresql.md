```sql
select
    n.nspname as schema_name,
    c.relname as table_name,
    coalesce(s.n_live_tup, 0) as row_count
from pg_class c
         join pg_namespace n on n.oid = c.relnamespace
         left join pg_stat_user_tables s on s.relid = c.oid
where c.relkind = 'r'
  and n.nspname not in ('pg_catalog', 'information_schema')
order by row_count desc;
```
