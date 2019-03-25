-- partition of queue_state_faild and queue_state_succeeded if needed
create or replace function create_parititons_for_queue(year int) returns void AS $$
declare
    month_name text;
    year_date date;
    interval_begin interval;
    interval_end interval;
    begin_at timestamp with time zone;
    end_at timestamp with time zone;
    main_table_name text;
    partition_table_name text;
begin

    year_date := ( select to_date( year ||'-1-1', 'YYYY-MM-DD') );

    foreach main_table_name in array '{"queue_state_failed", "queue_state_succeeded"}'::text[] loop
        for month_counter in 1..12 loop
            month_name := (select lpad(month_counter::text, 2, '0') );
            partition_table_name := main_table_name || '_' || year || '_' || month_name;
            interval_begin := ( select (month_counter - 1 || ' month')::interval  );
            interval_end := ( select (month_counter || ' month')::interval - interval '1 millisecond');
            begin_at := (select date_trunc('year', year_date) + interval_begin) at time zone 'UTC';
            end_at   := (select date_trunc('year', year_date) + interval_end) at time zone 'UTC';
            execute 'create table if not exists ' || partition_table_name || ' partition of '      || main_table_name || ' for values from ('||  quote_literal(begin_at) ||') to (' || quote_literal(end_at) || ')';
            execute 'create index if not exists ' || partition_table_name || '_created_at_idx on ' || partition_table_name || '(created_at)';
            execute 'create index if not exists ' || partition_table_name || '_remote_id_idx on '  || partition_table_name || '(remote_id)';
            execute 'create index if not exists ' || partition_table_name || '_id_idx on ' || partition_table_name || '(id)';
            if not exists (select constraint_name from information_schema.table_constraints where table_name = partition_table_name and constraint_type = 'PRIMARY KEY') then
                execute 'alter table ' || partition_table_name || ' add primary key (id)';
            end if;
        end loop;
    end loop;

end
$$ language 'plpgsql';
