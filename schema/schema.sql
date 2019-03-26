\c postgres
drop database queue;
create database queue;
\c queue

begin;

create table queue_pending (
    id      bigserial primary key,
    tx_id   text not null,
    payload jsonb not null,
    constraint tx_id_pengind_unique unique(tx_id)
);

create table queue_failed (
    id      bigserial primary key,
    tx_id   text not null,
    payload jsonb not null,
    reason  text,
    constraint tx_id_failed_unique unique(tx_id)
);

create table queue_succeeded (
    id      bigserial primary key,
    tx_id   text not null,
    payload jsonb not null,
    reason  text,
    constraint tx_id_succeeded_unique unique(tx_id)
);

/*
    Lock function
*/
create or replace function queue_obtain_lock_tx_id(tx_id text) returns void as $$
begin
    perform pg_advisory_xact_lock( ('x'||substr(md5($1),1,16))::bit(64)::bigint);
end
$$ language 'plpgsql';

create or replace function queue_try_obtain_lock_tx_id(tx_id text) returns void as $$
begin
    perform pg_try_advisory_xact_lock( ('x'||substr(md5($1),1,16))::bit(64)::bigint);
end
$$ language 'plpgsql';

/*
    Insert new task triggers
*/
create or replace function queue_insert_pending_check_constrain() returns trigger as $$
begin
    perform queue_obtain_lock_tx_id(NEW.tx_id);
    if exists (select 1 from queue_failed where tx_id = NEW.tx_id) then
        raise 'transaction already exists: %', NEW.tx_id using errcode = 'unique_violation';
    end if;
    if exists (select 1 from queue_succeeded where tx_id = NEW.tx_id) then
        raise 'transaction already exists: %', NEW.tx_id using errcode = 'unique_violation';
    end if;
    return new;
end;
$$ language 'plpgsql';
create trigger queue_pending_insert_trg before insert on queue_pending
    for each row execute procedure queue_insert_pending_check_constrain();

commit;
