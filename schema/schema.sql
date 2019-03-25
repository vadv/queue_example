\c postgres
drop database queue;
create database queue;
\c queue

begin;

create type queue_state as enum ('pending', 'succeeded', 'failed');

drop table if exists queue cascade;
create table queue (
    id             bigserial,
    transaction_id text not null,
    state          queue_state not null default 'pending',
    payload        jsonb not null,
    reason         text

) partition by list (state);

create table queue_state_pending partition of queue
    for values in ('pending');
alter table queue_state_pending add primary key(id);
create unique index queue_state_pending_uniq_transaction_id on queue_state_pending (transaction_id);

create table queue_state_failed partition of queue
    for values in ('failed');
alter table queue_state_failed add primary key(id);
create unique index queue_state_failed_uniq_transaction_id on queue_state_failed (transaction_id);

create table queue_state_succeeded partition of queue
    for values in ('succeeded');
alter table queue_state_succeeded add primary key(id);
create unique index queue_state_succeeded_uniq_transaction_id on queue_state_succeeded (transaction_id);

/*
    Lock function
*/
create or replace function queue_lock_transaction_id(transaction_id text) returns void as $$
begin
    perform pg_advisory_xact_lock( ('x'||substr(md5($1),1,16))::bit(64)::bigint);
end
$$ language 'plpgsql';

/*
    Insert new task triggers
*/
create or replace function queue_pending_insert_check() returns trigger as $$
begin
    perform queue_lock_transaction_id(NEW.transaction_id);
    if exists (select 1 from queue where transaction_id = NEW.transaction_id) then
        raise 'transaction already exists: %', NEW.transaction_id using errcode = 'unique_violation';
    end if;
    return new;
end;
$$ language 'plpgsql';
create trigger queue_pending_insert_trg before insert on queue_state_pending
    for each row execute procedure queue_pending_insert_check();

/*
    Update task status
*/
create or replace function queue_update_check() returns trigger as $$
begin
    perform queue_lock_transaction_id(NEW.transaction_id);
    if NEW.transaction_id <> OLD.transaction_id then
        raise 'forbidden to change transaction_id: %', OLD.transaction_id using errcode = 'unique_violation';
    end if;
    if NEW.state <> OLD.state then
        /* small FSM: allow to change state only from 'pending' to another */
        if OLD.state <> 'pending' then
            raise 'forbidden to change state of transaction_id: % from % to %', NEW.transaction_id, OLD.state, NEW.state using errcode = 'unique_violation';
        end if;
    end if;
    return new;
end;
$$ language 'plpgsql';
create trigger queue_update_trg before update on queue_state_pending
    for each row execute procedure queue_update_check();
create trigger queue_update_trg before update on queue_state_failed
    for each row execute procedure queue_update_check();
create trigger queue_update_trg before update on queue_state_succeeded
    for each row execute procedure queue_update_check();

commit;
