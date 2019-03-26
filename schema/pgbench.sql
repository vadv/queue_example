\set txid random(1, 100000 * :scale)
\set txid2 random(1, 100000 * :scale)

BEGIN;

insert into queue_pending
    (
            tx_id,
            payload
    )
values (
        md5(:txid::text || :txid2::text),
        ('{"txid":"' || :txid::text || '", "txid2":"' || :txid2::text || '"}')::jsonb
    );

END;
