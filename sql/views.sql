-- retrieve replication info
CREATE OR REPLACE VIEW replication_info AS
 SELECT 
    r.application_name,
    s.slot_name,
    s.slot_type,
    s.active,
    r.state,
    r.sync_priority,
    r.sync_state,
    pg_xlog_location_diff(r.sent_location,  r.replay_location) AS sent_bytes_lag,
    pg_xlog_location_diff(r.write_location, r.replay_location) AS write_bytes_lag,
    pg_xlog_location_diff(pg_current_xlog_location(), r.flush_location) AS flush_bytes_lag,
    pg_xlog_location_diff(pg_current_xlog_location(), r.replay_location) AS replay_bytes_lag,
    pg_xlog_location_diff(pg_current_xlog_location(), s.restart_lsn) AS slot_lsn_bytes_lag
   FROM pg_stat_replication r
   RIGHT JOIN pg_replication_slots s ON (r.pid = s.active_pid) 
;

