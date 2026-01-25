# Ring buffer concurrent access isolation test
# Tests that concurrent writes don't corrupt the buffer and stats reads don't block

setup {
    CREATE EXTENSION IF NOT EXISTS pg_stat_ch;
    SELECT pg_stat_ch_reset();
}

teardown {
    -- Show invariants for debugging (booleans only for determinism)
    SELECT
        enqueued_events >= 0 AS enqueued_valid,
        dropped_events >= 0 AS dropped_valid,
        queue_size >= 0 AS size_valid,
        queue_capacity > 0 AS capacity_valid
    FROM pg_stat_ch_stats();
}

# Session 1: Writer session
session s1
setup { SET application_name = 'writer1'; }
step s1_write { SELECT count(*) FROM generate_series(1, 50); }
step s1_check { SELECT enqueued_events > 0 AS has_events FROM pg_stat_ch_stats(); }

# Session 2: Writer session
session s2
setup { SET application_name = 'writer2'; }
step s2_write { SELECT count(*) FROM generate_series(1, 50); }
step s2_check { SELECT enqueued_events > 0 AS has_events FROM pg_stat_ch_stats(); }

# Session 3: Stats reader session
session s3
setup { SET application_name = 'reader'; }
step s3_stats { SELECT queue_size >= 0 AS valid_size FROM pg_stat_ch_stats(); }
step s3_reset { SELECT pg_stat_ch_reset(); }
step s3_invariants {
    SELECT
        dropped_events = 0 AS no_drops,
        queue_capacity = 65536 AS default_capacity
    FROM pg_stat_ch_stats();
}

# Test 1: Concurrent writes from two sessions don't corrupt
permutation s1_write s2_write s1_check s2_check

# Test 2: Stats read interleaved with writes (reader shouldn't block)
permutation s1_write s3_stats s2_write s3_stats s1_check

# Test 3: Reset during writes (should work without crash)
permutation s1_write s3_reset s2_write s1_check s3_invariants

# Test 4: Interleaved writes and stats checks
permutation s1_write s3_invariants s2_write s3_invariants

# Test 5: Multiple resets are idempotent
permutation s3_reset s3_reset s1_write s3_invariants
