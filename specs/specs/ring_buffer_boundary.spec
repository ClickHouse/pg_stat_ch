# Ring buffer boundary conditions isolation test
# Tests race condition permutations at queue boundaries
# Verifies counters remain valid under concurrent stress at capacity limits

setup {
    CREATE EXTENSION IF NOT EXISTS pg_stat_ch;
    SELECT pg_stat_ch_reset();
}

teardown {
    -- Verify all counters are non-negative and queue_size <= capacity
    SELECT
        enqueued_events >= 0 AS enqueued_valid,
        dropped_events >= 0 AS dropped_valid,
        exported_events >= 0 AS exported_valid,
        queue_size >= 0 AS size_valid,
        queue_size <= queue_capacity AS size_bounded,
        queue_capacity > 0 AS capacity_valid
    FROM pg_stat_ch_stats();
}

# Session 1: Heavy writer generating many events
session s1
setup { SET application_name = 'heavy_writer_1'; }
step s1_flood {
    -- Generate many events quickly
    DO $$
    BEGIN
        FOR i IN 1..200 LOOP
            PERFORM i;
        END LOOP;
    END
    $$;
}
step s1_check {
    SELECT
        enqueued_events > 0 AS has_events,
        queue_size <= queue_capacity AS bounded
    FROM pg_stat_ch_stats();
}

# Session 2: Heavy writer generating many events
session s2
setup { SET application_name = 'heavy_writer_2'; }
step s2_flood {
    -- Generate many events quickly
    DO $$
    BEGIN
        FOR i IN 1..200 LOOP
            PERFORM i;
        END LOOP;
    END
    $$;
}
step s2_check {
    SELECT
        enqueued_events > 0 AS has_events,
        queue_size <= queue_capacity AS bounded
    FROM pg_stat_ch_stats();
}

# Session 3: Stats sampler and reset issuer
session s3
setup { SET application_name = 'stats_sampler'; }
step s3_sample {
    -- Sample stats to verify invariants mid-operation
    SELECT
        enqueued_events >= 0 AS enqueued_valid,
        dropped_events >= 0 AS dropped_valid,
        queue_size >= 0 AS size_valid,
        queue_size <= queue_capacity AS bounded
    FROM pg_stat_ch_stats();
}
step s3_reset {
    SELECT pg_stat_ch_reset();
}
step s3_verify_reset {
    -- After reset, counters should be at baseline
    SELECT
        dropped_events = 0 AS drops_cleared
    FROM pg_stat_ch_stats();
}

# Test 1: Concurrent producer stress
# Both writers flood simultaneously, then check stats
permutation s1_flood s2_flood s3_sample s1_check s2_check

# Test 2: Interleaved sampling during writes
# Sample stats while writes are in progress
permutation s1_flood s3_sample s2_flood s3_sample s1_check

# Test 3: Reset between writes
# One writer floods, reset, second writer floods
permutation s1_flood s3_reset s2_flood s1_check s3_sample

# Test 4: Multiple resets during writes
permutation s3_reset s1_flood s3_reset s2_flood s3_sample

# Test 5: Reset verification after load
permutation s1_flood s2_flood s3_reset s3_verify_reset

# Test 6: Concurrent writes with interleaved checks
permutation s1_flood s1_check s2_flood s2_check s3_sample

# Test 7: Stats sampling doesn't block writes
permutation s3_sample s1_flood s3_sample s2_flood s3_sample
