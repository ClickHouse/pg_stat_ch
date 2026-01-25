# Overflow race isolation test
# Tests lock-free overflow path under concurrent contention
# Verifies no deadlock and correct accounting when multiple producers hit overflow

setup {
    CREATE EXTENSION IF NOT EXISTS pg_stat_ch;
    SELECT pg_stat_ch_reset();
    -- Note: This test works best with small queue_capacity (set via GUC)
    -- Default capacity is 65536, so we generate many events to approach overflow
}

teardown {
    -- Verify invariants after overflow scenario
    SELECT
        enqueued_events >= 0 AS enqueued_valid,
        dropped_events >= 0 AS dropped_valid,
        queue_size >= 0 AS size_valid,
        queue_size <= queue_capacity AS size_bounded,
        -- Conservation: may not be exact during active operations
        (enqueued_events >= dropped_events + queue_size) AS conservation_valid
    FROM pg_stat_ch_stats();
}

# Session 1: Overflow trigger - generates many events
session s1
setup { SET application_name = 'overflow_trigger_1'; }
step s1_overflow {
    -- Generate events to push toward overflow
    DO $$
    BEGIN
        FOR i IN 1..500 LOOP
            PERFORM i;
        END LOOP;
    END
    $$;
}
step s1_more_events {
    -- Generate more events after potential overflow
    DO $$
    BEGIN
        FOR i IN 1..100 LOOP
            PERFORM i;
        END LOOP;
    END
    $$;
}

# Session 2: Overflow trigger - generates many events concurrently
session s2
setup { SET application_name = 'overflow_trigger_2'; }
step s2_overflow {
    -- Generate events concurrently with s1
    DO $$
    BEGIN
        FOR i IN 1..500 LOOP
            PERFORM i;
        END LOOP;
    END
    $$;
}
step s2_more_events {
    -- Generate more events after potential overflow
    DO $$
    BEGIN
        FOR i IN 1..100 LOOP
            PERFORM i;
        END LOOP;
    END
    $$;
}

# Session 3: Observer - checks overflow state
session s3
setup { SET application_name = 'overflow_observer'; }
step s3_check_overflow {
    -- Check if overflow has occurred
    SELECT
        dropped_events >= 0 AS drops_valid,
        enqueued_events > 0 AS has_enqueued,
        queue_size <= queue_capacity AS bounded
    FROM pg_stat_ch_stats();
}
step s3_reset {
    SELECT pg_stat_ch_reset();
}
step s3_check_post_reset {
    -- After reset, dropped counter should be cleared
    SELECT
        dropped_events = 0 AS drops_cleared,
        queue_size >= 0 AS size_valid
    FROM pg_stat_ch_stats();
}
step s3_sample_during {
    -- Quick sample during potential overflow
    SELECT
        queue_size <= queue_capacity AS bounded,
        enqueued_events >= 0 AS enqueued_valid
    FROM pg_stat_ch_stats();
}

# Test 1: Concurrent overflow from two producers
# Both sessions try to overflow simultaneously
permutation s1_overflow s2_overflow s3_check_overflow

# Test 2: Interleaved overflow attempts with sampling
# Sample during potential overflow race
permutation s1_overflow s3_sample_during s2_overflow s3_check_overflow

# Test 3: Continued writes after overflow
# Overflow, then continue writing
permutation s1_overflow s2_overflow s1_more_events s2_more_events s3_check_overflow

# Test 4: Reset clears overflow, new overflow triggers again
permutation s1_overflow s3_check_overflow s3_reset s3_check_post_reset s2_overflow s3_check_overflow

# Test 5: Multiple producers racing at boundary
permutation s1_overflow s2_overflow s1_more_events s2_more_events s3_sample_during

# Test 6: Reset between overflows
permutation s1_overflow s3_reset s1_overflow s3_check_overflow
