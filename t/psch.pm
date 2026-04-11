# Helper module for pg_stat_ch TAP tests
package psch;

use strict;
use warnings;
use Exporter 'import';
use PostgreSQL::Test::Cluster;
use Time::HiRes qw(sleep time);

our @EXPORT = qw(
    psch_init_node
    psch_get_stats
    psch_reset_stats
    psch_clickhouse_available
    psch_start_clickhouse
    psch_stop_clickhouse
    psch_query_clickhouse
    psch_query_clickhouse_tsv
    psch_init_node_with_clickhouse
    psch_wait_for_export
    psch_wait_for_clickhouse_query
    psch_otelcol_available
    psch_start_otelcol
    psch_stop_otelcol
    psch_init_node_with_otel
    psch_get_otel_histogram_total
    psch_otel_metric_has_label
);

# Initialize a PostgreSQL node with pg_stat_ch loaded
sub psch_init_node {
    my ($name, %opts) = @_;

    my $queue_capacity = $opts{queue_capacity} // 65536;
    my $flush_interval_ms = $opts{flush_interval_ms} // 1000;
    my $enabled = $opts{enabled} // 'on';

    my $node = PostgreSQL::Test::Cluster->new($name);
    $node->init();
    $node->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'pg_stat_ch'
pg_stat_ch.enabled = $enabled
pg_stat_ch.queue_capacity = $queue_capacity
pg_stat_ch.flush_interval_ms = $flush_interval_ms
});
    $node->start();
    $node->safe_psql('postgres', 'CREATE EXTENSION pg_stat_ch');

    return $node;
}

# Get stats as a hash
sub psch_get_stats {
    my ($node) = @_;

    my $result = $node->safe_psql('postgres',
        'SELECT enqueued_events, dropped_events, exported_events, ' .
        'send_failures, queue_size, queue_capacity, queue_usage_pct, ' .
        'dsa_oom_count ' .
        'FROM pg_stat_ch_stats()');

    my @values = split /\|/, $result;
    return {
        enqueued      => $values[0] // 0,
        dropped       => $values[1] // 0,
        exported      => $values[2] // 0,
        send_failures => $values[3] // 0,
        queue_size    => $values[4] // 0,
        capacity      => $values[5] // 0,
        usage_pct     => $values[6] // 0,
        dsa_oom       => $values[7] // 0,
    };
}

# Reset stats
sub psch_reset_stats {
    my ($node) = @_;
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_reset()');
}

# ============================================================================
# ClickHouse Integration Helpers
# ============================================================================

# Check if Docker is available for ClickHouse tests
sub psch_clickhouse_available {
    return system("docker ps >/dev/null 2>&1") == 0;
}

# Start ClickHouse container using docker compose
sub psch_start_clickhouse {
    my $project_dir = $ENV{PROJECT_DIR} // '.';
    my $compose_file = "$project_dir/docker/docker-compose.test.yml";

    # Start container
    system("docker compose -f $compose_file up -d --wait") == 0
        or die "Failed to start ClickHouse container";

    # Wait for healthcheck (up to 30 seconds)
    for my $i (1..30) {
        my $result = system("docker exec psch-clickhouse clickhouse-client -q 'SELECT 1' >/dev/null 2>&1");
        return 1 if $result == 0;
        sleep(1);
    }
    die "ClickHouse container failed to become healthy";
}

# Stop ClickHouse container
sub psch_stop_clickhouse {
    my $project_dir = $ENV{PROJECT_DIR} // '.';
    my $compose_file = "$project_dir/docker/docker-compose.test.yml";

    system("docker compose -f $compose_file down -v");
}

# Query ClickHouse and return raw output
sub psch_query_clickhouse {
    my ($query) = @_;
    # Use --data-binary to send raw query; escape single quotes for shell
    $query =~ s/'/'\\''/g;
    my $result = `curl -s 'http://localhost:18123/' --data-binary '$query'`;
    chomp($result);
    return $result;
}

# Query ClickHouse and return result as TSV rows
sub psch_query_clickhouse_tsv {
    my ($query) = @_;
    my $result = psch_query_clickhouse("$query FORMAT TSVWithNames");
    my @lines = split(/\n/, $result);
    return @lines;
}

# Initialize a node with ClickHouse export enabled
sub psch_init_node_with_clickhouse {
    my ($name, %opts) = @_;

    my $queue_capacity = $opts{queue_capacity} // 65536;
    my $flush_interval_ms = $opts{flush_interval_ms} // 100;  # Fast flush for tests
    my $batch_max = $opts{batch_max} // 1000;
    my $enabled = $opts{enabled} // 'on';
    my $clickhouse_host = $opts{clickhouse_host} // 'localhost';
    my $clickhouse_port = $opts{clickhouse_port} // 19000;
    my $clickhouse_database = $opts{clickhouse_database} // 'pg_stat_ch';

    my $node = PostgreSQL::Test::Cluster->new($name);
    $node->init();
    $node->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'pg_stat_ch'
pg_stat_ch.enabled = $enabled
pg_stat_ch.queue_capacity = $queue_capacity
pg_stat_ch.flush_interval_ms = $flush_interval_ms
pg_stat_ch.batch_max = $batch_max
pg_stat_ch.clickhouse_host = '$clickhouse_host'
pg_stat_ch.clickhouse_port = $clickhouse_port
pg_stat_ch.clickhouse_database = '$clickhouse_database'
});
    $node->start();
    $node->safe_psql('postgres', 'CREATE EXTENSION pg_stat_ch');

    return $node;
}

# Wait for events to be exported to ClickHouse
# Returns the final exported count, or dies on timeout
sub psch_wait_for_export {
    my ($node, $min_expected, $timeout_secs) = @_;
    $timeout_secs //= 10;

    my $start_time = time();
    while (time() - $start_time < $timeout_secs) {
        my $stats = psch_get_stats($node);
        return $stats->{exported} if $stats->{exported} >= $min_expected;
        sleep(0.5);
    }

    # Timeout - return current count anyway for diagnostic
    my $stats = psch_get_stats($node);
    return $stats->{exported};
}

# Poll a ClickHouse query until the predicate accepts the raw result.
# Returns the latest result on success or timeout.
sub psch_wait_for_clickhouse_query {
    my ($query, $predicate, $timeout_secs, $poll_interval_secs) = @_;
    $timeout_secs //= 10;
    $poll_interval_secs //= 0.1;

    die "psch_wait_for_clickhouse_query requires a CODE predicate"
        unless ref($predicate) eq 'CODE';

    my $result = '';
    my $start_time = time();
    while (time() - $start_time < $timeout_secs) {
        $result = psch_query_clickhouse($query);
        return $result if $predicate->($result);
        sleep($poll_interval_secs);
    }

    return $result;
}

# ============================================================================
# OpenTelemetry Collector Integration Helpers
# ============================================================================

# Check if Docker is available and the OTel collector health endpoint responds
sub psch_otelcol_available {
    return 0 unless system("docker ps >/dev/null 2>&1") == 0;
    my $result = `curl -sf 'http://localhost:13133/' 2>/dev/null`;
    return $result =~ /Server available/;
}

# Start OTel collector container using docker compose
sub psch_start_otelcol {
    my $project_dir = $ENV{PROJECT_DIR} // '.';
    my $compose_file = "$project_dir/docker/docker-compose.otel.yml";

    system("docker compose -f $compose_file up -d") == 0
        or die "Failed to start OTel collector container";

    # Poll health check endpoint (up to 30 seconds)
    for my $i (1..30) {
        my $result = `curl -sf 'http://localhost:13133/' 2>/dev/null`;
        return 1 if $result =~ /Server available/;
        sleep(1);
    }
    die "OTel collector container failed to become healthy";
}

# Stop OTel collector container
sub psch_stop_otelcol {
    my $project_dir = $ENV{PROJECT_DIR} // '.';
    my $compose_file = "$project_dir/docker/docker-compose.otel.yml";

    system("docker compose -f $compose_file down -v");
}

# Initialize a node with OTel export enabled
sub psch_init_node_with_otel {
    my ($name, %opts) = @_;

    my $queue_capacity    = $opts{queue_capacity}    // 65536;
    my $flush_interval_ms = $opts{flush_interval_ms} // 100;   # Fast flush for tests
    my $batch_max         = $opts{batch_max}         // 1000;
    my $enabled           = $opts{enabled}           // 'on';
    my $otel_endpoint     = $opts{otel_endpoint}     // 'localhost:4317';
    my $hostname          = $opts{hostname}          // 'test-host';

    my $node = PostgreSQL::Test::Cluster->new($name);
    $node->init();
    $node->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'pg_stat_ch'
pg_stat_ch.enabled = $enabled
pg_stat_ch.queue_capacity = $queue_capacity
pg_stat_ch.flush_interval_ms = $flush_interval_ms
pg_stat_ch.batch_max = $batch_max
pg_stat_ch.use_otel = on
pg_stat_ch.otel_endpoint = '$otel_endpoint'
pg_stat_ch.hostname = '$hostname'
});
    $node->start();
    $node->safe_psql('postgres', 'CREATE EXTENSION pg_stat_ch');

    return $node;
}

# Sum all _count values for a histogram metric family across all label sets.
# The OTel collector Prometheus exporter creates one _count line per unique
# label combination, so we sum them to get the total number of observations.
#
# $metric_base: the metric name without suffix, e.g. "pg_stat_ch_duration_us"
# (the "pg_stat_ch_" prefix comes from namespace: pg_stat_ch in collector-config.yaml)
sub psch_get_otel_histogram_total {
    my ($metric_base) = @_;
    my $output = `curl -s 'http://localhost:9091/metrics' 2>/dev/null`;
    my $total = 0;
    for my $line (split /\n/, $output) {
        next if $line =~ /^#/;
        # Match: metric_base_count{labels} VALUE  (labels optional)
        if ($line =~ /^\Q${metric_base}\E_count(?:\{[^}]*\})?\s+(\d+(?:\.\d+)?)/) {
            $total += $1;
        }
    }
    return $total;
}

# Return true if any Prometheus metric line for $metric_base contains a label
# with the given key=value pair. Useful for verifying TagString columns.
#
# Example: psch_otel_metric_has_label("pg_stat_ch_duration_us", "db", "postgres")
sub psch_otel_metric_has_label {
    my ($metric_base, $label_key, $label_val) = @_;
    my $output = `curl -s 'http://localhost:9091/metrics' 2>/dev/null`;
    # Look for any line of this metric family containing the label key="value"
    return $output =~ /^\Q${metric_base}\E[{_][^#\n]*\Q${label_key}\E="\Q${label_val}\E"/m;
}

1;
