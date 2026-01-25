# Helper module for pg_stat_ch TAP tests
package psch;

use strict;
use warnings;
use Exporter 'import';
use PostgreSQL::Test::Cluster;

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
        'send_failures, queue_size, queue_capacity, queue_usage_pct ' .
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

1;
