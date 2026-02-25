# Helper module for pg_stat_ch TAP tests
package psch;

use strict;
use warnings;
use Exporter 'import';
use PostgreSQL::Test::Cluster;
use JSON::PP;
use File::Temp;

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
    psch_otel_available
    psch_start_otel_collector
    psch_stop_otel_collector
    psch_clear_otel_data
    psch_read_otel_log_records
    psch_count_otel_logs_matching
    psch_get_otel_log_attribute
    psch_init_node_with_otel
    psch_wait_for_otel_export
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

# ============================================================================
# OpenTelemetry Integration Helpers
# ============================================================================

# OTel data directory (set by env or default)
my $_otel_data_dir = $ENV{OTEL_DATA_DIR} // '/tmp/psch-otel-data';

# Check if Docker is available for OTel tests
sub psch_otel_available {
    return system("docker ps >/dev/null 2>&1") == 0;
}

# Start OTel Collector container, return data dir path
sub psch_start_otel_collector {
    my $project_dir = $ENV{PROJECT_DIR} // '.';
    my $compose_file = "$project_dir/docker/docker-compose.otel.yml";

    # Create temp dir for JSONL output
    $_otel_data_dir = $ENV{OTEL_DATA_DIR} // File::Temp::tempdir(
        'psch-otel-XXXX', TMPDIR => 1, CLEANUP => 1);

    # Collector runs as UID 10001; needs write access
    chmod 0777, $_otel_data_dir;

    $ENV{OTEL_DATA_DIR} = $_otel_data_dir;

    # Start container
    system("docker compose -f $compose_file up -d") == 0
        or die "Failed to start OTel Collector container";

    # Wait for health check (up to 30 seconds)
    for my $i (1..60) {
        my $result = `curl -sf http://localhost:13133/ 2>/dev/null`;
        return $_otel_data_dir if $? == 0;
        select(undef, undef, undef, 0.5);
    }
    die "OTel Collector failed to become healthy";
}

# Stop OTel Collector container
sub psch_stop_otel_collector {
    my $project_dir = $ENV{PROJECT_DIR} // '.';
    my $compose_file = "$project_dir/docker/docker-compose.otel.yml";
    system("docker compose -f $compose_file down -v");
}

# No-op kept for API compatibility; tests use content-based matching
sub psch_clear_otel_data { }

# Parse ALL JSONL log records, flatten attributes
# Returns arrayref of hashrefs with flattened attributes
sub psch_read_otel_log_records {
    my $logs_file = "$_otel_data_dir/logs.jsonl";
    return [] unless -f $logs_file;

    open(my $fh, '<', $logs_file) or return [];
    my @records;

    while (my $line = <$fh>) {
        chomp $line;
        next unless $line;

        my $data;
        eval { $data = decode_json($line); };
        next if $@;

        # Navigate: resourceLogs[].scopeLogs[].logRecords[]
        my $resource_logs = $data->{resourceLogs} // [];
        for my $rl (@$resource_logs) {
            my $scope_logs = $rl->{scopeLogs} // [];
            for my $sl (@$scope_logs) {
                my $log_records = $sl->{logRecords} // [];
                for my $lr (@$log_records) {
                    my $flat = _flatten_attributes($lr->{attributes} // []);
                    push @records, $flat;
                }
            }
        }
    }
    close $fh;
    return \@records;
}

# Flatten OTLP attributes array into {name => value} hash
sub _flatten_attributes {
    my ($attrs) = @_;
    my %flat;
    for my $attr (@$attrs) {
        my $key = $attr->{key};
        my $val = $attr->{value};
        if (defined $val->{stringValue}) {
            $flat{$key} = $val->{stringValue};
        } elsif (defined $val->{intValue}) {
            $flat{$key} = $val->{intValue};
        } elsif (defined $val->{doubleValue}) {
            $flat{$key} = $val->{doubleValue};
        } elsif (defined $val->{boolValue}) {
            $flat{$key} = $val->{boolValue};
        } else {
            $flat{$key} = undef;
        }
    }
    return \%flat;
}

# Count log records matching attribute filters (values can be regex)
sub psch_count_otel_logs_matching {
    my (%criteria) = @_;
    my $records = psch_read_otel_log_records();
    my $count = 0;

    for my $rec (@$records) {
        my $match = 1;
        for my $key (keys %criteria) {
            my $expected = $criteria{$key};
            my $actual = $rec->{$key};
            if (!defined $actual) {
                $match = 0;
                last;
            }
            if (ref $expected eq 'Regexp') {
                unless ($actual =~ $expected) {
                    $match = 0;
                    last;
                }
            } else {
                unless ($actual eq $expected) {
                    $match = 0;
                    last;
                }
            }
        }
        $count++ if $match;
    }
    return $count;
}

# Get a specific attribute value from a record hash
sub psch_get_otel_log_attribute {
    my ($record, $name) = @_;
    return $record->{$name};
}

# Initialize a node with OTel export enabled
sub psch_init_node_with_otel {
    my ($name, %opts) = @_;

    my $queue_capacity = $opts{queue_capacity} // 65536;
    my $flush_interval_ms = $opts{flush_interval_ms} // 100;
    my $batch_max = $opts{batch_max} // 1000;
    my $enabled = $opts{enabled} // 'on';
    my $otel_endpoint = $opts{otel_endpoint} // 'localhost:4317';
    my $hostname = $opts{hostname} // 'test-host';

    my $node = PostgreSQL::Test::Cluster->new($name);
    $node->init();
    $node->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'pg_stat_ch'
pg_stat_ch.enabled = $enabled
pg_stat_ch.use_otel = on
pg_stat_ch.otel_endpoint = '$otel_endpoint'
pg_stat_ch.hostname = '$hostname'
pg_stat_ch.queue_capacity = $queue_capacity
pg_stat_ch.flush_interval_ms = $flush_interval_ms
pg_stat_ch.batch_max = $batch_max
});
    $node->start();
    $node->safe_psql('postgres', 'CREATE EXTENSION pg_stat_ch');

    return $node;
}

# Wait for events to be exported via OTel (polls stats counter)
# Returns the final exported count, or current count on timeout
sub psch_wait_for_otel_export {
    my ($node, $min_expected, $timeout_secs) = @_;
    $timeout_secs //= 15;

    my $start_time = time();
    while (time() - $start_time < $timeout_secs) {
        my $stats = psch_get_stats($node);
        return $stats->{exported} if $stats->{exported} >= $min_expected;
        select(undef, undef, undef, 0.5);
    }

    my $stats = psch_get_stats($node);
    return $stats->{exported};
}

1;
