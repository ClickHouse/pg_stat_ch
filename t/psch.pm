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
        'queue_size, queue_capacity, queue_usage_pct ' .
        'FROM pg_stat_ch_stats()');

    my @values = split /\|/, $result;
    return {
        enqueued   => $values[0] // 0,
        dropped    => $values[1] // 0,
        exported   => $values[2] // 0,
        queue_size => $values[3] // 0,
        capacity   => $values[4] // 0,
        usage_pct  => $values[5] // 0,
    };
}

# Reset stats
sub psch_reset_stats {
    my ($node) = @_;
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_reset()');
}

1;
