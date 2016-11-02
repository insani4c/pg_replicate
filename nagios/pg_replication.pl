#!/usr/bin/env perl 
use strict;
use warnings;
use utf8;

use DBI;
use Getopt::Long;
use POSIX qw/setuid/;
use Data::Dumper;

my @size_labels = qw/Bytes KiloBytes MegaBytes GigaBytes TerraBytes/;

my($host, $db, $port, $threshold, $debug);
GetOptions(
    '--host=s'      => \$host,
    '--db=s'        => \$db,
    '--port=s'      => \$port,
    '--threshold=i' => \$threshold,
    '--debug'       => \$debug,
);

$host       ||= '/var/run/postgresql';
$port       ||= 5432;
$db         ||= 'postgres';
$threshold  ||= 80;

# 0 = OK
# 1 = WARNING
# 2 = CRITICAL
my $return_code = 0;
my @err_msg = ();

setuid(scalar getpwnam('postgres'));

my $dbh = DBI->connect("dbi:Pg:host=$host;dbname=$db;port=$port", 'postgres', undef);
my $stats = $dbh->selectall_arrayref("select * from replication_info", {Slice => {}});

# first check which slots are inactive
my @inactive = grep {$_->{active} == 0} @$stats;
if(scalar @inactive){
    print "Inactive ones: \n", Dumper(\@inactive) if $debug;
    my ($io) = '';
    map {$io .= $_->{slot_name} . " "} @inactive;
    push @err_msg, "CRITICAL: Inactive replication slots found ($io)!\n";
    $return_code = 2;
}

my $disk_free = do{ local $/; open my $c, '-|', 'df | grep /pg_archive'; <$c> };
my ($total_space, $space_used, $space_available, $percentage_used ) = ($disk_free =~ /\S+\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)%\s+\S+/);
print "Kilobytes free: $space_available KB\n" if $debug;

my $total_lag = 0;
map {$total_lag += $_->{slot_lsn_bytes_lag}} @$stats;
print "Current total lag: ", pretty_size($total_lag), "\n" if $debug;


if($percentage_used > 90){
    $return_code = 1 unless $return_code;
    push @err_msg, "CRITICAL: Percentage used ($percentage_used %) for pg_archive is bigger than 90%!!\n";
}
elsif($percentage_used > $threshold){
    $return_code = 1 unless $return_code;
    push @err_msg, "WARNING: Percentage used ($percentage_used %) for pg_archive is bigger than threshold ($threshold %)\n";
}

print "Lag per node:\n" if $debug;
foreach my $n (sort {$b->{slot_lsn_bytes_lag} <=> $a->{slot_lsn_bytes_lag}} @$stats){
    print " - ", $n->{slot_name}, ": ", pretty_size($n->{slot_lsn_bytes_lag}), " (", ($n->{slot_lsn_bytes_lag}/$total_lag*100) ,"%)\n" if $debug;
}

if(scalar @err_msg){
    print foreach @err_msg;
}
else {
    print "OK: All replication slots are fine\n"
}
exit $return_code;

sub pretty_size {
    my ($size) = @_;

    my $size_label = 0;
    while($size > 1024){
        $size = sprintf("%.2f", $size / 1024);
        $size_label++;
    }

    return "$size $size_labels[$size_label]"
}
