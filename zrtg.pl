#!/usr/bin/perl

use strict;
use warnings;
use 5.10.0;

use ZRTG::Config;

use Getopt::Long;

my ($debug, $takeover, $worker);

GetOptions ("d|debug"           => \$debug,
            "w|worker=s"        => \$worker,
            "t|takeover=s"      => \$takeover);

if ($debug) {
    $ZRTG::Config::DEBUG = $debug;
}

my $endpoint = shift;

if (! defined($endpoint) ||
      $endpoint !~ /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+$/) {
    print "usage: $0 <server:port>\n";
    exit -1;
}

my $obj;

if ($worker) {
    require ZRTG::Worker;
    $obj = ZRTG::Worker->new($worker);
} else {
    require ZRTG::Ventilator;
    $obj = ZRTG::Ventilator->new();
}

# Use 172.28.15.250:5301 for testing
if ($takeover) {
    $obj->start("tcp://${endpoint}", $takeover);
} else {
    $obj->start("tcp://${endpoint}");
}


