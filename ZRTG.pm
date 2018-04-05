package ZRTG;

use 5.10.0;

use strict;
use warnings;

use threads;

use Data::Dumper;

use JSON;
use Log::Message::Simple qw(:STD);

use Net::SNMP;
use ZRTG::Constants qw(:all);
use ZMQ::LibZMQ3;
use ZMQ::Constants qw(:all);

use ZRTG::Payload;
use ZRTG::Ventilator;
use ZRTG::Worker;

use DBI;

use Carp;


my $debug = 1;
my $superdebug = 0;

1;


