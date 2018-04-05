#!/usr/bin/perl

use strict;
use warnings;

use ZRTG::Config;
use DBI;

my $number = shift;

my $host =  $ZRTG::Config::DB_HOST;
my $user =  $ZRTG::Config::DB_USER;
my $pw =  $ZRTG::Config::DB_PASS;
my $db =  $ZRTG::Config::DB_SCHEMA;

my $dbh = DBI->connect("dbi:mysql:database=$db;host=$host",
                $user,$pw) or die $DBI::errstr;

$dbh->do('truncate `interface`');

for my $rid (1 .. $number) {
    for my $tbl (qw(ifOutUcastPkts ifInUcastPkts ifOutOctets ifInOctets)) {
        $dbh->do(qq/drop table if exists `${tbl}_${rid}`/);
        $dbh->do(qq/create table `${tbl}_${rid}` like `${tbl}`/);
    }
    
}

