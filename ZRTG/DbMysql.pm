package ZRTG::DbMysql;

use 5.10.0;

use strict;
use warnings;

use DBI;

use ZRTG::Config;

sub new {
    my $class = shift;

    my $self = {
        dbh             => undef,
    };

    bless $self, $class;
}

sub getIid {
    my $self = shift;
    my $host = shift;
    my $ifName = shift;
    
    if (! $self->{targets}->{$host}->{interfaces}->{$ifName}) {
        # We don't know about this interface yet, look it up
        
        my $ret = $self->dbh->selectall_arrayref(q/select `id` from `interface` where rid=? and name=?/, { }, $self->{targets}->{$host}->{rid}, $ifName );
        
        if (@{$ret}) {
            $self->{targets}->{$host}->{interfaces}->{$ifName} = $ret->[0]->[0];
        } else {
            $self->dbh->do(q/insert into `interface` (`name`, `rid`) values (?, ?)/, {}, $ifName, $self->{targets}->{$host}->{rid});
            $self->{targets}->{$host}->{interfaces}->{$ifName} = $self->dbh->{mysql_insertid};
        }
    }
    
    return $self->{targets}->{$host}->{interfaces}->{$ifName};
}

sub dbh {
    my $self = shift;
    
    if (! $self->{dbh} ) {
        my $host = $ZRTG::Config::DB_HOST;
        my $user = $ZRTG::Config::DB_USER;
        my $pw = $ZRTG::Config::DB_PASS;
        my $db = $ZRTG::Config::DB_SCHEMA;
        
        $self->{dbh} = DBI->connect("dbi:mysql:database=$db;host=$host",
                $user,$pw) or die $DBI::errstr;
    }

    return $self->{dbh};
}

sub insertCounterValues {
    my $self = shift;
    my $table = shift;
    my $chunk = shift;

    # TODO: Fix this so we aren't at risk of SQL injection
    my $query = "insert into `${table}` (`id`, `dtime`, `counter`, `rate`) values ";
    
    for my $i (0 .. @{$chunk} - 1) {
        $query .= "($chunk->[$i]->[0],'$chunk->[$i]->[1]',$chunk->[$i]->[2],$chunk->[$i]->[3])";
        
        if ($i < @{$chunk} - 1) {
            $query .= ',';
        }
    }
    
    $self->dbh->do($query);
}

1;
