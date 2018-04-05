package ZRTG::Ventilator;

use 5.10.0;

use strict;
use warnings;

# Various necessary libraries
use Carp;
use JSON;
use Time::HiRes qw(gettimeofday tv_interval);
use Log::Message::Simple qw(:STD);
use POSIX qw(strftime);

# ZMQ Libraries
use ZMQ::LibZMQ3;
use ZMQ::Constants qw(:all);

# ZRTG Libraries
use ZRTG::Constants qw(:all);
use ZRTG::Config;
use ZRTG::Payload;
use ZRTG::DbMysql;

sub new {
    my $class = shift;

    my $self = {
        db                  => ZRTG::DbMysql->new(),
        insertBuffer        => {},
        context             => zmq_ctx_new(),
        debug               => $ZRTG::Config::DEBUG,
        workerSocket        => undef,
        targets             => {},
        workers             => {},
        startupTime         => 1.5,
        pollInterval        => 30,                          # Seconds
        blockInterval       => 100,                         # Milliseconds
        timeFormat          => '%Y-%m-%d %H:%M:%S',
        insertBatchSize     => 50,
        pruneThreshold      => 20,
        shutdownWait        => 2000,                        # Milliseconds
        readyToPoll         => 1,
    };

    bless $self, $class;

    $self->{db}->{targets} = $self->{targets};

    return $self;
}

sub DESTROY {
    my $self = shift;
    
    if ( $self->{workerSocket} ) {
        zmq_close( $self->{workerSocket} );
    }
    
    if ( $self->{context} ) {
        zmq_ctx_destroy( $self->{context} );
    }
}

sub connectWorker {
    my $self = shift;
    my $address = shift;
    
    my $workerSocket = zmq_socket( $self->{context}, ZMQ_ROUTER );
    
    if (! defined($workerSocket)) {
        die "Could not create worker socket: $!";
    }
    
    if (0 != zmq_bind( $workerSocket, $address )) {
        die "Could not bind worker socket ($address): $!";
    }
    
    $self->{workerSocket} = $workerSocket;
}

sub start {
    my $self = shift;
    my $workerPort = shift;
    my $takeoverFrom = shift;
    
    $self->loadConfig();
    
    debug "Connecting to worker socket", $self->{debug};
    
    $self->connectWorker($workerPort);
    
    my $dealerItem = {
        socket   => $self->{workerSocket},
        events   => ZMQ_POLLIN,
        callback => sub {
            sink( $self );
        },
    };
    
    
    # Don't need to wait if we are going to go asking to takeover
    # from another ventilator, but we should make sure somewhere that
    # we ignore children until then
    
    if (! $takeoverFrom) {
        # Give us some time to listen for workers before we
        # start shelling out orders.
        my $t0 = [gettimeofday];

        while ( tv_interval($t0) < $self->{startupTime} ) {
            zmq_poll( [ $dealerItem ], 0)
        }
    }

    # Alright, now go ham
    while (1) {
        if (defined($takeoverFrom)) {
            # Initiate the takeover request
            $self->requestTakeover($takeoverFrom, $workerPort);
            
            # Don't poll anything until we've heard back
            $self->{readyToPoll} = 0;
            
            # Don't try and make another takeover request
            $takeoverFrom = undef;
        }
        
        my $events = zmq_poll( [ $dealerItem ], $self->{blockInterval} );
        
        $self->pruneWorkers();
        
        if ($self->{readyToPoll}) {
            $self->makeRequests();
        }
        
        $self->processInserts();
        
        
    }
}

sub requestTakeover {
    my $self = shift;
    my $from = shift;
    my $workerPort = shift;
    
    my $requestSocket = zmq_socket( $self->{context}, ZMQ_DEALER );
    
    if (0 != zmq_connect( $requestSocket, "tcp://${from}" )) {
        die "Could not connect to peer ventilator ($from): $!";
    }
    
    debug "Requesting to takeover from ventilator at $from", $self->{debug};
    
    zmq_send($requestSocket, $from, 0, ZMQ_SNDMORE);
    zmq_send($requestSocket, ZRTG_TAKEOVER_REQUEST, -1, ZMQ_SNDMORE);
    zmq_send($requestSocket, $workerPort, -1);
}

sub pruneWorkers {
    my $self = shift;
    
    for my $workerId (keys(%{$self->{workers}})) {
        if ($self->{workers}->{$workerId}->{alive} == 0) {
            # We've seen you before, time for the boot
            debug "Worker $workerId is getting the boot", $self->{debug};
            delete($self->{workers}->{$workerId});
        } elsif ((time() - $self->{workers}->{$workerId}->{alive}) > $self->{pruneThreshold}) {
            my $payload = ZRTG::Payload->new(ZRTG_PING);
            
            # Put this guy in the dog house
            $self->{workers}->{$workerId}->{alive} = 0;
            
            $payload->send($self->{workerSocket}, $workerId);
        }
    }
}

# Pass on flags if they are sent in
sub getMessage {
    my $self = shift;
    
    my $msg = zmq_recvmsg( $self->{workerSocket}, @_ );
    
    if (! defined($msg)) {
        error "Error receiving message: $!"
    }
    
    return $msg;
}

# Pass on flags if they are sent in
sub getData {
    my $self = shift;
    
    my $msg = $self->getMessage( @_ );
    
    if (defined($msg)) {
        return zmq_msg_data( $msg );
    }
    
    return undef;
}

sub sink {
    my $self = shift;

    # Things coming into the sink will be prefaced with a workerId and empty
    # frame
    my $workerId = $self->getData();
    my $empty = $self->getData();

    # If there was no empty frame... something is wrong
    if ($empty) {
        carp "Expected empty frame from but got $empty";
        return;
    }

    # It should be safe to receive the payload after that
    my $payload = ZRTG::Payload->new( $self->{workerSocket} );

    if ($payload->{type} ne ZRTG_TAKEOVER_REQUEST &&
        $payload->{type} ne ZRTG_TAKEOVER_RESPONSE) {
        # Record that the worker isn't dead
        $self->{workers}->{$workerId}->{alive} = time();
    }

    if ($payload->{type} eq ZRTG_READY) {
        debug "Worker $workerId is ready", $self->{debug};
    } elsif ($payload->{type} eq ZRTG_SNMP_RESPONSE) {
        $self->processIncomingCounters( $payload, $workerId );
        
    } elsif ($payload->{type} eq ZRTG_INTERFACE_RESPONSE) {
        $self->processIncomingInterfaces( $payload, $workerId );
        
    } elsif ($payload->{type} eq ZRTG_TAKEOVER_REQUEST) {
        $self->processTakeoverRequest( $payload, $workerId );
        
    } elsif ($payload->{type} eq ZRTG_TAKEOVER_RESPONSE) {
        $self->processTakeoverResponse( $payload, $workerId );
        
    } else {
        say "Unknown type $payload->{type}";
    }
}

sub processTakeoverResponse {
    my $self = shift;
    my $payload = shift;
    my $from = shift;
    
    my $newTargets = decode_json($payload->{data}->[0]);
    
    $self->{targets} = $newTargets;
    
    # We're good to go now!
    $self->{readyToPoll} = 1;
}

sub processTakeoverRequest {
    my $self = shift;
    my $payload = shift;
    my $newGuy = shift;

    my $connectTo = $payload->{data}->[0];

    my $replySocket = zmq_socket( $self->{context}, ZMQ_DEALER );

    debug "Takeover request received.  Handing over to ventilator at $connectTo", $self->{debug};

    # Connect to the peer ventilator who is taking over
    if (0 != zmq_connect( $replySocket, $connectTo )) {
        warn "Could not connect to peer ventilator ($connectTo): $!";
    }

    # Send our targets hash to them
    zmq_send($replySocket, '', 0, ZMQ_SNDMORE);
    zmq_send($replySocket, ZRTG_TAKEOVER_RESPONSE, -1, ZMQ_SNDMORE);
    zmq_send($replySocket, encode_json($self->{targets}), -1);


    # Yuck, copy and pasted from start(), let's make this nicer
    my $dealerItem = {
        socket   => $self->{workerSocket},
        events   => ZMQ_POLLIN,
        callback => sub {
            sink(  $self );
        },
    };
    
    my $events;
    
    debug "Handed off everything I know, waiting a few for straggling workers", $self->{debug};
    
    do {
        # Wait a little bit to see if workers are still shelling out responses
        # to us
        $events = zmq_poll( [ $dealerItem ], $self->{shutdownWait} );
        
        $self->processInserts();
    } while ($events);
    
    debug "Looks good, shutting down", $self->{debug};
    
    # Clean up after ourselves
    zmq_close( $replySocket );
    
    # Game over folks
    exit 0;
}

sub chooseWorker {
    my $self = shift;
    my $host = shift;
    my $method = shift;

    my @workers = grep({ $self->{workers}->{$_}->{alive} ne 0 } keys(%{$self->{workers}}));

    my $winner;

    # These choices are easy
    if (@workers == 0) {
        return undef;
    } elsif (@workers == 1) {
        return $workers[0];
    }

    # For these we need to do something at least a little intelligent
    if ($method eq 'latency') {
        # [ worker, latency ]
        my @cheapestWorker;
        
        # If we find any workers who have never polled, we want to try them
        # ASAP to see if they have a better connection
        my $slacker;
        
        for my $worker (@workers) {
            if ($self->{workers}->{$worker}->{latency}->{$host}) {
                my $latency = $self->{workers}->{$worker}->{latency}->{$host};
                
                if (! $cheapestWorker[1]) {
                    @cheapestWorker = ( $worker, $latency );
                } elsif ($cheapestWorker[1] > $latency) {
                    @cheapestWorker = ( $worker, $latency );
                }
            } else {
                $slacker = $worker;
            }
        }
        
        # I guess the slacker always wins?
        if ($slacker) {
            $winner = $slacker;
        } elsif (@cheapestWorker) {
            $winner = $cheapestWorker[0];
        }
    }

    if (! $winner) {
        # The good old fallback: choose a random worker
        $winner = $workers[rand() * 10 % scalar(@workers)]
    }

    return $winner;
}

#
# Returns a hash:
# $poller -> $host -> targetgroups -> $oid
sub needsPolling {
    my $self = shift;
    
    my $targets = $self->{targets};
    my %ret;
    my @pollers = keys(%{$self->{workers}});

    if (@pollers) {
        for my $host (keys(%{$targets})) {
            # Use latency based load balancing
            my $poller;

            for my $targetgroup (keys(%{$targets->{$host}->{targetgroups}})) {
                if (defined($targets->{$host}->{targetgroups}->{$targetgroup}->{lastPolled})) {
                    my $time = time();
                    my $last = $targets->{$host}->{targetgroups}->{$targetgroup}->{lastPolled};
                    
                    if ($time - $last > $self->{pollInterval}) {
                        debug "$host:$targetgroup was last polled at $last, but it's $time", $self->{debug};
                        if (! $poller) {
                            # Only do this calculation once we know we are going to poll
                            $poller = $self->chooseWorker($host, 'latency');
                        }
                        push @{$ret{$poller}->{$host}->{targetgroups}}, $targetgroup;
                    }
                } else {
                    if (! $poller) {
                        # Only do this calculation once we know we are going to poll
                        $poller = $self->chooseWorker($host, 'latency');
                    }
                    push @{$ret{$poller}->{$host}->{targetgroups}}, $targetgroup;
                }
            }
        }
    }

    return \%ret;
}

sub createRefreshRequest {
    my $self = shift;
    my $rid = shift;
    
    my $payload;
    
    for my $host (keys(%{$self->{targets}})) {
        if ($self->{targets}->{$host}->{rid} eq $rid) {
            $payload = ZRTG::Payload->new(ZRTG_REFRESH_INTERFACES);
            $payload->setData($host,
                              $self->{targets}->{$host}->{community},
                              $self->{targets}->{$host}->{snmpver}
                             );
            return $payload;
        }
    }
    
    return undef;
}

sub processInserts {
    my $self = shift;
    
    for my $table (keys(%{$self->{insertBuffer}})) {
        while (my @chunk = splice(@{$self->{insertBuffer}->{$table}}, 0, $self->{insertBatchSize})) {
            $self->{db}->insertCounterValues($table, \@chunk);
        }
    }
}

sub makeRequests {
    my $self = shift;

    # Get a list of things that need polling, with a random poller chosen
    my $needsPolling = $self->needsPolling();
    
    for my $poller (keys(%{$needsPolling})) {
        for my $targetHost (keys(%{$needsPolling->{$poller}})) {
            my @targetGroups = @{$needsPolling->{$poller}->{$targetHost}->{targetgroups}};
            
            if (@targetGroups) {
                # We have entire groups that need polling, make SNMP "smart requests""
                my $payload = ZRTG::Payload->new(ZRTG_SNMP_SMART_REQUEST);
                
                $payload->setData(
                    $targetHost,
                    $self->{targets}->{$targetHost}->{community},
                    $self->{targets}->{$targetHost}->{snmpver},
                    @targetGroups,
                );
                
                $payload->send($self->{workerSocket}, $poller);

                my $time = time();

                for my $targetGroup (@{$needsPolling->{$poller}->{$targetHost}->{targetgroups}}) {
                    $self->{targets}->{$targetHost}->{targetgroups}->{$targetGroup}->{lastPolled} = $time;
                }
            }
            
            # We could make some dumb requests here, if we cared to implement that
        }
    }
}



sub loadConfig {
    my $self = shift;
    
    my @hosts = @ZRTG::Config::HOSTS;
    
    for my $i (1 .. @hosts) {
        my $host = $hosts[$i - 1];
        
        $self->{targets}->{$host}->{community} = $ZRTG::Config::COMMUNITY;
        $self->{targets}->{$host}->{snmpver} = 'snmpv2c';
        $self->{targets}->{$host}->{rid} = $i;                  #### THIS IS NEW ###
        $self->{targets}->{$host}->{targetgroups}->{'.1.3.6.1.2.1.31.1.1.1.7'} = { # ifHCInUcastPkts
            table   => "ifInUcastPkts_$i",
        };
        $self->{targets}->{$host}->{targetgroups}->{'.1.3.6.1.2.1.31.1.1.1.6'} = { # ifHCInOctets
            table   => "ifInOctets_$i",
        };
        $self->{targets}->{$host}->{targetgroups}->{'.1.3.6.1.2.1.31.1.1.1.10'} = { # ifHCOutOctets
            table   => "ifOutOctets_$i",
        };
        $self->{targets}->{$host}->{targetgroups}->{'.1.3.6.1.2.1.31.1.1.1.11'} =  { # ifHCOutUcastPkts
            table   => "ifOutUcastPkts_$i",
        };
    }
}

sub processIncomingInterfaces {
    my $self = shift;
    my $payload = shift;
    my $workerId = shift;
    
    my $host = $payload->{data}->[0];
    my $json = $payload->{data}->[1];
    
    debug "Received interface data from $workerId for $host", $self->{debug};
}

sub processIncomingCounters {
    my $self = shift;
    my $payload = shift;
    my $workerId = shift;
    
    my $host = $payload->{data}->[0];
    my $latency = unpack('f<', $payload->{data}->[1]);
    my $json = $payload->{data}->[2];

    # Nice even millisecond number
    my $latencyFormatted = int($latency * 1000);

    # Used for recording when this data came in
    my $time = time();

    debug "Received counter data from $workerId: Latency to host $host was $latencyFormatted milliseconds, JSON payload (" . length($json) . " bytes) was attached", $self->{debug};

    $self->{workers}->{$workerId}->{latency}->{$host} = $latency;

    # [ oid, counter, time, type ]
    my $data = decode_json($json);

    my %receivedBaseOids;

    for my $set (@{$data}) {
        # set = [ oid, ifName, counter, dtime, counterType ]
        
        if ($set->[0] =~ /(.*)(\.\d+)/) {
            my ($base, $interface) = ($1, $2);
            
            my $iid = $self->{db}->getIid($host, $set->[1]);
            
            my $table;
            
            if (exists($self->{targets}->{$host}->{targetgroups}->{$base}->{table})) {
                $table = $self->{targets}->{$host}->{targetgroups}->{$base}->{table};
            } else {
                # This shouldn't really ever happen, it means we got bogus data
                # from the worker
                warn "Could not find table for base OID $base";
                next;
            }
            
            if (! $iid) {
                # We couldn't look up (or create) an interface (), just drop
                # this until we determine something better to do with it
                next;
            }
            
            $receivedBaseOids{$base}++;
            
            # counters hash => [ counter, dtime ]
            
            if (exists($self->{targets}->{$host}->{counters}->{$table}->{$iid})) {
                # We have a previous counter value in format [ counter, time ]
                # Calculate the delta
                my ($lastCounter, $lastTime) = @{$self->{targets}->{$host}->{counters}->{$table}->{$iid}};
                
                my $timeDiff = $set->[3] - $lastTime;
                my $counterDiff = $set->[2] - $lastCounter;
                
                my $rate = $counterDiff / $timeDiff;
                
                # Queue it for inserting into the database
                push @{$self->{insertBuffer}->{$table}}, [ $iid, strftime($self->{timeFormat}, gmtime($set->[3])), $counterDiff, $rate ]
                
            } else {
                # First time we've seen this interface, insert 0 value
                push @{$self->{insertBuffer}->{$table}}, [ $iid, strftime($self->{timeFormat}, gmtime($set->[3])), 0, 0 ]
            }
            
            # Set the counter values in our internal data structure [ counter, time ]
            $self->{targets}->{$host}->{counters}->{$table}->{$iid} = [ $set->[2], $set->[3] ];
        } else {
            error "$set->[0] doesn't look like an OID"
        }
    }
    
    for my $receivedBaseOid (keys(%receivedBaseOids)) {
        $self->{targets}->{$host}->{targetgroups}->{$receivedBaseOid}->{lastReceived} = $time;
    }
}

1;
