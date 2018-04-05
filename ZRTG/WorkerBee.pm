package ZRTG::WorkerBee;

use 5.10.0;

use threads;

use strict;
use warnings;

# Various necessary libraries
use Log::Message::Simple qw(:STD);
use Time::HiRes qw(gettimeofday tv_interval);
use JSON;

# Should use a different SNMP module since the maximum message size
# with Net::SNMP will likely be problematic
use Net::SNMP;

# ZMQ Libraries
use ZMQ::LibZMQ3;
use ZMQ::Constants qw(:all);

# ZRTG Libraries
use ZRTG::Constants qw(:all);
use ZRTG::Payload;

# Thread entry point
sub main {
    my $context = shift;
    my $debug = shift;

    # Socket to talk to dispatcher
    my $dealer = zmq_socket($context, ZMQ_REP);

    if (0 != zmq_connect( $dealer, 'inproc://workers' )) {
        die "Could not connect dealer socket: $!";
    }

    while (1) {
        # Block while waiting for some data
        my $payload = ZRTG::Payload->new($dealer);

        # Handle incoming data
        if ($payload->{type} eq ZRTG_SNMP_SMART_REQUEST) {
            snmpSmartRequest($payload, $dealer, $debug);
        } elsif ($payload->{type} eq ZRTG_REFRESH_INTERFACES) {
            snmpGrabInterfaces($payload, $dealer, $debug);
        } else {
            # Probably don't really want to die here, but for debugging it's not
            # very nice if we hit this and things go into an infinite loop or
            # something
            die "Unknown request"
        }
        
        # Should flush log buffer periodically
    }
}

# Bulk walk an entire OID tree based on the number of interfces
sub snmpSmartRequest {
    my $payload = shift;
    my $socket = shift;
    my $debug = shift;
    
    # Suck in arguments in the order in which we expect them
    my @oids = @{$payload->{data}};
    my $host = shift(@oids);
    my $community = shift(@oids);
    my $version = shift(@oids);
    
    # Log which thread this came from, helps out with debugging
    my $tid = threads->tid;
    
    debug "Processing SNMP_SMART_REQUEST for $host ($tid)", $debug;
    
    # Initialize an SNMP session
    my ($session, $error) = Net::SNMP->session(
        -hostname  => $host,
        -community => $community,
        -version   => $version,
    );

    # Store retrieved counter values here
    my @workerResults;

    if (! defined $session) {
        error "Could not create SNMP session: $error";
        return;
    }

    # Grab the number of interfaces on this device and record how long it takes
    my $t0 = [gettimeofday];
    my $snmp_interfaces = $session->get_request(-varbindlist => [ NUM_INTERFACES_OID ]);
    my $t1 = [gettimeofday];

    # We're going to say that's a pretty good indication of the RTT for this
    # device
    my $latency = pack('f<', tv_interval($t0, $t1));

    # If we got that back, let's go ahead and do bulk requests for all of our
    # OIDs using that count as our bulk limit
    if (defined($snmp_interfaces->{&NUM_INTERFACES_OID})) {
        my $count = $snmp_interfaces->{&NUM_INTERFACES_OID};

        # These next requests could get big...
        $session->max_msg_size(65535);

        # Theoretical interface name -> oid associations can change per-reboot
        # since the name is what we generally use for a key, grab that every time
        my $names = $session->get_bulk_request(-maxrepetitions => $count,
                                               -varbindlist => [ INTERFACE_NAMES_OID ]
                                              );

        # Now bulk walk the OID tree for each requested MIB, usingthe number of
        # interfaces for a max repetitions value
        for my $mib (@oids) {
            my $res = $session->get_bulk_request(-maxrepetitions => $count,
                                                 -varbindlist => [ $mib ]
                                                );
            my $t2 = [gettimeofday];
            my $types = $session->var_bind_types();

            # Populate results hash based off of the time on this machine
            for my $oid (keys(%{$res})) {
                # oid
                # name
                # counter
                # time
                # type
                if ($oid =~ /^(.*)(\.\d+)$/) {
                    if ($1 ne $mib) {
                        debug "Dropping $oid since we didn't ask for it", ($debug > 1);
                        next;
                    } elsif (! $names->{INTERFACE_NAMES_OID . $2}) {
                        debug "Dropping $oid since we couldn't resolve an interface name", ($debug > 1);
                        next;
                    }
                    
                    push @workerResults, [ $oid, $names->{INTERFACE_NAMES_OID . $2}, $res->{$oid}, $t2->[0], $types->{$oid} ];
                } else {
                    warn "Uh oh, that's a weird looking oid: $oid";
                }
            }
        }
    } else {
        warn "Failed to find the number of interfaces on this device"
    }

    # And close the SNMP connection
    $session->close();

    debug "Shipping off result for $host ($tid)", $debug;

    my $out = ZRTG::Payload->new(ZRTG_SNMP_RESPONSE);

    $out->setData(
                    $host,
                    $latency,
                    JSON->new->allow_nonref->encode(\@workerResults)
                );

    $out->send($socket, '');
}

sub snmpGrabInterfaces {
    my $payload = shift;
    my $socket = shift;
    my $debug = shift;
    
    # Suck in arguments in the order in which we expect them
    my $host = $payload->{data}->[0];
    my $community = $payload->{data}->[1];
    my $version = $payload->{data}->[2];
    
    # Log which thread this came from, helps out with debugging
    my $tid = threads->tid;
    
    # Store retrieved values here
    my %workerResults;
    
    debug "Processing REFRESH_INTERFACES for $host ($tid)", $debug;
    
    my ($session, $error) = Net::SNMP->session(
        -hostname  => $payload->{data}->[0],
        -community => $payload->{data}->[1],
        -version   => $payload->{data}->[2],
    );
    
    my $snmp_interfaces = $session->get_request(-varbindlist => [ NUM_INTERFACES_OID ]);
    
    if (defined($snmp_interfaces->{&NUM_INTERFACES_OID})) {
        my $count = $snmp_interfaces->{&NUM_INTERFACES_OID};

        # These could get big...
        $session->max_msg_size(65535);
        
        my $names = $session->get_bulk_request(-maxrepetitions => $count,
                                               -varbindlist => [ INTERFACE_NAMES_OID ]
                                              );
        
        my $aliases = $session->get_bulk_request(-maxrepetitions => $count,
                                                 -varbindlist => [ INTERFACE_ALIASES_OID ]
                                                );
        
        my $speeds = $session->get_bulk_request(-maxrepetitions => $count,
                                               -varbindlist => [ INTERFACE_HIGHSPEED_OID ]
                                              );
        my $adminStatus = $session->get_bulk_request(-maxrepetitions => $count,
                                                     -varbindlist => [ INTERFACE_ADMIN_STATUS_OID ]
                                                    );
        my $operStatus = $session->get_bulk_request(-maxrepetitions => $count,
                                                    -varbindlist => [ INTERFACE_OPER_STATUS_OID ]
                                                   );
        $session->close();
        
        for (keys(%{$names})) {
            /^(.*)\.(\d+)$/;
            if ($1 ne INTERFACE_NAMES_OID) {
                debug "Dropping $1 since we didn't ask for it", $debug;
                next;
            }
            $workerResults{$2}->[0] = $names->{$_};
        }
        
        for (keys(%{$aliases})) {
            /^(.*)\.(\d+)$/;
            if ($1 ne INTERFACE_ALIASES_OID) {
                debug "Dropping $1 since we didn't ask for it", $debug;
                next;
            }
            $workerResults{$2}->[1] = $aliases->{$_};
        }
        
        for (keys(%{$speeds})) {
            /^(.*)\.(\d+)$/;
            if ($1 ne INTERFACE_HIGHSPEED_OID) {
                debug "Dropping $1 since we didn't ask for it", $debug;
                next;
            }
            $workerResults{$2}->[2] = $speeds->{$_};
        }
        
        for (keys(%{$adminStatus})) {
            /^(.*)\.(\d+)$/;
            if ($1 ne INTERFACE_ADMIN_STATUS_OID) {
                debug "Dropping $1 since we didn't ask for it", $debug;
                next;
            }
            $workerResults{$2}->[3] = $adminStatus->{$_};
        }
        
        for (keys(%{$operStatus})) {
            /^(.*)\.(\d+)$/;
            if ($1 ne INTERFACE_OPER_STATUS_OID) {
                debug "Dropping $1 since we didn't ask for it", $debug;
                next;
            }
            $workerResults{$2}->[4] = $operStatus->{$_};
        }

        debug "Shipping off result for $host ($tid)", $debug;

        my $out = ZRTG::Payload->new(ZRTG_INTERFACE_RESPONSE);

        $out->setData($host,
                      encode_json(\%workerResults)
                     );

        $out->send($socket, '');
        
    }
}

1;
