package ZRTG::Worker;

use 5.10.0;

use threads;

use strict;
use warnings;

# Various necessary libraries
use Carp;
use Log::Message::Simple qw(:STD);

# ZMQ Libraries
use ZMQ::LibZMQ3;
use ZMQ::Constants qw(:all);

# ZRTG Libraries
use ZRTG::Constants qw(:all);
use ZRTG::Config;
use ZRTG::Payload;
use ZRTG::WorkerBee;

sub new {
    my $class = shift;
    my $name = shift;

    my $self = {
        context             => zmq_ctx_new(),
        upstreamSocket      => undef,
        downstreamSocket    => undef,
        debug               => $ZRTG::Config::DEBUG,
        name                => $name,
        numThreads          => 2,
        keepAlive           => 300,                         # Seconds
        blockInterval       => 100,                         # Milliseconds
        lastUpstream        => 0,
    };

    bless $self, $class;
}

sub DESTROY {
    my $self = shift;
    
    if ( $self->{upstreamSocket} ) {
        zmq_close( $self->{upstreamSocket} );
    }
    
    if ( $self->{downstreamSocket} ) {
        zmq_close( $self->{downstreamSocket} );
    }
    
    if ( $self->{context} ) {
        zmq_ctx_destroy( $self->{context} );
    }
}

sub connectSockets {
    my $self = shift;
    my $upstreamAddress = shift;
    
    # Start with the upstream socket
    my $upstreamSocket = zmq_socket( $self->{context}, ZMQ_DEALER );
    
    if (! defined($upstreamSocket)) {
        die "Could not create upstream socket: $!";
    }
    
    # Set the identity of this worker
    zmq_setsockopt( $upstreamSocket, ZMQ_IDENTITY, $self->{name} );
    
    if (0 != zmq_connect( $upstreamSocket, $upstreamAddress )) {
        die "Could not connect upstream socket: $!";
    }
    
    # Looks good, save it
    $self->{upstreamSocket} = $upstreamSocket;
    
    # Time to do the downstream
    my $downstreamSocket = zmq_socket( $self->{context}, ZMQ_DEALER );
    
    if (! defined($downstreamSocket)) {
        die "Could not create downstream socket: $!";
    }
    
    if (0 != zmq_bind( $downstreamSocket, 'inproc://workers' ) ) {
        die "Could not bind downstream socket: $!";
    }
    
    # Looks good, save it
    $self->{downstreamSocket} = $downstreamSocket;
    
    return 0;
}

sub start {
    my $self = shift;
    my $port = shift;

    debug "Connecting sockets", $self->{debug};
    $self->connectSockets($port);

    # Launch pool of worker threads
    debug "Creating $self->{numThreads} threads", $self->{debug};
    for ( 1 .. $self->{numThreads} ) {
        threads->create( 'ZRTG::WorkerBee::main', $self->{context}, $self->{debug} );
    }

    # Tell the ventilator that we're ready to roll
    $self->sendReady();

    # Items to poll on
    my $upstreamItem = {
        socket => $self->{upstreamSocket},
        events => ZMQ_POLLIN,
        callback => sub {
            debug "Received request, forwarding downstream", $self->{debug};
            $self->dealerForwarder('downstream');
        },
    };

    my $downstreamItem = {
        socket => $self->{downstreamSocket},
        events => ZMQ_POLLIN,
        callback => sub { 
            debug "Received response, forwarding upstream", $self->{debug};
            $self->dealerForwarder('upstream');
        },
    };

    # Just keep swimming
    while (1) {
        my $events = zmq_poll([ $upstreamItem, $downstreamItem ], $self->{blockInterval});
        
        if ($self->{keepAlive}) {
            if (time() - $self->{lastUpstream} > $self->{keepAlive}) {
                $self->sendReady();
            }
        }
    }

    # We should flush log buffer (to somewhere) periodically
}

# Forward packages up/downstream to workers.  Fair queued by the ZMQ dealer
sub dealerForwarder {
    my $self = shift;
    my $mode = shift;
    
    my $fromSocket;
    my $toSocket;
    
    # If mode is upstream then we strip off the leading empty frame
    # If mode is downstream then we add a leading empty frame
    if ($mode eq 'upstream') {
        $fromSocket = $self->{downstreamSocket};
        $toSocket = $self->{upstreamSocket};
        
        my $empty;
        my $emptyReceived = zmq_recv($fromSocket, $empty, 1);

        if ($emptyReceived) {
            carp "Expected empty frame from downstream but got $empty";
            return;
        } else {
            print "Stripping empty frame\n" if ($self->{debug} > 2);
        }
        
        $self->{lastUpstream} = time();
    } elsif ($mode eq 'downstream') {
        $fromSocket = $self->{upstreamSocket};
        $toSocket = $self->{downstreamSocket};
        
        my $typeMessage = zmq_recvmsg($fromSocket);
        my $type = zmq_msg_data($typeMessage);
        
        if ($type == ZRTG_PING) {
            # Deal with this immediately
            $self->sendReady();
            
            return;
        } else {
            print "sending empty frame and more\n"  if ($self->{debug} > 2);
            zmq_send($toSocket, '', 0, ZMQ_SNDMORE);
            print "sending $type and more\n"  if ($self->{debug} > 2);
            zmq_send($toSocket, $type, -1, ZMQ_SNDMORE);
        }
    }

    # Forward message"
    while (1) {
        my $msg = zmq_msg_init();
        my $data;

        zmq_msg_recv($msg, $fromSocket);
        $data = zmq_msg_data($msg);

        if (zmq_getsockopt($fromSocket, ZMQ_RCVMORE)) {
            print "sending $data and more\n"  if ($self->{debug} > 2);
            zmq_send($toSocket, $data, -1, ZMQ_SNDMORE);
        } else {
            print "sending $data\n" if ($self->{debug} > 2);
            zmq_send($toSocket, $data, -1);
            last;
        }
    }
}

sub sendReady {
    my $self = shift;
    
    debug "Sending READY", $self->{debug};
    
    $self->{lastUpstream} = time();
    
    zmq_send($self->{upstreamSocket}, '', 0, ZMQ_SNDMORE);
    zmq_send($self->{upstreamSocket}, ZRTG_READY, -1);
}

1;
