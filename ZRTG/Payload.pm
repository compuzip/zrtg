package ZRTG::Payload;

use 5.10.0;

use strict;
use warnings;

use Carp;

use ZRTG::Config;
use ZRTG::Constants qw(:all);
use Log::Message::Simple qw(:STD);

use ZMQ::LibZMQ3;
use ZMQ::Constants qw(:all);

sub new {
    my $class = shift;
    my $arg = shift;
    my @data = @_;

    my $self = { data => (@_) ? [@_] : [] };

    if (ref($arg)) {
        # Read in all of our data (block)
        my $msg;
        
        while (1) {
            my $msg = zmq_msg_init();
            my $data;

            zmq_msg_recv($msg, $arg);
            $data = zmq_msg_data($msg);

            if (! $self->{type}) {
                if ($data) {
                    $self->{type} = $data;
                } else {
                    print "No type yet, and our data frame is empty!\n";
                }
            } else {
                push @{$self->{data}}, $data;
            }
            
            if (! zmq_getsockopt($arg, ZMQ_RCVMORE)) {
                last;
            }
        }
    } else {
        $self->{type} = $arg;
    }

    bless $self, $class;
}

sub setData {
    my $self = shift;
    my @data = @_;
    
    push @{$self->{data}}, @data;
}

sub send {
    my $self = shift;
    my $socket = shift;
    my $destination = shift;

    if (! ref($socket) || ! $socket->isa('ZMQ::LibZMQ3::Socket')) {
        carp("Expected a ZMQ::LibZMQ3::Socket object, bailing");
        return;
    }

    if ($self->{type} == ZRTG_READY) {
        debug "Sending READY", $ZRTG::Config::DEBUG;
        
    } elsif ($self->{type} == ZRTG_PING) {
        debug "Sending PING to $destination", $ZRTG::Config::DEBUG;
        
    } elsif ($self->{type} == ZRTG_SNMP_SMART_REQUEST) {
        debug "Sending request to $destination: $self->{data}->[0]", $ZRTG::Config::DEBUG;
        
    } elsif ($self->{type} == ZRTG_SNMP_RESPONSE) {
        debug "Sending SNMP response", $ZRTG::Config::DEBUG;
        
    } elsif ($self->{type} == ZRTG_REFRESH_INTERFACES) {
        debug "Requesting interface refresh", $ZRTG::Config::DEBUG;
        
    } elsif ($self->{type} == ZRTG_INTERFACE_RESPONSE) {
        debug "Sending interface refresh response", $ZRTG::Config::DEBUG;
        
    } elsif ($self->{type} == ZRTG_TAKEOVER_REQUEST) {
        debug "Sending takeover request", $ZRTG::Config::DEBUG;
        
    } elsif ($self->{type} == ZRTG_TAKEOVER_RESPONSE) {
        debug "Sending takeover response", $ZRTG::Config::DEBUG;
        
        zmq_send($socket, $destination, -1, ZMQ_SNDMORE);
        
    } else {
        say "Unknown message type: $self->{type}";
        return undef;
    }

    $self->_sendAll($socket, $destination);
}

sub _sendAll {
    my $self = shift;
    my $socket = shift;
    my $destination = shift;
    
    if (! defined($self->{type})) {
        carp "No type defined for payload";
        return -1;
    }
    
    if (defined($destination)) {
        zmq_send($socket, $destination, -1, ZMQ_SNDMORE);
    }
    
    if (@{$self->{data}} == 0) {
        zmq_send($socket, $self->{type}, -1);
    } elsif (@{$self->{data}} == 1) {
        zmq_send($socket, $self->{type}, -1, ZMQ_SNDMORE);
        zmq_send($socket, $self->{data}->[0], -1)
    } else {
        zmq_send($socket, $self->{type}, -1, ZMQ_SNDMORE);
        
        for (my $i = 0; $i < @{$self->{data}} - 1; $i++) {
            zmq_send($socket, $self->{data}->[$i], -1, ZMQ_SNDMORE);
        }
        zmq_send($socket, $self->{data}->[-1]);
    }
}

1;
