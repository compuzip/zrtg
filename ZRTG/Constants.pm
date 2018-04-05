package ZRTG::Constants;
use strict;
use base qw(Exporter);
use Carp ();

my %constants;
BEGIN {
    %constants  = (
        # Message types
        ZRTG_PING                   => 30,
        ZRTG_READY                  => 31,
        ZRTG_SNMP_SMART_REQUEST     => 40,
        ZRTG_SNMP_RESPONSE          => 51,
        ZRTG_REFRESH_INTERFACES     => 60,
        ZRTG_INTERFACE_RESPONSE     => 61,
        ZRTG_TAKEOVER_REQUEST       => 70,
        ZRTG_TAKEOVER_RESPONSE      => 71,
        
        # SNMP OIDs
        NUM_INTERFACES_OID          => '.1.3.6.1.2.1.2.1.0',
        INTERFACE_HIGHSPEED_OID     => '.1.3.6.1.2.1.31.1.1.1.15',
        INTERFACE_NAMES_OID         => '.1.3.6.1.2.1.31.1.1.1.1',
        INTERFACE_ALIASES_OID       => '.1.3.6.1.2.1.31.1.1.1.18',
        INTERFACE_ADMIN_STATUS_OID  => '.1.3.6.1.2.1.2.2.1.7',
        INTERFACE_OPER_STATUS_OID   => '.1.3.6.1.2.1.2.2.1.8',
    );
}

use constant \%constants;
our @EXPORT;
our @EXPORT_OK = keys %constants;
our %EXPORT_TAGS = (
    all => [ qw(
        ZRTG_PING
        ZRTG_READY
        ZRTG_SNMP_SMART_REQUEST
        ZRTG_SNMP_RESPONSE
        ZRTG_REFRESH_INTERFACES
        ZRTG_INTERFACE_RESPONSE
        ZRTG_TAKEOVER_REQUEST
        ZRTG_TAKEOVER_RESPONSE
        
        NUM_INTERFACES_OID
        INTERFACE_HIGHSPEED_OID
        INTERFACE_NAMES_OID
        INTERFACE_ALIASES_OID
        INTERFACE_ADMIN_STATUS_OID
        INTERFACE_OPER_STATUS_OID
    ) ]
);

$EXPORT_TAGS{all} = [ @EXPORT_OK ];

1;
