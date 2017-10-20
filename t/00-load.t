#!perl -T
use 5.006;
use strict;
use warnings;
use Test::More;

plan tests => 1;

BEGIN {
    use_ok( 'CCR::Utilities::General' ) || print "Bail out!\n";
}

diag( "Testing CCR::Utilities::General $CCR::Utilities::General::VERSION, Perl $], $^X" );
