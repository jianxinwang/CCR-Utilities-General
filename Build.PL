use 5.006;
use strict;
use warnings;
use Module::Build;

my $builder = Module::Build->new(
    module_name         => 'CCR::Utilities::General',
    license             => 'artistic_2',
    dist_author         => q{Jianxin Wang <jw24@buffalo.edu>},
    dist_version_from   => 'lib/CCR/Utilities/General.pm',
    release_status      => 'stable',
    configure_requires => {
        'Module::Build' => 0,
    },
    build_requires => {
        'Test::More' => 0,
    },
    requires => {
        #'ABC'              => 1.6,
        #'Foo::Bar::Module' => 5.0401,
    },
    add_to_cleanup     => [ 'CCR-Utilities-General-*' ],
    create_makefile_pl => 'traditional',
);

$builder->create_build_script();
