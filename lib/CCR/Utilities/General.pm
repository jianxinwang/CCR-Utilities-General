package CCR::Utilities::General;

use 5.006;
use strict;
use warnings;
use POSIX qw/strftime/;
use Carp;
use Pod::Usage;
use Data::Dumper;
use YAML qw/LoadFile/;
use File::Path;
use Params::Validate qw(:all);
use File::Temp 'tempfile';

=head1 NAME

CCR::Utilities::General - The great new CCR::Utilities::General!

=head1 VERSION

Version 0.01

=cut

our $VERSION = '0.03';

=head1 SYNOPSIS

Quick summary of what the module does.

Perhaps a little code snippet.

    use CCR::Utilities::General;

    my $foo = CCR::Utilities::General->new();
    ...

=head1 EXPORT

A list of functions that can be exported.  You can delete this section
if you don't export anything, such as for a purely object-oriented module.

=head1 SUBROUTINES/METHODS

=head2 create_submit_slurm_job, returns a slum job id

=cut

#####################################################
# submit jobs without checkpointing enabled
#####################################################
sub create_submit_slurm_job {
    my %args = validate(
        @_,
        {
            debug => {
                type     => SCALAR,
                required => 0,
                default  => 0
            },
            script => {
                type     => SCALAR,
                required => 1,
                default  => undef
            },
            command => {
                type     => SCALAR,
                required => 1,
                default  => undef
            },
            modules => {
                type     => SCALAR,
                required => 0,
                default  => undef
            },
            script_dir => {
                type     => SCALAR,
                required => 0,
                default  => '.'
            },
            partition => {
                type     => SCALAR,
                required => 0,
                default  => 'general-compute'
            },
            cluster => {
                type     => SCALAR,
                required => 0,
                default  => 'ub-hpc'
            },
            qos => {
                type     => SCALAR,
                required => 0,
                default  => 'general-compute'
            },
            email => {
                type     => SCALAR,
                required => 0,
                default  => undef
            },
            sendemail => {
                type     => SCALAR,
                required => 0,
                default  => 'N'
            },
            account => {
                type     => SCALAR,
                required => 1,
                default  => undef
            },
            time => {
                type     => SCALAR,
                required => 0,
                default  => '02:00:00'
            },
            nodes => {
                type     => SCALAR,
                required => 0,
                default  => 1
            },
            memory => {
                type     => SCALAR,
                required => 0,
                default  => 15000
            },
            ntasks_per_node => {
                type     => SCALAR,
                required => 0,
                default  => 1
            },
            job_name => {
                type     => SCALAR,
                required => 1,
                default  => undef
            },
            dependency => {
                type     => SCALAR,
                required => 1,
                default  => 'none'
            },
            output => {
                type     => SCALAR,
                required => 1,
                default  => undef
            }
        }
    );

    # create a string with each module to load
    my @list_of_modules = split(',', $args{'modules'});
	for ( my $i = 0; $i < scalar @list_of_modules; $i++ ) {
        $list_of_modules[$i] = "module load $list_of_modules[$i]";
    }

    my $modules = join( "\n", @list_of_modules );

    my $script = join( '/', $args{'script_dir'}, $args{'script'} );

    open( FH, ">$script" ) or Carp::croak("Cannot open file $script: $!");
	print FH <<EOF;
#!/bin/bash
#SBATCH --partition=$args{partition}
#SBATCH --clusters=$args{cluster}
#SBATCH --qos=$args{qos}
#SBATCH --time=$args{time}
#SBATCH --nodes=$args{nodes}
#SBATCH --mem=$args{memory}
#SBATCH --ntasks-per-node=$args{ntasks_per_node}
#SBATCH --job-name=$args{job_name}
EOF

if ($args{dependency} ne 'none' ){
	print FH <<EOF;
#SBATCH --dependency=afterok:$args{dependency}
EOF
}
print FH <<EOF;
#SBATCH --output=$args{output}
#SBATCH --account=$args{account}
#SBATCH --requeue
#SBATCH --mail-user=$args{email}

EOF
if ($args{sendemail} eq 'Y') {
	print FH <<EOF;
#SBATCH --mail-type=END
EOF
}
print FH <<EOF;
ulimit -s unlimited

$modules

echo "Launch job"
$args{'command'}

echo \"SLURM_JOBID=\"\$SLURM_JOBID
echo \"SLURM_JOB_NODELIST\"=\$SLURM_JOB_NODELIST
echo \"SLURM_NNODES\"=\$SLURM_NNODES
echo \"SLURMTMPDIR=\"\$SLURMTMPDIR
echo \"working directory = \"\$SLURM_SUBMIT_DIR
echo \"Slurm job submitted sccessfully!\"
EOF
    close FH;

   # check if we can submit the job (max 1000 jobs)
    while (1) {
        my $num_slurm_jobs = count_slurm_jobs($args{cluster});

        if ( $num_slurm_jobs > 995 ) { # CCR allows max 1000 jobs per user, so leave 5 free slots for other use
            sleep(60);
        }
        else {
            my $rv;
            # check if we want to just generate slurm scripts for testing/debuging
            if ( $args{'debug'} eq 'N' ) {
                # submit the job
                $rv = `sbatch $script`;
            } else {
                $rv = `sbatch -H $script`; # submit but hold the job
            }
            my ($slurm_jobid) = $rv =~ /.*?(\d+)/;
            return ($slurm_jobid);
        }
    }
}


=head2 count_slurm_jobs

=cut

sub count_slurm_jobs {
	my $cluster = shift;
    my $username = $ENV{LOGNAME} || $ENV{USER} || getpwuid($<);
    my $num_slurm_jobs = `squeue -u $username -M $cluster| wc -l`;

    return ($num_slurm_jobs);
}

=head1 AUTHOR

Jianxin Wang, C<< <jw24 at buffalo.edu> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-ccr-utilities-general at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=CCR-Utilities-General>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.




=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc CCR::Utilities::General


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker (report bugs here)

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=CCR-Utilities-General>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/CCR-Utilities-General>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/CCR-Utilities-General>

=item * Search CPAN

L<http://search.cpan.org/dist/CCR-Utilities-General/>

=back


=head1 ACKNOWLEDGEMENTS


=head1 LICENSE AND COPYRIGHT

Copyright 2015 Jianxin Wang.

This program is free software; you can redistribute it and/or modify it
under the terms of the the Artistic License (2.0). You may obtain a
copy of the full license at:

L<http://www.perlfoundation.org/artistic_license_2_0>

Any use, modification, and distribution of the Standard or Modified
Versions is governed by this Artistic License. By using, modifying or
distributing the Package, you accept this license. Do not use, modify,
or distribute the Package, if you do not accept this license.

If your Modified Version has been derived from a Modified Version made
by someone other than you, you are nevertheless required to ensure that
your Modified Version complies with the requirements of this license.

This license does not grant you the right to use any trademark, service
mark, tradename, or logo of the Copyright Holder.

This license includes the non-exclusive, worldwide, free-of-charge
patent license to make, have made, use, offer to sell, sell, import and
otherwise transfer the Package with respect to any patent claims
licensable by the Copyright Holder that are necessarily infringed by the
Package. If you institute patent litigation (including a cross-claim or
counterclaim) against any party alleging that the Package constitutes
direct or contributory patent infringement, then this Artistic License
to you shall terminate on the date that such litigation is filed.

Disclaimer of Warranty: THE PACKAGE IS PROVIDED BY THE COPYRIGHT HOLDER
AND CONTRIBUTORS "AS IS' AND WITHOUT ANY EXPRESS OR IMPLIED WARRANTIES.
THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
PURPOSE, OR NON-INFRINGEMENT ARE DISCLAIMED TO THE EXTENT PERMITTED BY
YOUR LOCAL LAW. UNLESS REQUIRED BY LAW, NO COPYRIGHT HOLDER OR
CONTRIBUTOR WILL BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, OR
CONSEQUENTIAL DAMAGES ARISING IN ANY WAY OUT OF THE USE OF THE PACKAGE,
EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


=cut

1;    # End of CCR::Utilities::General
