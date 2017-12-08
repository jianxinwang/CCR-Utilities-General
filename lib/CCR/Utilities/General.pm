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
       		exe => {
                type     => SCALAR,
                required => 0,
                default  => undef
            },
       		arg => {
                type     => SCALAR,
                required => 0,
                default  => undef
            },
       		errorfile => {
                type     => SCALAR,
                required => 0,
                default  => undef
            },
	        logfile => {
                type     => SCALAR,
                required => 0,
                default  => undef
            },
            outfile=> {
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
	my $clusters = 'ub-hpc';
	my $qos = 'ub-hpc';
    my $script = join( '/', $args{'script_dir'}, $args{'script'} );
	my $email = $args{email} .'@buffalo.edu';
    open( FH, ">$script" ) or Carp::croak("Cannot open file $script: $!");

	if ($args{partition} eq 'general-compute' or $args{partition} eq 'industry') {
		my $command;
		if (defined $args{'exe'} and $args{'arg'} ){
			$command = $args{'exe'} .' '. $args{'arg'} .' >  '. $args{'outfile'};
		} else {
			$command = $args{'command'};
		}
		if ($args{partition} eq 'industry') {
			$clusters = 'industry';
			$qos = 'industry';
		}
		print FH <<EOF;
#!/bin/bash
#SBATCH --partition=$args{partition}
#SBATCH --clusters=$clusters
#SBATCH --qos=$qos
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
#SBATCH --output=$args{logfile}
#SBATCH --account=$args{account}
#SBATCH --requeue
#SBATCH --mail-user=$email

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
$command

echo \"SLURM_JOBID=\"\$SLURM_JOBID
echo \"SLURM_JOB_NODELIST\"=\$SLURM_JOB_NODELIST
echo \"SLURM_NNODES\"=\$SLURM_NNODES
echo \"SLURMTMPDIR=\"\$SLURMTMPDIR
echo \"working directory = \"\$SLURM_SUBMIT_DIR
echo \"Slurm job submitted sccessfully!\"
EOF
	} elsif ($args{partition} eq 'scavenger') {
		print FH <<EOF;
#!/bin/bash
#SBATCH --time=$args{time}
#SBATCH --nodes=$args{nodes}
#SBATCH --cpus-per-task=4
#SBATCH --mem=$args{memory}
#SBATCH --ntasks-per-node=$args{ntasks_per_node}
#SBATCH --job-name=$args{job_name}
#SBATCH --output=$args{logfile}
##SBATCH --constraint=IB&CPU-E5-2650v2
EOF

		if ($args{dependency} ne 'none' ){
			print FH <<EOF;
#SBATCH --dependency=afterok:$args{dependency}
EOF
		}
		print FH <<EOF;
#SBATCH --account=$args{account}
#SBATCH --requeue
#SBATCH --mail-user=$email
EOF
		if ($args{sendemail} eq 'Y') {
			print FH "#SBATCH --mail-type=END";
		}
		print FH <<EOF;

echo \"SLURM_JOBID=\"\$SLURM_JOBID
echo \"SLURM_JOB_NODELIST\"=\$SLURM_JOB_NODELIST
echo \"SLURM_NNODES\"=\$SLURM_NNODES
echo \"SLURMTMPDIR=\"\$SLURMTMPDIR
echo \"working directory = \"\$SLURM_SUBMIT_DIR

$modules
module load dmtcp/2.5.0
ulimit -s unlimited

#
# How long to run the application before checkpointing.
# After checkpointing, the application will be shut down. 
# Users will typically want to set this to occur a bit before 
# the job's walltime expires.
#
CHECKPOINT_TIME=30m

# EXE is the name of the application/executable
# ARGS is any command-line args
# OUTFILE is the file where stdout will be redirected
# ERRFILE if the file where stderr will be redirected
EXE=$args{'exe'}
ARGS=$args{'arg'}
OUTFILE=$args{'outfile'}
ERRFILE=$args{'errorfile'}

# This script with auto-sense whether to perform a checkpoint
# or restart operation. Set FORCE_CHECKPOINT to yes if you 
# DO NOT want to restart even if a restart script is located 
# in the working directory.
FORCE_CHECKPOINT=No

# *************************************************************************************************
# *************************************************************************************************
# Users should not have to change anything beyond this point!
# *************************************************************************************************
# *************************************************************************************************
export DMTCP_TMPDIR=\$SLURM_SUBMIT_DIR

# =================================================================================================
# start_coordinator() 
#   Routine provided by Artem Polyakov
#
# Start dmtcp coordinator on launching node. Free TCP port is automatically allocated.
# this function creates dmtcp_command.\$JOBID script that serves like a wrapper around
# dmtcp_command that tunes it on exact dmtcp_coordinator (it's host name and port)
# instead of typing "dmtcp_command -h <coordinator host name> -p <coordinator port> <command>"
# you just type "dmtcp_command.\$JOBID <command>" and talk to coordinator of JOBID job
# =================================================================================================
start_coordinator()
{
    fname=dmtcp_command.\$SLURM_JOBID
    h=`hostname -s`
    echo "dmtcp_coordinator --daemon --exit-on-last -p 0 --port-file \$fname \$\@ 1>/dev/null 2>&1"
    dmtcp_coordinator --daemon --exit-on-last -p 0 --port-file \$fname \$\@ 1>/dev/null 2>&1
    
    while true; do 
        if [ -f \"\$fname\" ]; then
            p=`cat \$fname`
            if [ -n \"\$p\" ]; then
                # try to communicate ? dmtcp_command -p \$p l
                break
            fi
        fi
    done
    
    # Create dmtcp_command wrapper for easy communication with coordinator
    p=`cat \$fname`
    chmod +x \$fname
    echo \"#!/bin/bash\" > \$fname
    echo >> \$fname
    echo \"export PATH=\$PATH" >> \$fname
    echo \"export DMTCP_HOST=\$h\" >> \$fname
    echo \"export DMTCP_PORT=\$p\" >> \$fname
    echo \"export DMTCP_COORD_HOST=\$h\" >> \$fname
    echo \"export DMTCP_COORD_PORT=\$p\" >> \$fname
    echo \"dmtcp_command \$\@\" >> \$fname

    # Setup local environment for DMTCP
    export DMTCP_HOST=\$h
    export DMTCP_PORT=\$p
    export DMTCP_COORD_HOST=\$h
    export DMTCP_COORD_PORT=\$p
}

echo "Launching dmtcp coordintor daemon"
echo "start_coordinator"
start_coordinator 

# convert checkpoint time to seconds
nTics=`echo \$CHECKPOINT_TIME | \
sed 's/m/ \* 60/g' | \
sed 's/h/ \* 3600/g' | \
sed 's/d/ \* 86400/g' | \
sed 's/s//g' | \
bc | \
awk '{ printf(\"\%d\\n\", \$1); }'`
echo "Checkpointing will commence after \$nTics seconds"

tic=`date +%s`
if [[ -f ./dmtcp_restart_script.sh ]] && [[ \"\${FORCE_CHECKPOINT}\" == \"No\" ]]; then
  echo \"Restarting application under dmtcp control\"
  echo \"./dmtcp_restart_script.sh -h \$DMTCP_HOST -p \$DMTCP_PORT -i \$nTics 1>>\$OUTFILE 2>>\$ERRFILE\"
  ./dmtcp_restart_script.sh -h \$DMTCP_HOST -p \$DMTCP_PORT -i \$nTics 1>>\${OUTFILE}.\${SLURM_JOB_ID} 2>>\${ERRFILE}.\${SLURM_JOB_ID}
  cat \${OUTFILE}.\${SLURM_JOB_ID} >> \${OUTFILE}
  rm -f \${OUTFILE}.\${SLURM_JOB_ID}
  cat \${ERRFILE}.\${SLURM_JOB_ID} >> \${ERRFILE}
  rm -f \${ERRFILE}.\${SLURM_JOB_ID}
else
  # clear output and error files
  echo \"\" > \${OUTFILE}
  echo \"\" > \${ERRFILE}
  echo \"Launching application under dmtcp control\"
  echo \"srun dmtcp_launch --quiet --rm -i \$nTics \$EXE \$ARGS 1>\${OUTFILE} 2>\${ERRFILE}\"
  srun dmtcp_launch --quiet --rm -i \$nTics \$EXE \$ARGS 1>\${OUTFILE} 2>\${ERRFILE}
fi
toc=`date +%s`

elapsedTime=`expr \$toc - \$tic`
overheadTime=`expr \$elapsedTime - \$nTics`
if [ \"\$overheadTime" -lt \"0\" ]; then
  overheadTime=0
  echo \"All done - no checkpoint was required."
else
  echo "All done - checkpoint files are listed below:"
  ls -1 *.dmtcp
fi

echo "Elapsed Time = \$elapsedTime seconds"
echo "Checkpoint Overhead = \$overheadTime seconds"

EOF
	}

    close FH;
    # check if we can submit the job (max 1000 jobs)
    while (1) {
        my $num_slurm_jobs = count_slurm_jobs($clusters);

        if ( $num_slurm_jobs > 995 ) { # CCR allows max 1000 jobs per user, so leave 5 free slots for other use
            sleep(60);
        }
        else {
            my $rv;
            # check if we want to just generate slurm scripts for testing/debuging
            if ( $args{'debug'} eq 'N' ) {
                # submit the job
                if ($args{partition} eq 'general-compute' || $args{partition} eq 'industry'){
                	$rv = `sbatch $script`;
				} elsif ($args{partition} eq 'scavenger') {
					while (1) {
                        my $tmp = `scabatch $script`;
                        ($rv) = $tmp =~ /.*?job (\d+) .*/ms;
                        last if (defined $rv);
                        sleep(60);
                    }
				}
            } else {
				if ($args{partition} eq 'general-compute' || $args{partition} eq 'industry'){
                	$rv = `sbatch -H $script`; # submit but hold the job
				} elsif ($args{partition} eq 'scavenger') {
					while (1) {
						my $tmp = `scabatch $script`;
						($rv) = $tmp =~ /.*?job (\d+) .*/ms;
						last if (defined $rv);
						sleep(60);
					}
				}
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

