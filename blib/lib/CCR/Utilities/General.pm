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

our $VERSION = '0.02';

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

    print FH "#!/bin/bash\n\n";
    print FH "\#SBATCH --partition=$args{partition}\n";
    print FH "\#SBATCH --time=$args{time}\n";
    print FH "\#SBATCH --nodes=$args{nodes}\n";
    print FH "\#SBATCH --mem=$args{memory}\n";
    print FH "\#SBATCH --ntasks-per-node=$args{ntasks_per_node}\n";
    print FH "\#SBATCH --job-name=$args{job_name}\n";
    print FH "\#SBATCH --dependency=afterok:$args{dependency}\n" if ( $args{dependency} ne 'none' );
    print FH "\#SBATCH --output=$args{output}\n\n";
    print FH "\#SBATCH --account=big\n";
    print FH "\#SBATCH --qos=supporters\n";
    print FH "\#SBATCH --requeue\n";
    print FH "\#SBATCH --mail-user=jw24\@buffalo.edu\n";
    print FH "\#SBATCH --mail-type=END\n";
    
    print FH "ulimit -s unlimited\n\n";

    print FH "$modules\n\n";

    print FH "echo \"Launch job\"\n";
    print FH "$args{'command'}\n\n";

    print FH "echo \"SLURM_JOBID=\"\$SLURM_JOBID\n";
    print FH "echo \"SLURM_JOB_NODELIST\"=\$SLURM_JOB_NODELIST\n";
    print FH "echo \"SLURM_NNODES\"=\$SLURM_NNODES\n";
    print FH "echo \"SLURMTMPDIR=\"\$SLURMTMPDIR\n";
    print FH "echo \"working directory = \"\$SLURM_SUBMIT_DIR\n";
    print FH "echo \"Slurm job submitted sccessfully!\"\n";
    close FH;

    # check if we want to just generate slurm scripts for testing/debuging
    if ( $args{'debug'} eq 'N' ) {

        # check if we can submit the job (max 1000 jobs)
        while (1) {
            my $num_slurm_jobs = count_slurm_jobs();

            if ( $num_slurm_jobs > 995 ) {
                sleep(60);
            }
            else {
                # submit the job
                my $rv = `sbatch $script`;
                my ($slurm_jobid) = $rv =~ /.*?(\d+)/;
                return ($slurm_jobid);
            }
        }
    } else {
        return($args{job_name});
    }

}

#############################################################
# submitting jobs with checkpointing enabled
#############################################################
sub create_submit_ckp_slurm_job {
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
            },
            exe => {
                type     => SCALAR,
                required => 1,
                default  => undef
            },
            args => {
                type     => SCALAR,
                required => 1,
                default  => undef
            },
           errorfile => {
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

    print FH "#!/bin/bash\n\n";
    print FH "\#SBATCH --partition=$args{partition}\n";
    print FH "\#SBATCH --time=$args{time}\n";
    print FH "\#SBATCH --nodes=$args{nodes}\n";
    print FH "\#SBATCH --mem=$args{memory}\n";
    print FH "\#SBATCH --ntasks-per-node=$args{ntasks_per_node}\n";
    print FH "\#SBATCH --job-name=$args{job_name}\n";
    print FH "\#SBATCH --dependency=afterok:$args{dependency}\n" if ( $args{dependency} ne 'none' );
    print FH "\#SBATCH --output=$args{output}\n\n";
    print FH "\#SBATCH --account=big\n";
    print FH "\#SBATCH --qos=supporters\n";
    print FH "\#SBATCH --requeue\n";
    print FH "\#SBATCH --mail-user=jw24\@buffalo.edu\n";
    print FH "\#SBATCH --mail-type=END\n\n";
    print FH "module load dmtcp/2.5.0\n";
    print FH "$modules\n\n";
    print FH "ulimit -s unlimited\n\n";

	print FH "CHECKPOINT_TIME=4200m\n";

    print FH "echo \"Launch job\"\n";

    print FH "EXE=\"$args{exe}\"\n\n";
	print FH "ARGS=\"$args{args}\"\n";
	print FH "OUTFILE=\"$args{output}\"\n";
	print FH "ERRFILE=\"$args{errorfile}\"\n";
	print FH "FORCE_CHECKPOINT=No\n";

    print FH "echo \"SLURM_JOBID=\"\$SLURM_JOBID\n";
    print FH "echo \"SLURM_JOB_NODELIST\"=\$SLURM_JOB_NODELIST\n";
    print FH "echo \"SLURM_NNODES\"=\$SLURM_NNODES\n";
    print FH "echo \"SLURMTMPDIR=\"\$SLURMTMPDIR\n";
    print FH "echo \"working directory = \"\$SLURM_SUBMIT_DIR\n";
    print FH "echo \"Slurm job submitted sccessfully!\"\n";
    close FH;

    # check if we want to just generate slurm scripts for testing/debuging
    if ( $args{'debug'} eq 'N' ) {

        # check if we can submit the job (max 1000 jobs)
        while (1) {
            my $num_slurm_jobs = count_slurm_jobs();

            if ( $num_slurm_jobs > 995 ) {
                sleep(60);
            }
            else {
                # submit the job
                my $rv = `sbatch $script`;
                my ($slurm_jobid) = $rv =~ /.*?(\d+)/;
                return ($slurm_jobid);
            }
        }
    } else {
        return($args{job_name});
    }

}

#####################################################################
# submitting jobs to scavenger partion wiith checkpointing enabled
#####################################################################
sub create_submit_scav_slurm_job {
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
            },
            exe => {
                type     => SCALAR,
                required => 1,
                default  => undef
            },
            args => {
                type     => SCALAR,
                required => 1,
                default  => undef
            },
           errorfile => {
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

	my $fh = new IO::File;
	$fh->open("$script","w");
	print $fh <<EOF;
#!/bin/bash
#SBATCH --time=$args{time}
#SBATCH --nodes=$args{nodes}
#SBATCH --mem=$args{memory}
#SBATCH --ntasks-per-node=$args{ntasks_per_node}
#SBATCH --job-name=$args{job_name}
EOF

if ( $args{dependency} ne 'none' ) {	
	print $fh <<EOF;
#SBATCH --dependency=afterok:$args{dependency}
EOF
}
print $fh <<EOF;
#SBATCH --account=big
#SBATCH --requeue
#SBATCH --mail-user=jw24\@buffalo.edu
#SBATCH --mail-type=END
module load dmtcp/2.5.0
$modules
ulimit -s unlimited
CHECKPOINT_TIME=60m
echo \"Launch job\"

EXE=$args{exe}
ARGS=\'$args{args}\'
OUTFILE=$args{output}
ERRFILE=$args{errorfile}

FORCE_CHECKPOINT=No

echo \"SLURM_JOBID=\"\$SLURM_JOBID
echo \"SLURM_JOB_NODELIST\"=\$SLURM_JOB_NODELIST
echo \"SLURM_NNODES\"=\$SLURM_NNODES
echo \"SLURMTMPDIR=\"\$SLURMTMPDIR
echo \"working directory = \"\$SLURM_SUBMIT_DIR
echo \"Slurm job submitted sccessfully!\"

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
        if [ -f "\$fname" ]; then
            p=`cat \$fname`
            if [ -n "\$p" ]; then
                # try to communicate ? dmtcp_command -p \$p l
                break
            fi
        fi
    done

    # Create dmtcp_command wrapper for easy communication with coordinator
    p=`cat \$fname`
    chmod +x \$fname
    echo "#!/bin/bash" > \$fname
    echo >> \$fname
    echo "export PATH=\$PATH" >> \$fname
    echo "export DMTCP_HOST=\$h" >> \$fname
    echo "export DMTCP_PORT=\$p" >> \$fname
    echo "export DMTCP_COORD_HOST=\$h" >> \$fname
    echo "export DMTCP_COORD_PORT=\$p" >> \$fname
    echo "dmtcp_command \\\$@" >> \$fname

    # Setup local environment for DMTCP
    export DMTCP_HOST=\$h
    export DMTCP_PORT=\$p
    export DMTCP_COORD_HOST=\$h
    export DMTCP_COORD_PORT=\$p
}

echo "Launching dmtcp coordintor daemon"
echo "start_coordinator --exit-after-ckpt"
start_coordinator --exit-after-ckpt

# convert checkpoint time to seconds
nTics=`echo \$CHECKPOINT_TIME | \\
sed 's/m/ \\* 60/g' | \\
sed 's/h/ \\* 3600/g' | \\
sed 's/d/ \\* 86400/g' | \\
sed 's/s//g' | \\
bc | \\
awk '{ printf("%d\\n", \$1); }'`
echo "Checkpointing will commence after \$nTics seconds"

tic=`date +%s`
if [[ -f ./dmtcp_restart_script.sh ]] && [[ "\${FORCE_CHECKPOINT}" == "No" ]]; then
  echo "Restarting application under dmtcp control"
  echo "./dmtcp_restart_script.sh -h \$DMTCP_HOST -p \$DMTCP_PORT -i \$nTics 1>>\$OUTFILE 2>>\$ERRFILE"
  ./dmtcp_restart_script.sh -h \$DMTCP_HOST -p \$DMTCP_PORT -i \$nTics 1>>\${OUTFILE}.\${SLURM_JOB_ID} 2>>\${ERRFILE}.\${SLURM_JOB_ID}
  cat \${OUTFILE}.\${SLURM_JOB_ID} >> \${OUTFILE}
  rm -f \${OUTFILE}.\${SLURM_JOB_ID}
  cat \${ERRFILE}.\${SLURM_JOB_ID} >> \${ERRFILE}
  rm -f \${ERRFILE}.\${SLURM_JOB_ID}
else
  # clear output and error files
  echo "" > \${OUTFILE}
  echo "" > \${ERRFILE}
  echo "Launching application under dmtcp control"
  echo "srun dmtcp_launch --quiet --rm -i \$nTics \$EXE \$ARGS 1>\${OUTFILE} 2>\${ERRFILE}"
  srun dmtcp_launch --quiet --rm -i \$nTics \$EXE \$ARGS 1>\${OUTFILE} 2>\${ERRFILE}
fi
toc=`date +%s`

elapsedTime=`expr \$toc - \$tic`
overheadTime=`expr \$elapsedTime - \$nTics`
if [ "\$overheadTime" -lt "0" ]; then
  overheadTime=0
  echo "All done - no checkpoint was required."
else
  echo "All done - checkpoint files are listed below:"
  ls -1 *.dmtcp
fi

echo "Elapsed Time = \$elapsedTime seconds"
echo "Checkpoint Overhead = \$overheadTime seconds"

EOF

    # check if we want to just generate slurm scripts for testing/debuging
    if ( $args{'debug'} eq 'N' ) {

        # check if we can submit the job (max 1000 jobs)
        while (1) {
            my $num_slurm_jobs = count_slurm_jobs();

            if ( $num_slurm_jobs > 995 ) {
                sleep(60);
            }
            else {
                # submit the job
                my $rv = `scabatch $script`;
                my ($slurm_jobid) = $rv =~ /.*?(\d+)/;
                return ($slurm_jobid);
            }
        }
    } else {
        return($args{job_name});
    }

}

=head2 count_slurm_jobs

=cut

sub count_slurm_jobs {
    my $username = $ENV{LOGNAME} || $ENV{USER} || getpwuid($<);
    my $num_slurm_jobs = `squeue -u $username | wc -l`;

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
