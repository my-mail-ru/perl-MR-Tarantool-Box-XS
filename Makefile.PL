use 5.008008;
use strict;
use warnings;
use ExtUtils::MakeMaker;
use Getopt::Long;
use File::Spec;

my $iprotodir = '/home/a.mashanov/octopus/client/libiproto'; # FIXME
my $iprotosharddir = '../../shard';
my $iprotoxsdir = '../../xs';
my $tarantoolboxdir = '../box';
my $iprotoxsso = '../../xs/blib/arch/auto/MR/IProto/XS/XS.so';

GetOptions(
    'iprotodir=s'      => \$iprotodir,
    'iprotosharddir=s' => \$iprotosharddir,
    'iprotoxsdir=s'    => \$iprotoxsdir,
    'tarntoolboxdir=s' => \$tarantoolboxdir,
);
die "--iprotodir is required" unless $iprotodir;
$tarantoolboxdir = File::Spec->rel2abs($tarantoolboxdir);
$iprotoxsso = File::Spec->rel2abs($iprotoxsso);

my $TARANTOOLBOX_BUILDDIR = 'build-tarantoolbox';

my $cmakeopts = '';
system("mkdir -p $TARANTOOLBOX_BUILDDIR && cd $TARANTOOLBOX_BUILDDIR && cmake $cmakeopts $tarantoolboxdir")
    and die "Failed to 'cmake tarantoolbox'\n";

WriteMakefile(
    NAME              => 'MR::Tarantool::Box::XS',
    VERSION_FROM      => 'lib/MR/Tarantool/Box/XS.pm', # finds $VERSION
    PREREQ_PM         => {}, # e.g., Module::Name => 1.1
    ($] >= 5.005 ?     ## Add these new keywords supported since 5.005
      (ABSTRACT_FROM  => 'lib/MR/Tarantool/Box/XS.pm', # retrieve abstract from module
       AUTHOR         => 'a.mashanov <a.mashanov@localdomain>') : ()),
    LIBS              => [''], # e.g., '-lm'
    DEFINE            => '', # e.g., '-DHAVE_SOMETHING'
    INC               => "-I$tarantoolboxdir -I$iprotosharddir -I$iprotodir -I$iprotoxsdir",
	# Un-comment this if you add C files to link with later:
    # OBJECT            => '$(O_FILES)', # link all the C files too
    CCFLAGS           => '-std=gnu99 -Wall -Werror',
    OPTIMIZE          => '-g',
    DIR               => [$TARANTOOLBOX_BUILDDIR],
    LDFROM            => "\$(OBJECT) $TARANTOOLBOX_BUILDDIR/libtarantoolbox.a $iprotoxsso",
    depend            => {
        'XS.c' => "$TARANTOOLBOX_BUILDDIR/libtarantoolbox.a",
        "$TARANTOOLBOX_BUILDDIR/libtarantoolbox.a" => 'subdirs',
    },
    clean             => { FILES => $TARANTOOLBOX_BUILDDIR },
);
