use 5.008008;
use strict;
use warnings;
use ExtUtils::MakeMaker;
use Getopt::Long;
use File::Spec;
use Config;

my $range_check;
my $math_int64;

GetOptions(
    'range-check!' => \$range_check,
    'math-int64!'  => \$math_int64,
);

my $define = join ' ',
    $range_check ? '-DWITH_RANGE_CHECK' : (),
    $math_int64 ? '-DWITH_MATH_INT64' : ();

my $tarantoolboxdir = '../box';
my $iprotoxsso = eval {
    require MR::IProto::XS;
    my $path = $INC{'MR/IProto/XS.pm'};
    $path =~ s/\/MR\/IProto\/XS\.pm$/\/auto\/MR\/IProto\/XS\/XS.so/;
    $path;
} || '../../xs/blib/arch/auto/MR/IProto/XS/XS.so';

$iprotoxsso = File::Spec->rel2abs($iprotoxsso);

my $inc = $math_int64 ? "-I$Config{vendorarch}/auto/Math/Int64/include" : '';
my $mathint64a = $math_int64 ? "$Config{vendorarch}/auto/Math/Int64/libperl_math_int64.a" : '';

WriteMakefile(
    NAME              => 'MR::Tarantool::Box::XS',
    VERSION_FROM      => 'lib/MR/Tarantool/Box/XS.pm', # finds $VERSION
    PREREQ_PM         => {}, # e.g., Module::Name => 1.1
    ($] >= 5.005 ?     ## Add these new keywords supported since 5.005
      (ABSTRACT_FROM  => 'lib/MR/Tarantool/Box/XS.pm', # retrieve abstract from module
       AUTHOR         => 'a.mashanov <a.mashanov@localdomain>') : ()),
    LIBS              => ['-ltarantoolbox'], # e.g., '-lm'
    DEFINE            => $define, # e.g., '-DHAVE_SOMETHING'
    INC               => $inc,
	# Un-comment this if you add C files to link with later:
    # OBJECT            => '$(O_FILES)', # link all the C files too
    CCFLAGS           => '-std=gnu99 -Wall -Werror',
    OPTIMIZE          => '-ggdb3',
    LDFROM            => "\$(OBJECT) $iprotoxsso $mathint64a",
);
