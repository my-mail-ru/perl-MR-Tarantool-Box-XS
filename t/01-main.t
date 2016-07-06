use strict;
use warnings;
use Test::More tests => 122;
use Test::LeakTrace;
use Perl::Destruct::Level level => 2;
use Getopt::Long;
use MR::IProto::XS;
BEGIN { use_ok('MR::Tarantool::Box::XS') };
use Encode;
use AnyEvent;
use utf8;

my ($range_check, $math_int64, $cp1251, $ev, $coro);
BEGIN {
    GetOptions(
        'range-check!'  => \$range_check,
        'math-int64!'   => \$math_int64,
        'cp1251!'       => \$cp1251,
        'ev!'           => \$ev,
        'coro!'         => \$coro,
    );
    if ($coro) {
        import MR::IProto::XS 'coro';
    } elsif ($ev) {
        import MR::IProto::XS 'ev';
    }
}

my $TEST_ID = 1999999999;
my $TEST2_ID = 1999999998;
my $TEST3_ID = 1999999997;

Coro::async(sub { AnyEvent->condvar->recv() }) if $coro;

MR::IProto::XS->set_logmask(MR::IProto::XS::LOG_NOTHING);
MR::Tarantool::Box::XS->set_logmask(MR::Tarantool::Box::XS::LOG_NOTHING);
my $shard_iproto = MR::IProto::XS->new(masters => [$ENV{SHARD_SERVER}]);

my $ns = MR::Tarantool::Box::XS->new(
    iproto    => $shard_iproto,
    namespace => 22,
    format    => 'lLLll &&&&& L&l & LL&lLlLLl&L&',
    fields    => [qw/ ID Bi1 Bi2 F3 F4 F5 F6 F7 F8 SubStr F10 F11 F12 F13 Bits F15 F16 F17 F18 F19 F20 F21 F22 F23 F24 F25 /],
    indexes   => [ { name => 'id', keys => ['ID'] }, { name => 'bi', keys => ['Bi1', 'Bi2'] } ],
);
isa_ok($ns, 'MR::Tarantool::Box::XS');

my $dispatch_iproto = MR::IProto::XS->new(masters => [$ENV{DISPATCH_SERVER}]);
my $dispatch = MR::Tarantool::Box::XS->new(
    iproto    => $dispatch_iproto,
    namespace => 0,
    format    => 'l& &S&L',
    fields    => [qw/ID Email D2 D3 D4 D5/],
    indexes   => [
        { name => 'primary_id', keys => ['ID'], default => 1 },
        { name => 'primary_email', keys => ['Email'] },
    ],
);

my $lua_iproto = MR::IProto::XS->new(masters => [$ENV{SHARD_SERVER}]);
my $echo = MR::Tarantool::Box::XS::Function->new(
    iproto     => $lua_iproto,
    name       => 'client_autotest.echo',
    in_format  => 'L$C',
    in_fields  => [ 'one', 'two', 'three' ],
    out_format => 'L$C',
    out_fields => [ 'one', 'two', 'three' ],
);
isa_ok($echo, 'MR::Tarantool::Box::XS::Function');

{
    package Test::IProto;
    use base 'MR::IProto::XS';
}

{
    package Test::JUData;
    use base 'MR::Tarantool::Box::XS';
}

{
    package Test::Function;
    use base 'MR::Tarantool::Box::XS::Function';
}

check_select();
check_insert();
check_update();
check_delete();
check_call();
check_multicluster();
check_pack();
check_singleton();
check_info();
check_async();
check_leak();

sub check_select {
    my $resp = $dispatch->do({
        type    => 'select',
        keys    => [1000011658, 1000011659],
    });
    is($resp->{tuples}->[0]->{ID}, 1000011658, "select do");

    $resp = $dispatch->bulk([{
        type    => 'select',
        keys    => [1000011658, 1000011659],
    }]);
    is($resp->[0]->{tuples}->[0]->{ID}, 1000011658, "select list by id 1");
    is($resp->[0]->{tuples}->[1]->{ID}, 1000011659, "select list by id 2");

    $resp = $dispatch->bulk([{
        type    => 'select',
        keys    => [1000011658, 1000011659],
        hash_by => 'ID',
    }]);
    is($resp->[0]->{tuples}->{1000011658}->{ID}, 1000011658, "select hash by id");

    $resp = $dispatch->bulk([{
        type    => 'select',
        keys    => [1000011658, 1000011659],
        hash_by => 'Email',
    }]);
    is($resp->[0]->{tuples}->{'kadabra100@mail.ru'}->{ID}, 1000011658, "select hash by email");

    $resp = $dispatch->bulk([{
        type      => 'select',
        use_index => 'primary_email',
        keys      => ['kadabra100@mail.ru', 'kadabra101@mail.ru'],
    }]);
    is($resp->[0]->{tuples}->[0]->{ID}, 1000011658, "select list by email");

    $resp = $dispatch->bulk([{
        type    => 'select',
        keys    => [ [ 1000011658 ], { ID => 1000011659 } ],
    }]);
    is($resp->[0]->{tuples}->[0]->{ID}, 1000011658, "select by key in array");
    is($resp->[0]->{tuples}->[1]->{ID}, 1000011659, "select by key in hash");

    $resp = $ns->do({
        type      => 'select',
        use_index => 'bi',
        keys      => [ [ 1, 2 ] ],
        limit     => 5,
    });
    is(scalar @{$resp->{tuples}}, 5, "select by multipart key - limit");
    is($resp->{tuples}->[0]->{Bi1}, 1, "select by multipart key 1");
    is($resp->{tuples}->[0]->{Bi2}, 2, "select by multipart key 2");

    $resp = $ns->do({
        type      => 'select',
        use_index => 'bi',
        keys      => [ [ 2 ] ],
        limit     => 15,
    });
    is(scalar @{$resp->{tuples}}, 15, "select by part of multipart key - limit");
    is(grep({ $_->{Bi1} == 2 } @{$resp->{tuples}}), scalar @{$resp->{tuples}}, "select by part of multipart key 1");

    check_replica_flag();
    return;
}

sub check_replica_flag {
    my $iproto = MR::IProto::XS->new(masters => ['10.0.0.1:9999'], replicas => [$ENV{DISPATCH_SERVER}]);
    my $box = MR::Tarantool::Box::XS->new(
        iproto    => $iproto,
        namespace => 0,
        format    => 'l& &S&L',
        fields    => [qw/ID Email D2 D3 D4 D5/],
        indexes   => [
            { name => 'primary_id', keys => ['ID'], default => 1 },
            { name => 'primary_email', keys => ['Email'] },
        ],
    );
    my $resp = $box->do({ type => 'select', keys => [ 1000011658 ] });
    ok($resp->{replica}, "replica flag set");
    $resp = $dispatch->do({ type => 'select', keys => [ 1000011658 ] });
    ok(!$resp->{replica}, "replica flag unset");
    return;
}

sub check_insert {
    my $tuple = {
        ID => $TEST_ID,
        map { $_ => 0 } qw/ Bi1 Bi2 F3 F4 F5 F6 F7 F8 SubStr F10 F11 F12 F13 Bits F15 F16 F17 F18 F19 F20 F21 F22 F23 F24 F25 /
    };
    my $resp = $ns->bulk([{
        type => 'insert',
        tuple => $tuple,
    }]);
    is($resp->[0]->{tuple}, 1, "insert");
    $resp = $ns->bulk([{
        type => 'insert',
        tuple => $tuple,
        want_result => 1,
    }]);
    is($resp->[0]->{tuple}->{ID}, $TEST_ID, "insert want_result");
}

sub check_update {
    my $resp = $ns->do({
        type => 'update',
        key  => $TEST_ID,
        ops  => [ [ Bi2 => set => 10 ] ],
    });
    is($resp->{tuple}, 1, "update");

    $resp = $ns->do({
        type => 'update',
        key  => $TEST_ID,
        ops  => [ [ Bi2 => set => 16 ], [ Bi2 => add => 4 ] ],
        want_result => 1,
    });
    is($resp->{tuple}->{Bi2}, 20, "update add");

    $resp = $ns->do({
        type => 'update',
        key  => $TEST_ID,
        ops  => [ [ Bi2 => set => 16 ], [ Bi2 => add => -4 ] ],
        want_result => 1,
    });
    is($resp->{tuple}->{Bi2}, 12, "update add negative");

    $resp = $ns->do({
        type => 'update',
        key  => $TEST_ID,
        ops  => [ [ Bits => set => 0xff010402 ], [ Bits => and => 0x81000102 ] ],
        want_result => 1,
    });
    is($resp->{tuple}->{Bits}, 0x81000002, "update and");

    $resp = $ns->do({
        type => 'update',
        key  => $TEST_ID,
        ops  => [ [ Bits => set => 0xff010402 ], [ Bits => xor => 0x81000102 ] ],
        want_result => 1,
    });
    is($resp->{tuple}->{Bits}, 0x7e010500, "update xor");

    $resp = $ns->do({
        type => 'update',
        key  => $TEST_ID,
        ops  => [ [ Bits => set => 0xff010402 ], [ Bits => or => 0x10000201 ] ],
        want_result => 1,
    });
    is($resp->{tuple}->{Bits}, 0xff010603, "update or");

    $resp = $ns->do({
        type => 'update',
        key  => $TEST_ID,
        ops  => [ [ Bits => set => 0xff010402 ], [ Bits => bit_set => 0x10000201 ] ],
        want_result => 1,
    });
    is($resp->{tuple}->{Bits}, 0xff010603, "update bit_set");

    $resp = $ns->do({
        type => 'update',
        key  => $TEST_ID,
        ops  => [ [ Bits => set => 0xff010402 ], [ Bits => bit_clear => 0x81000102 ] ],
        want_result => 1,
    });
    is($resp->{tuple}->{Bits}, 0x7e010400, "update bit_clear");

    $resp = $ns->do({
        type => 'update',
        key  => $TEST_ID,
        ops  => [ [ Bits => set => 0xff010402 ], [ Bits => bit_set => -1 ] ],
        want_result => 1,
    });
    is($resp->{tuple}->{Bits}, 0xffffffff, "update bit_set negative");

    $resp = $ns->do({
        type => 'update',
        key  => $TEST_ID,
        ops  => [ [ Bits => set => 0xff010402 ], [ Bits => bit_clear => -1 ] ],
        want_result => 1,
    });
    is($resp->{tuple}->{Bits}, 0, "update bit_clear negative");

    $resp = $ns->do({
        type => 'update',
        key  => $TEST_ID,
        ops  => [ [ Bi2 => set => 16 ], [ Bi2 => num_add => 4 ] ],
        want_result => 1,
    });
    is($resp->{tuple}->{Bi2}, 20, "update num_add");

    $resp = $ns->do({
        type => 'update',
        key  => $TEST_ID,
        ops  => [ [ Bi2 => set => 16 ], [ Bi2 => num_sub => 4 ] ],
        want_result => 1,
    });
    is($resp->{tuple}->{Bi2}, 12, "update num_sub");

    $resp = $ns->do({
        type => 'update',
        key  => $TEST_ID,
        ops  => [ [ Bi2 => set => 16 ], [ Bi2 => num_add => -4 ] ],
        want_result => 1,
    });
    is($resp->{tuple}->{Bi2}, 12, "update num_add negative");

    $resp = $ns->do({
        type => 'update',
        key  => $TEST_ID,
        ops  => [ [ Bi2 => set => 16 ], [ Bi2 => num_sub => -4 ] ],
        want_result => 1,
    });
    is($resp->{tuple}->{Bi2}, 20, "update num_sub negative");

    $resp = $ns->do({
        type => 'update',
        key  => $TEST_ID,
        ops  => [ [ Bi2 => set => 16 ], [ Bi2 => or => 1 ] ],
        want_result => 1,
    });
    is($resp->{tuple}->{Bi2}, 17, "update 2 ops");

    $resp = $ns->do({
        type => 'update',
        key  => $TEST_ID,
        ops  => [ [ SubStr => set => "ABCDEFGHI" ], [ SubStr => splice => [ 3, 3, "XXXXX" ] ] ],
        want_result => 1,
    });
    is($resp->{tuple}->{SubStr}, "ABCXXXXXGHI", "update splice");

    $resp = $ns->do({
        type => 'update',
        key  => $TEST_ID,
        ops  => [ [ SubStr => set => "ABCDEFGHI" ], [ SubStr => splice => [ 3, 3 ] ] ],
        want_result => 1,
    });
    is($resp->{tuple}->{SubStr}, "ABCGHI", "update splice no string");

    $resp = $ns->do({
        type => 'update',
        key  => $TEST_ID,
        ops  => [ [ SubStr => set => "ABCDEFGHI" ], [ SubStr => splice => [ 3 ] ] ],
        want_result => 1,
    });
    is($resp->{tuple}->{SubStr}, "ABC", "update splice no length");

    $resp = $ns->do({
        type => 'update',
        key  => $TEST_ID,
        ops  => [ [ SubStr => set => "ABCDEFGHI" ], [ SubStr => splice => [ ] ] ],
        want_result => 1,
    });
    is($resp->{tuple}->{SubStr}, "", "update splice no offset");

    $resp = $ns->do({
        type => 'update',
        key  => $TEST_ID,
        ops  => [ [ SubStr => set => "ABCDEFGHI" ], [ SubStr => append => "XXX" ] ],
        want_result => 1,
    });
    is($resp->{tuple}->{SubStr}, "ABCDEFGHIXXX", "update append");

    $resp = $ns->do({
        type => 'update',
        key  => $TEST_ID,
        ops  => [ [ SubStr => set => "ABCDEFGHI" ], [ SubStr => prepend => "XXX" ] ],
        want_result => 1,
    });
    is($resp->{tuple}->{SubStr}, "XXXABCDEFGHI", "update prepend");

    $resp = $ns->do({
        type => 'update',
        key  => $TEST_ID,
        ops  => [ [ SubStr => set => "ABCDEFGHI" ], [ SubStr => cutbeg => 3 ] ],
        want_result => 1,
    });
    is($resp->{tuple}->{SubStr}, "DEFGHI", "update cutbeg");

    $resp = $ns->do({
        type => 'update',
        key  => $TEST_ID,
        ops  => [ [ SubStr => set => "ABCDEFGHI" ], [ SubStr => cutend => 3 ] ],
        want_result => 1,
    });
    is($resp->{tuple}->{SubStr}, "ABCDEF", "update cutend");

    $resp = $ns->do({
        type => 'update',
        key  => $TEST_ID,
        ops  => [ [ SubStr => set => "ABCDEFGHI" ], [ SubStr => substr => [ 3, 3, "XXXXX" ] ] ],
        want_result => 1,
    });
    is($resp->{tuple}->{SubStr}, "ABCXXXXXGHI", "update substr");

    $resp = $ns->do({
        type => 'update',
        key  => $TEST_ID,
        ops  => [ [ 26 => insert => "Trololo" ] ],
        want_result => 1,
    });
    is($resp->{tuple}->{extra_fields}->[0], "Trololo", "update insert");

    $resp = $ns->do({
        type => 'update',
        key  => $TEST_ID,
        ops  => [ [ 26 => 'delete' ] ],
        want_result => 1,
    });
    is($resp->{tuple}->{extra_fields}, undef, "update delete");

    return;
}

sub check_delete {
    my $resp = $ns->bulk([{
        type => 'delete',
        key  => $TEST_ID,
    }]);
    is($resp->[0]->{tuple}, 1, "delete");
    return;
}

sub check_call {
    my $function = $echo;
    my %tuple = ( one => 1, two => 'two', three => 3 );
    my @tuple = ( 1, 'two', 3 );
    my $resp = $function->bulk([
        {
            type  => 'call',
            tuple => \%tuple,
        },
        {
            type  => 'call',
            tuple => \@tuple,
        },
        {
            type  => 'call',
            tuple => \@tuple,
            raw   => 1,
        },
    ]);
    is_deeply($resp->[0]->{tuples}, [ \%tuple ], "call function with named parameters and named result");
    is_deeply($resp->[1]->{tuples}, [ \%tuple ], "call function with positional parameters and named result");
    is_deeply($resp->[2]->{tuples}, [ \@tuple ], "call function with positional parameters and positional result");

    $function = MR::Tarantool::Box::XS::Function->new(
        iproto     => $lua_iproto,
        name       => 'client_autotest.complex',
        in_format  => 'CSLQ$',
        in_fields  => [qw/ uint8 uint16 uint32 uint64 string /],
        out_format => [ '$$L', 'S$', '$L', '$L' ],
        out_fields => [ [ '1st', '2nd', '3rd' ], [ 'num', 'str' ], undef, [ 'key', 'val' ] ],
    );
    $resp = $function->do({
        type  => 'call',
        tuple => [ 8, 16, 32, 64, 'string' ],
    });
    cmp_ok($resp->{error}, '==', MR::Tarantool::Box::XS::ERR_CODE_OK, "call function: pack parameters");
    my @tuples = ( { '1st' => 'one', '2nd' => 'two', '3rd' => 10 }, { 'str' => 'two', 'num' => 16 }, [ 'three', 3 ], { 'val' => 4, 'key' => 'four' }, { 'val' => 5, 'key' => 'five' }, { 'val' => 6, 'key' => 'six' } );
    is_deeply($resp->{tuples}, \@tuples, "call function: unpack parameters");
    return;
}

sub check_multicluster {
    my $resp = MR::Tarantool::Box::XS->do({
        namespace => $dispatch,
        type      => 'select',
        keys      => [1000011658, 1000011659],
    });
    is($resp->{tuples}->[0]->{ID}, 1000011658, "namespace's do as a class method");

    my %tuple = ( one => 1, two => 'two', three => 3 );
    $resp = MR::Tarantool::Box::XS->do({
        function => $echo,
        type     => 'call',
        tuple    => \%tuple,
    });
    is_deeply($resp->{tuples}, [ \%tuple ], "function's do as a class method");

    $resp = MR::Tarantool::Box::XS->bulk([
        {
            namespace => $ns,
            type      => 'select',
            keys      => [1000011658, 1000011659],
        },
        {
            namespace => $dispatch,
            type      => 'select',
            keys      => [1000011658, 1000011659],
        },
        {
            function => $echo,
            type     => 'call',
            tuple    => \%tuple,
        }
    ]);
    ok($resp->[0]->{tuples}->[0]->{ID} == 1000011658 && $resp->[1]->{tuples}->[0]->{ID} == 1000011658
        && exists $resp->[0]->{tuples}->[0]->{F11} && exists $resp->[1]->{tuples}->[0]->{D2}, "select from more then one cluster");
    is_deeply($resp->[2]->{tuples}, [ \%tuple ], "call from more than one cluster");
    return;
}

sub check_pack {
    my $set = sub {
        my ($field, $value) = @_;
        return {
            type => 'update',
            key  => $TEST3_ID,
            ops  => [ [ $field => set => $value ] ],
            want_result => 1,
        };
    };
    my $ns = MR::Tarantool::Box::XS->new(
        iproto    => $shard_iproto,
        namespace => 23,
        format    => 'l Ll Ss Cc &$' . ($math_int64 ? 'Qq' : ''),
        fields    => [qw/ ID UInt32 Int32 UInt16 Int16 UInt8 Int8 String Utf8String /, $math_int64 ? qw/UInt64 Int64/ : ()],
        indexes   => [ { name => 'id', keys => ['ID'] } ],
    );
    my $resp = $ns->bulk([{
        type  => 'insert',
        tuple => {
            ID     => $TEST3_ID,
            UInt32 => 17,
            Int32  => 18,
            UInt16 => 19,
            Int16  => 20,
            UInt8  => 21,
            Int8   => 22,
            String     => "Some test string",
            Utf8String => "Another test string",
            $math_int64 ? (
                UInt64 => 15,
                Int64  => 16,
            ) : (),
        },
    }]);
    is($resp->[0]->{tuple}, 1, "insert");

    $resp = $ns->do({
        type => 'select',
        keys => [$TEST3_ID],
    });

    SKIP: {
        skip "Math::Int64", 8 unless $math_int64;
        require Math::Int64;

        isa_ok($resp->{tuples}->[0]->{UInt64}, 'Math::UInt64', "UInt64");
        isa_ok($resp->{tuples}->[0]->{Int64}, 'Math::Int64', "Int64");
        cmp_ok($resp->{tuples}->[0]->{UInt64}, '==', 15, "UInt64 compare with UV");
        cmp_ok($resp->{tuples}->[0]->{Int64}, '==', 16, "Int64 compare with IV");

        $resp = $ns->bulk([ $set->(UInt64 => '9000000000000000001'), $set->(Int64 => '9000000000000000002') ]);
        is($resp->[0]->{tuple}->{UInt64}, '9000000000000000001', "pack uint64 from string");
        is($resp->[1]->{tuple}->{Int64}, '9000000000000000002', "pack int64 from string");

        $resp = $ns->bulk([ $set->(UInt64 => Math::Int64::uint64('9000000000000000003')), $set->(Int64 => Math::Int64::int64('9000000000000000004')) ]);
        is($resp->[0]->{tuple}->{UInt64}, '9000000000000000003', "pack uint64 from object");
        is($resp->[1]->{tuple}->{Int64}, '9000000000000000004', "pack int64 from object");
    }

    SKIP: {
        skip "range check", 12 unless $range_check;

        # 32
        $resp = $ns->bulk([ $set->(UInt32 => -1), $set->(Int32 => -1) ]);
        cmp_ok($resp->[0]->{error}, '==', MR::Tarantool::Box::XS::ERR_CODE_INVALID_REQUEST, "pack uint32 < 0 - invalid request");
        is($resp->[1]->{tuple}->{Int32}, -1, "pack int32 < 0 - ok");

        $resp = $ns->bulk([ $set->(UInt32 => 4294967295), $set->(Int32 => 4294967295) ]);
        is($resp->[0]->{tuple}->{UInt32}, 4294967295, "pack uint32 > INT32_MAX - ok");
        cmp_ok($resp->[1]->{error}, '==', MR::Tarantool::Box::XS::ERR_CODE_INVALID_REQUEST, "pack int32 > INT32_MAX - invalid request");

        # 16
        $resp = $ns->bulk([ $set->(UInt16 => -1), $set->(Int16 => -1) ]);
        cmp_ok($resp->[0]->{error}, '==', MR::Tarantool::Box::XS::ERR_CODE_INVALID_REQUEST, "pack uint16 < 0 - invalid request");
        is($resp->[1]->{tuple}->{Int16}, -1, "pack int16 < 0 - ok");

        $resp = $ns->bulk([ $set->(UInt16 => 65535), $set->(Int16 => 65535), $set->(UInt16 => 65536) ]);
        is($resp->[0]->{tuple}->{UInt16}, 65535, "pack uint16 > INT16_MAX - ok");
        cmp_ok($resp->[1]->{error}, '==', MR::Tarantool::Box::XS::ERR_CODE_INVALID_REQUEST, "pack int16 > INT16_MAX - invalid request");
        cmp_ok($resp->[2]->{error}, '==', MR::Tarantool::Box::XS::ERR_CODE_INVALID_REQUEST, "pack int16 > UINT16_MAX - invalid request");

        # 8
        $resp = $ns->bulk([ $set->(UInt8 => 128), $set->(Int8 => 128), $set->(UInt8 => 256) ]);
        is($resp->[0]->{tuple}->{UInt8}, 128, "pack uint8 > INT8_MAX - ok");
        cmp_ok($resp->[1]->{error}, '==', MR::Tarantool::Box::XS::ERR_CODE_INVALID_REQUEST, "pack int8 > INT8_MAX - invalid request");
        cmp_ok($resp->[2]->{error}, '==', MR::Tarantool::Box::XS::ERR_CODE_INVALID_REQUEST, "pack int8 > UINT8_MAX - invalid request");
    }

    $resp = $ns->bulk([ $set->(String => Encode::encode('cp1251', "Строка в cp1251")), $set->(Utf8String => "Строка в utf8") ]);
    is($resp->[0]->{tuple}->{String}, Encode::encode('cp1251', "Строка в cp1251"), "pack non-utf8 string as non-utf8 - ok");
    is($resp->[1]->{tuple}->{Utf8String}, "Строка в utf8", "pack utf8 string as utf8 - ok");

    my $cp1251_with_flag = Encode::encode('cp1251', "Строка в cp1251");
    Encode::_utf8_on($cp1251_with_flag);
    my $utf8_without_flag = "Строка в utf8";
    Encode::_utf8_off($utf8_without_flag);
    $resp = $ns->bulk([ $set->(String => $cp1251_with_flag), $set->(Utf8String => $utf8_without_flag) ]);
    is($resp->[0]->{tuple}->{String}, Encode::encode('cp1251', "Строка в cp1251"), "pack utf8 string as non-utf8 - ok");
    is($resp->[1]->{tuple}->{Utf8String}, "Строка в utf8", "pack non-utf8 string as utf8 - ok");

    $resp = $ns->do($set->(Utf8String => $cp1251_with_flag));
    cmp_ok($resp->{error}, '==', MR::Tarantool::Box::XS::ERR_CODE_INVALID_REQUEST, "malformed utf-8 string - invalid request");

    SKIP: {
        skip "check cp1251 <=> utf8", 6 unless $cp1251;

        my $ns = MR::Tarantool::Box::XS->new(
            iproto    => $shard_iproto,
            namespace => 23,
            format    => 'l &',
            fields    => [qw/ ID Str /],
            indexes   => [ { name => 'id', keys => ['ID'] } ],
        );
        my $cp1251_ns = MR::Tarantool::Box::XS->new(
            iproto    => $shard_iproto,
            namespace => 23,
            format    => 'l <',
            fields    => [qw/ ID Str /],
            indexes   => [ { name => 'id', keys => ['ID'] } ],
        );
        my $utf8_ns = MR::Tarantool::Box::XS->new(
            iproto    => $shard_iproto,
            namespace => 23,
            format    => 'l >',
            fields    => [qw/ ID Str /],
            indexes   => [ { name => 'id', keys => ['ID'] } ],
        );

        my $resp = $cp1251_ns->do({ type => 'insert', tuple => [ $TEST3_ID, "Строка" ], want_result => 1 });
        ok(utf8::is_utf8($resp->{tuple}->{Str}), "string returned from cp1251 field has utf8 flag");
        is($resp->{tuple}->{Str}, "Строка", "string returned from cp1251 field is in utf8");
        $resp = $ns->do({ type => 'select', keys => [ $TEST3_ID ] });
        is($resp->{tuples}->[0]->{Str}, Encode::encode('cp1251', "Строка"), "string in cp1251 field is realy in cp1251");

        $resp = $utf8_ns->do({ type => 'insert', tuple => [ $TEST3_ID, Encode::encode('cp1251', "Строка") ], want_result => 1 });
        ok(!utf8::is_utf8($resp->{tuple}->{Str}), "string returned from utf8 field has no utf8 flag");
        is($resp->{tuple}->{Str}, Encode::encode('cp1251', "Строка"), "string returned from utf8 field is in cp1251");
        $resp = $ns->do({ type => 'select', keys => [ $TEST3_ID ] });
        is($resp->{tuples}->[0]->{Str}, Encode::encode('utf8', "Строка"), "string in utf8 field is realy in utf8");
    }

    {
        my $wrong_ns = MR::Tarantool::Box::XS->new(
            iproto    => $shard_iproto,
            namespace => 23,
            format    => 'l $l Ss Cc &$' . ($math_int64 ? 'Qq' : ''),
            fields    => [qw/ ID UInt32 Int32 UInt16 Int16 UInt8 Int8 String Utf8String /, $math_int64 ? qw/UInt64 Int64/ : ()],
            indexes   => [ { name => 'id', keys => ['ID'] } ],
        );
        $wrong_ns->do({
            type => 'update',
            key  => $TEST3_ID,
            ops  => [ [ UInt32 => set => "" ] ],
        });
        my $resp = $ns->do({
            type => 'select',
            keys => [ $TEST3_ID ],
        });
        cmp_ok($resp->{error}, '==', MR::Tarantool::Box::XS::ERR_CODE_INVALID_RESPONSE, "invalid integer type field size");
    }

    $resp = $ns->bulk([{
        type => 'delete',
        key  => $TEST3_ID,
    }]);
    is($resp->[0]->{tuple}, 1, "delete");

    return;
}

sub check_singleton {
    {
        my $singleton = Test::JUData->create_singleton(
            iproto    => $shard_iproto,
            namespace => 22,
            format    => 'l',
            fields    => [qw/ ID /],
            indexes   => [ { name => 'id', keys => ['ID'] } ],
        );
        isa_ok($singleton, "Test::JUData", "create_singleton()");
    }
    my $resp = Test::JUData->bulk([{
        type => 'select',
        keys => [1000011658],
    }]);
    is($resp->[0]->{tuples}->[0]->{ID}, 1000011658, "access to namespace by singleton");
    {
        my $singleton = Test::JUData->instance();
        isa_ok($singleton, "Test::JUData", "instance()");
        cmp_ok($singleton->instance(), '==', $singleton, "instance() called on namespace's object");
        cmp_ok(Test::JUData->iproto(), '==', $singleton->iproto(), "iproto() called on namespace's singleton");
    }
    {
        my $singleton = Test::JUData->remove_singleton();
        isa_ok($singleton, "Test::JUData", "remove_singleton()");
    }
    {
        my $singleton = Test::Function->create_singleton(
            iproto     => $lua_iproto,
            name       => 'client_autotest.echo',
            in_format  => 'L$C',
            in_fields  => [ 'one', 'two', 'three' ],
            out_format => 'L$C',
            out_fields => [ 'one', 'two', 'three' ],
        );
        isa_ok($singleton, "Test::Function", "create_singleton()");
    }
    my %tuple = ( one => 1, two => 'two', three => 3 );
    $resp = Test::Function->do({
        type  => 'call',
        tuple => \%tuple,
    });
    is_deeply($resp->{tuples}, [ \%tuple ], "call function by singleton");
    {
        my $singleton = Test::Function->instance();
        isa_ok($singleton, "Test::Function", "instance()");
        cmp_ok($singleton->instance(), '==', $singleton, "instance() called on function's object");
        cmp_ok(Test::Function->iproto(), '==', $singleton->iproto(), "iproto() called on function's singleton");
    }
    {
        my $singleton = Test::Function->remove_singleton();
        isa_ok($singleton, "Test::Function", "remove_singleton()");
    }
    {
        Test::IProto->create_singleton(masters => [$ENV{SHARD_SERVER}]);
        my $namespace = MR::Tarantool::Box::XS->new(
            iproto    => 'Test::IProto',
            namespace => 22,
            format    => 'lLLll &&&&& LLl & LLLLLLLLLLLL',
            fields    => [qw/ ID Bi1 Bi2 F3 F4 F5 F6 F7 F8 SubStr F10 F11 F12 F13 Bits F15 F16 F17 F18 F19 F20 F21 F22 F23 F24 F25 /],
            indexes   => [ { name => 'id', keys => ['ID'] } ],
        );
        my $resp = $namespace->do({ type => 'select', keys => [1000011658] });
        is($resp->{tuples}->[0]->{ID}, 1000011658, "access to namespace's cluster by singleton");
        Test::IProto->remove_singleton();
    }
    {
        Test::IProto->create_singleton(masters => [$ENV{SHARD_SERVER}]);
        my $function = MR::Tarantool::Box::XS::Function->new(
            iproto     => 'Test::IProto',
            name       => 'client_autotest.echo',
            in_format  => 'L$C',
            in_fields  => [ 'one', 'two', 'three' ],
            out_format => 'L$C',
            out_fields => [ 'one', 'two', 'three' ],
        );
        my %tuple = ( one => 1, two => 'two', three => 3 );
        my $resp = $function->do({ type  => 'call', tuple => \%tuple });
        is_deeply($resp->{tuples}, [ \%tuple ], "access to function's cluster by singleton");
        Test::IProto->remove_singleton();
    }
    return;
}

sub check_info {
    is($dispatch->iproto(), $dispatch_iproto, "namespace's iproto()");
    is($echo->iproto(), $lua_iproto, "function's iproto()");
    return;
}

sub check_async {
    SKIP: {
        skip "cannot check async when internal loop is used", 6 unless $ev || $coro;

        {
            my $resp;
            my $cv = AnyEvent->condvar();
            $dispatch->do({
                type     => 'select',
                keys     => [1000011658, 1000011659],
                callback => sub { $resp = $_[0]; $cv->send(); },
            });
            $cv->recv();
            is($resp->{tuples}->[0]->{ID}, 1000011658, "async select do");
        }

        {
            my @resp;
            my $cv = AnyEvent->condvar();
            $dispatch->bulk([{
                type     => 'select',
                keys     => [1000011658, 1000011659],
                callback => sub { push @resp, $_[0]; $cv->send() },
            }]);
            $cv->recv();
            is($resp[0]->{tuples}->[0]->{ID}, 1000011658, "async select list by id");
        }

        my $ns = MR::Tarantool::Box::XS->new(
            iproto    => $shard_iproto,
            namespace => 23,
            format    => 'l$',
            fields    => [qw/ ID Utf8String / ],
            indexes   => [ { name => 'id', keys => ['ID'] } ],
        );

        {
            my $cp1251_with_flag = Encode::encode('cp1251', "Строка в cp1251");
            Encode::_utf8_on($cp1251_with_flag);
            my $cv = AnyEvent->condvar();
            my $resp;
            $ns->do({
                type => 'update',
                key  => $TEST3_ID,
                ops  => [ [ Utf8String => set => $cp1251_with_flag ] ],
                callback => sub { $resp = $_[0]; $cv->send() },
            });
            $cv->recv();
            cmp_ok($resp->{error}, '==', MR::Tarantool::Box::XS::ERR_CODE_INVALID_REQUEST, "async invalid do request");
        }

        {
            my $cp1251_with_flag = Encode::encode('cp1251', "Строка в cp1251");
            Encode::_utf8_on($cp1251_with_flag);
            my $cv = AnyEvent->condvar();
            my $resp;
            $ns->bulk([{
                type => 'update',
                key  => $TEST3_ID,
                ops  => [ [ Utf8String => set => $cp1251_with_flag ] ],
                callback => sub { $resp = $_[0]; $cv->send() },
            }]);
            $cv->recv();
            cmp_ok($resp->{error}, '==', MR::Tarantool::Box::XS::ERR_CODE_INVALID_REQUEST, "async invalid bulk request");
        }

        {
            my $resp;
            my $cv = AnyEvent->condvar();
            my %tuple = ( one => 1, two => 'two', three => 3 );
            $echo->do({
                type  => 'call',
                tuple => \%tuple,
                callback => sub { $resp = $_[0]; $cv->send() },
            });
            $cv->recv();
            is_deeply($resp->{tuples}, [ \%tuple ], "async do function");
        }

        {
            my $resp;
            my $cv = AnyEvent->condvar();
            my %tuple = ( one => 1, two => 'two', three => 3 );
            $echo->bulk([{
                type  => 'call',
                tuple => \%tuple,
                callback => sub { $resp = $_[0]; $cv->send() },
            }]);
            $cv->recv();
            is_deeply($resp->{tuples}, [ \%tuple ], "async bulk function");
        }
    }
    return;
}

sub check_leak {
    no warnings 'redefine';
    local *main::is = sub {};
    local *main::ok = sub {};
    local *main::cmp_ok = sub {};
    local *main::isa_ok = sub {};
    local *main::is_deeply = sub {};
    local *main::skip = sub { no warnings 'exiting'; last SKIP };
    no_leaks_ok { check_select() } "select not leaks";
    no_leaks_ok { check_insert() } "insert not leaks";
    no_leaks_ok { check_update() } "update not leaks";
    no_leaks_ok { check_delete() } "delete not leaks";
    no_leaks_ok { check_multicluster() } "multicluster not leaks";
    no_leaks_ok { check_call() } "call not leaks";
    no_leaks_ok { check_pack() } "various pack/unpack not leaks";
    no_leaks_ok { check_singleton() } "singleton not leaks";
    no_leaks_ok { check_info() } "info not leaks";
    no_leaks_ok { check_async() } "async not leaks";
    return;
}
