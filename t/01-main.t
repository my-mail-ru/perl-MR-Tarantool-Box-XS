use strict;
use warnings;
use Test::More tests => 63;
use Test::LeakTrace;
use MR::IProto::XS;
BEGIN { use_ok('MR::Tarantool::Box::XS') };
use Encode;
use utf8;

my $TEST_ID = 1999999999;
my $TEST2_ID = 1999999998;
my $TEST3_ID = 1999999997;

MR::IProto::XS->set_logmask(MR::IProto::XS::LOG_NOTHING);
MR::Tarantool::Box::XS->set_logmask(MR::Tarantool::Box::XS::LOG_NOTHING);
my $shard_iproto = MR::IProto::XS->new(masters => [$ENV{SHARD_SERVER}]);

my $ns = MR::Tarantool::Box::XS->new(
    iproto    => $shard_iproto,
    namespace => 22,
    format    => 'lLLll &&&&& L&l & LL&lLlLLl&L&',
    fields    => [qw/ ID Bi1 Bi2 F3 F4 F5 F6 F7 F8 SubStr F10 F11 F12 F13 Bits F15 F16 F17 F18 F19 F20 F21 F22 F23 F24 F25 /],
    indexes   => [ { name => 'id', keys => ['ID'] } ],
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

{
    package Test::JUData;
    use base 'MR::Tarantool::Box::XS';
}

check_select();
check_insert();
check_update();
check_delete();
check_pack();
check_singleton();
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
    is($resp->[0]->{tuples}->[0]->{ID}, 1000011658, "select list by id");

    $resp = $dispatch->bulk([{
        type    => 'select',
        keys    => [1000011658, 1000011659],
        hash_by => 'primary_id',
    }]);
    is($resp->[0]->{tuples}->{1000011658}->{ID}, 1000011658, "select hash by id");

    $resp = $dispatch->bulk([{
        type    => 'select',
        keys    => [1000011658, 1000011659],
        hash_by => 'primary_email',
    }]);
    is($resp->[0]->{tuples}->{'kadabra100@mail.ru'}->{ID}, 1000011658, "select hash by email");

    $resp = $dispatch->bulk([{
        type    => 'select',
        index   => 'primary_email',
        keys    => ['kadabra100@mail.ru', 'kadabra101@mail.ru'],
    }]);
    is($resp->[0]->{tuples}->[0]->{ID}, 1000011658, "select list by email");

    $resp = $dispatch->bulk([{
        type    => 'select',
        keys    => [ [ 1000011658 ], { ID => 1000011659 } ],
    }]);
    is($resp->[0]->{tuples}->[0]->{ID}, 1000011658, "select by key in array");
    is($resp->[0]->{tuples}->[1]->{ID}, 1000011659, "select by key in hash");
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

sub check_pack {
    my $ns = MR::Tarantool::Box::XS->new(
        iproto    => $shard_iproto,
        namespace => 23,
        format    => 'l Ll Ss Cc &$',
        fields    => [qw/ ID UInt32 Int32 UInt16 Int16 UInt8 Int8 String Utf8String /],
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
        },
    }]);
    is($resp->[0]->{tuple}, 1, "insert");

    $resp = $ns->bulk([
            {
                type => 'update',
                key  => $TEST3_ID,
                ops  => [ [ UInt32 => set => -1 ] ],
                want_result => 1,
            },
            {
                type => 'update',
                key  => $TEST3_ID,
                ops  => [ [ Int32 => set => -1 ] ],
                want_result => 1,
            },
    ]);
    cmp_ok($resp->[0]->{error}, '==', MR::Tarantool::Box::XS::ERR_CODE_INVALID_REQUEST, "pack uint32 < 0 - invalid request");
    is($resp->[1]->{tuple}->{Int32}, -1, "pack int32 < 0 - ok");

    $resp = $ns->bulk([
        {
            type => 'update',
            key  => $TEST3_ID,
            ops  => [ [ UInt32 => set => 4294967295 ] ],
            want_result => 1,
        },
        {
            type => 'update',
            key  => $TEST3_ID,
            ops  => [ [ Int32 => set => 4294967295 ] ],
            want_result => 1,
        },
    ]);
    is($resp->[0]->{tuple}->{UInt32}, 4294967295, "pack uint32 > INT32_MAX - ok");
    cmp_ok($resp->[1]->{error}, '==', MR::Tarantool::Box::XS::ERR_CODE_INVALID_REQUEST, "pack int32 > INT32_MAX - invalid request");

    $resp = $ns->bulk([
        {
            type => 'update',
            key  => $TEST3_ID,
            ops  => [ [ UInt16 => set => -1 ] ],
            want_result => 1,
        },
        {
            type => 'update',
            key  => $TEST3_ID,
            ops  => [ [ Int16 => set => -1 ] ],
            want_result => 1,
        },
    ]);
    cmp_ok($resp->[0]->{error}, '==', MR::Tarantool::Box::XS::ERR_CODE_INVALID_REQUEST, "pack uint16 < 0 - invalid request");
    is($resp->[1]->{tuple}->{Int16}, -1, "pack int16 < 0 - ok");

    $resp = $ns->bulk([
        {
            type => 'update',
            key  => $TEST3_ID,
            ops  => [ [ UInt16 => set => 65535 ] ],
            want_result => 1,
        },
        {
            type => 'update',
            key  => $TEST3_ID,
            ops  => [ [ Int16 => set => 65535 ] ],
            want_result => 1,
        },
        {
            type => 'update',
            key  => $TEST3_ID,
            ops  => [ [ UInt16 => set => 65536 ] ],
            want_result => 1,
        },
    ]);
    is($resp->[0]->{tuple}->{UInt16}, 65535, "pack uint16 > INT16_MAX - ok");
    cmp_ok($resp->[1]->{error}, '==', MR::Tarantool::Box::XS::ERR_CODE_INVALID_REQUEST, "pack int16 > INT16_MAX - invalid request");
    cmp_ok($resp->[2]->{error}, '==', MR::Tarantool::Box::XS::ERR_CODE_INVALID_REQUEST, "pack int16 > UINT16_MAX - invalid request");

    $resp = $ns->bulk([
        {
            type => 'update',
            key  => $TEST3_ID,
            ops  => [ [ UInt8 => set => 128 ] ],
            want_result => 1,
        },
        {
            type => 'update',
            key  => $TEST3_ID,
            ops  => [ [ Int8 => set => 128 ] ],
            want_result => 1,
        },
        {
            type => 'update',
            key  => $TEST3_ID,
            ops  => [ [ UInt8 => set => 256 ] ],
            want_result => 1,
        },
    ]);
    is($resp->[0]->{tuple}->{UInt8}, 128, "pack uint8 > INT8_MAX - ok");
    cmp_ok($resp->[1]->{error}, '==', MR::Tarantool::Box::XS::ERR_CODE_INVALID_REQUEST, "pack int8 > INT8_MAX - invalid request");
    cmp_ok($resp->[2]->{error}, '==', MR::Tarantool::Box::XS::ERR_CODE_INVALID_REQUEST, "pack int8 > UINT8_MAX - invalid request");

    $resp = $ns->bulk([
        {
            type => 'update',
            key  => $TEST3_ID,
            ops  => [ [ String => set => Encode::encode('cp1251', "Строка в cp1251") ] ],
            want_result => 1,
        },
        {
            type => 'update',
            key  => $TEST3_ID,
            ops  => [ [ Utf8String => set => "Строка в utf8" ] ],
            want_result => 1,
        },
    ]);
    is($resp->[0]->{tuple}->{String}, Encode::encode('cp1251', "Строка в cp1251"), "pack non-utf8 string as non-utf8 - ok");
    is($resp->[1]->{tuple}->{Utf8String}, "Строка в utf8", "pack utf8 string as utf8 - ok");

    my $cp1251_with_flag = Encode::encode('cp1251', "Строка в cp1251");
    utf8::decode($cp1251_with_flag);
    my $utf8_without_flag = "Строка в utf8";
    utf8::encode($utf8_without_flag);
    $resp = $ns->bulk([
        {
            type => 'update',
            key  => $TEST3_ID,
            ops  => [ [ String => set => $cp1251_with_flag ] ],
            want_result => 1,
        },
        {
            type => 'update',
            key  => $TEST3_ID,
            ops  => [ [ Utf8String => set => $utf8_without_flag ] ],
            want_result => 1,
        },
    ]);
    is($resp->[0]->{tuple}->{String}, Encode::encode('cp1251', "Строка в cp1251"), "pack utf8 string as non-utf8 - ok");
    is($resp->[1]->{tuple}->{Utf8String}, "Строка в utf8", "pack non-utf8 string as utf8 - ok");

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
        isa_ok($singleton, "Test::JUData");
    }
    my $resp = Test::JUData->bulk([{
        type => 'select',
        keys => [1000011658],
    }]);
    is($resp->[0]->{tuples}->[0]->{ID}, 1000011658, "access to namespace by singleton");
    {
        my $singleton = Test::JUData->remove_singleton();
        isa_ok($singleton, "Test::JUData");
    }
}

sub check_leak {
    no warnings 'redefine';
    local *main::is = sub {};
    local *main::cmp_ok = sub {};
    local *main::isa_ok = sub {};
    no_leaks_ok { check_select() } "select not leaks";
    no_leaks_ok { check_insert() } "insert not leaks";
    no_leaks_ok { check_update() } "update not leaks";
    no_leaks_ok { check_delete() } "delete not leaks";
    no_leaks_ok { check_pack() } "various pack/unpack not leaks";
    no_leaks_ok { check_singleton() } "singleton not leaks";
    return;
}
