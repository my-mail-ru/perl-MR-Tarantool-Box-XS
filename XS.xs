#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include "ppport.h"

#ifdef WITH_MATH_INT64
#define MATH_INT64_NATIVE_IF_AVAILABLE
#include <perl_math_int64.h>
#endif

#include <tarantoolbox.h>
#include <iprotoxs.h>
#include <assert.h>

#define MY_CXT_KEY "MR::Tarantool::Box::XS::_guts" XS_VERSION

typedef struct {
    HV *namespaces;
    HV *functions;
#ifdef WITH_CP1251
    SV *cp1251;
#endif
} my_cxt_t;

START_MY_CXT;

typedef struct {
    AV *fields;
    HV *field_id_by_name;
    SV *format;
} tbtupleconf_t;

typedef struct {
    uint32_t namespace;
    uint32_t default_index;
    AV *indexes;
    HV *index_id_by_name;
    AV *index_format;
    AV *index_fields;
    tbtupleconf_t tuple;
} tbns_t;

typedef struct {
    char *name;
    tbtupleconf_t in;
    uint32_t out_count;
    tbtupleconf_t *out;
} tbfunc_t;

typedef struct {
    SV *cluster;
    uint16_t microsharding;
    bool is_func;
    union {
        tbns_t ns;
        tbfunc_t func;
    };
} tbinst_t;

typedef struct {
    SV *instance;
    tbinst_t *inst;
    SV *request;
    tarantoolbox_message_type_t type;
    tarantoolbox_message_t *message;
    SV *error;
    SV *callback;
} tbxs_data_t;

typedef SV * MR__IProto__XS;
typedef SV * MR__Tarantool__Box__XS;
typedef SV * MR__Tarantool__Box__XS__Function;

static SV *tbxs_context_response(tbxs_data_t *context);
static void tbxs_context_free(tbxs_data_t *context);

static void tbxs_context_callback(tbxs_data_t *context) {
    dSP;
    ENTER;
    SAVETMPS;
    PUSHMARK(SP);
    mXPUSHs(tbxs_context_response(context));
    PUTBACK;
    call_sv(context->callback, G_EVAL|G_DISCARD);
    SPAGAIN;
    if (SvTRUE(ERRSV)) {
        warn("MR::Tarantool::Box::XS: died in callback: %s", SvPV_nolen(ERRSV));
    }
    FREETMPS;
    LEAVE;
    tbxs_context_free(context);
}

static void tbxs_callback(iproto_message_t *message) {
    tbxs_data_t *context = (tbxs_data_t *)iproto_message_options(message)->data;
    assert(tarantoolbox_message_get_iproto_message(context->message) == message);
    tbxs_context_callback(context);
}

#ifdef WITH_MATH_INT64
#define SvFieldOK(sv) (SvPOK(sv) || SvIOK(sv) || SvI64OK(sv) || SvU64OK(sv))
#else
#define SvFieldOK(sv) (SvPOK(sv) || SvIOK(sv))
#endif

#ifdef WITH_RANGE_CHECK
#define tbxs_check_value(type, condition, data, errsv, pv) \
    do { \
        if (!(condition)) { \
            data = NULL; \
            sv_setpvf(errsv, "value %s is out of range for "#type, SvPV_nolen(pv)); \
        } \
    } while (0)
#else
#define tbxs_check_value(type, condition, data, errsv, pv) \
    do { \
        if (!(condition)) { \
            warn("value %s is out of range for "#type, SvPV_nolen(pv)); \
        } \
    } while (0)
#endif

#ifdef WITH_CP1251
static SV *tbxs_find_cp1251(void) {
    require_pv("Encode.pm");
    dSP;
    dTARGET;
    ENTER;
    SAVETMPS;
    PUSHMARK(sp);
    XPUSHp("cp1251", 6);
    PUTBACK;
    call_pv("Encode::find_encoding", G_SCALAR);
    SPAGAIN;
    SV *cp1251 = POPs;
    if (!SvOK(cp1251))
        croak("encoding cp1251 not found");
    SvREFCNT_inc(cp1251);
    PUTBACK;
    FREETMPS;
    LEAVE;
    return cp1251;
}

static SV *tbxs_decode_cp1251(SV *sv) {
    dMY_CXT;
    dSP;
    ENTER;
    SAVETMPS;
    PUSHMARK(sp);
    XPUSHs(MY_CXT.cp1251);
    XPUSHs(sv);
    PUTBACK;
    call_method("decode", G_SCALAR);
    SPAGAIN;
    SV *res = POPs;
    SvREFCNT_inc(res);
    PUTBACK;
    FREETMPS;
    LEAVE;
    return res;
}

static SV *tbxs_encode_cp1251(SV *sv) {
    dMY_CXT;
    dSP;
    ENTER;
    SAVETMPS;
    PUSHMARK(sp);
    XPUSHs(MY_CXT.cp1251);
    XPUSHs(sv);
    PUTBACK;
    call_method("encode", G_SCALAR);
    SPAGAIN;
    SV *res = POPs;
    SvREFCNT_inc(res);
    PUTBACK;
    FREETMPS;
    LEAVE;
    return res;
}
#endif

static void *tbxs_sv_to_field(char format, SV *value, size_t *size, SV *errsv, bool range_check) {
    void *data;
    switch (format) {
#ifdef WITH_MATH_INT64
        case 'Q': {
            uint64_t v = SvU64(value);
            SV *tmp = sv_2mortal(newSVpvn((void *)&v, sizeof(uint64_t)));
            data = SvPV(tmp, *size);
            break;
        }
        case 'q': {
            int64_t v = SvI64(value);
            SV *tmp = sv_2mortal(newSVpvn((void *)&v, sizeof(int64_t)));
            data = SvPV(tmp, *size);
            break;
        }
#elif defined(HAS_QUAD) && IVSIZE >= I64SIZE
        case 'Q':
            SvUV(value);
            data = &SvUVX(value);
            *size = sizeof(uint64_t);
            if (range_check)
                tbxs_check_value(uint64_t, SvIOK_UV(value) || SvUVX(value) <= INT64_MAX, data, errsv, value);
            break;
        case 'q':
            SvIV(value);
            data = &SvIVX(value);
            *size = sizeof(int64_t);
            if (range_check)
                tbxs_check_value(int64_t, SvIOK_notUV(value) || SvIVX(value) >= 0, data, errsv, value);
            break;
#endif
        case 'L':
            SvUV(value);
            data = &SvUVX(value);
            *size = sizeof(uint32_t);
            if (range_check)
#if UVSIZE > U32SIZE
                tbxs_check_value(uint32_t, SvUVX(value) <= UINT32_MAX, data, errsv, value);
#else
                tbxs_check_value(uint32_t, SvIOK_UV(value) || SvUVX(value) <= INT32_MAX, data, errsv, value);
#endif
            break;
        case 'l':
            SvIV(value);
            data = &SvIVX(value);
            *size = sizeof(int32_t);
            if (range_check)
#if IVSIZE > I32SIZE
                tbxs_check_value(int32_t, SvIVX(value) >= INT32_MIN && SvIVX(value) <= INT32_MAX, data, errsv, value);
#else
                tbxs_check_value(int32_t, SvIOK_notUV(value) || SvIVX(value) >= 0, data, errsv, value);
#endif
            break;
        case 'S':
            SvUV(value);
            data = &SvUVX(value);
            *size = sizeof(uint16_t);
            if (range_check)
                tbxs_check_value(uint16_t, SvUVX(value) <= UINT16_MAX, data, errsv, value);
            break;
        case 's':
            SvIV(value);
            data = &SvIVX(value);
            *size = sizeof(int16_t);
            if (range_check)
                tbxs_check_value(int16_t, SvIVX(value) >= INT16_MIN && SvIVX(value) <= INT16_MAX, data, errsv, value);
            break;
        case 'C':
            SvUV(value);
            data = &SvUVX(value);
            *size = sizeof(uint8_t);
            if (range_check)
                tbxs_check_value(uint8_t, SvUVX(value) <= UINT8_MAX, data, errsv, value);
            break;
        case 'c':
            SvIV(value);
            data = &SvIVX(value);
            *size = sizeof(int8_t);
            if (range_check)
                tbxs_check_value(int8_t, SvIVX(value) >= INT8_MIN && SvIVX(value) <= INT8_MAX, data, errsv, value);
            break;
        case '&':
            data = SvPV(value, *size);
            break;
        case '$':
            data = SvPV(value, *size);
            if (!is_utf8_string(data, *size)) {
                data = NULL;
                sv_setpv(errsv, SvUTF8(value) ? "malformed UTF-8 character string" : "malformed UTF-8 octet string");
            }
            break;
#ifdef WITH_CP1251
        case '<': {
            SV *cp1251_value = tbxs_encode_cp1251(value);
            data = SvPV(cp1251_value, *size);
            sv_2mortal(cp1251_value);
            break;
        }
        case '>': {
            SV *utf8_value = tbxs_decode_cp1251(value);
            data = SvPV(utf8_value, *size);
            sv_2mortal(utf8_value);
            break;
        }
#endif
        default:
            croak("unknown format: '%c'", format);
    }
    return data;
}

// TODO check data size
SV *tbxs_field_to_sv(char format, void *data, size_t size) {
    SV *sv;
    switch (format) {
#ifdef WITH_MATH_INT64
        case 'Q':
            sv = newSVu64(*(uint64_t *)data);
            break;
        case 'q':
            sv = newSVi64(*(int64_t *)data);
            break;
#elif defined(HAS_QUAD) && IVSIZE >= I64SIZE
        case 'Q':
            sv = newSVuv(*(uint64_t *)data);
            break;
        case 'q':
            sv = newSViv(*(int64_t *)data);
            break;
#endif
        case 'L':
            sv = newSVuv(*(uint32_t *)data);
            break;
        case 'l':
            sv = newSViv(*(int32_t *)data);
            break;
        case 'S':
            sv = newSVuv(*(uint16_t *)data);
            break;
        case 's':
            sv = newSViv(*(int16_t *)data);
            break;
        case 'C':
            sv = newSVuv(*(uint8_t *)data);
            break;
        case 'c':
            sv = newSViv(*(int8_t *)data);
            break;
        case '&':
            sv = newSVpvn(data, size);
            break;
        case '$':
            sv = newSVpvn(data, size);
            SvUTF8_on(sv);
            break;
#ifdef WITH_CP1251
        case '<': {
            SV *cp1251_sv = newSVpvn(data, size);
            sv = tbxs_decode_cp1251(cp1251_sv);
            SvREFCNT_dec(cp1251_sv);
            break;
        }
        case '>': {
            SV *utf8_sv = newSVpvn(data, size);
            SvUTF8_on(utf8_sv);
            sv = tbxs_encode_cp1251(utf8_sv);
            SvREFCNT_dec(utf8_sv);
            break;
        }
#endif
        default:
            croak("unknown format: '%c'", format);
    }
    return sv;
}

tarantoolbox_tuple_t *tbxs_av_to_tuple(AV *av, SV *formatsv, SV *errsv) {
    STRLEN formatlen;
    char *format = formatsv ? SvPV(formatsv, formatlen) : NULL;
    tarantoolbox_tuple_t *tuple = tarantoolbox_tuple_init(av_len(av) + 1);
    for (I32 i = 0; i <= av_len(av); i++) {
        SV **val = av_fetch(av, i, 0);
        SV *value = *val;
        size_t size;
        void *data;
        if (format && i < formatlen) {
            data = tbxs_sv_to_field(format[i], value, &size, errsv, true);
            if (data == NULL) {
                tarantoolbox_tuple_free(tuple);
                return NULL;
            }
        } else {
            data = SvPV(value, size);
        }
        tarantoolbox_tuple_set_field(tuple, i, data, size);
    }
    return tuple;
}

tarantoolbox_tuple_t *tbxs_hv_to_tuple(HV *hv, AV *fields, SV *format, SV *errsv) {
    I32 size = av_len(fields) + 1;

    AV *extra_fields = NULL;
    SV **val = hv_fetch(hv, "extra_fields", 12, 0);
    if (val) {
        if (!(SvROK(*val) && SvTYPE(SvRV(*val)) == SVt_PVAV))
            croak("\"extra_fields\" should be an ARRAYREF");
        extra_fields = (AV *)SvRV(*val);
        size += av_len(extra_fields) + 1;
    }

    AV *av = newAV();
    av_extend(av, size - 1);
    sv_2mortal((SV *)av);

    I32 i;
    for (i = 0; i <= av_len(fields); i++) {
        val = av_fetch(fields, i, 0);
        STRLEN keylen;
        char *key = SvPV(*val, keylen);
        val = hv_fetch(hv, key, keylen, 0);
        if (!val)
            croak("\"%s\" is missed in tuple", key);
        (void)av_store(av, i, SvREFCNT_inc(*val));
    }
    if (extra_fields) {
        for (I32 j = 0; j <= av_len(extra_fields); j++, i++) {
            val = av_fetch(extra_fields, j, 0);
            (void)av_store(av, i, SvREFCNT_inc(*val));
        }
    }
    return tbxs_av_to_tuple(av, format, errsv);
}

tarantoolbox_tuple_t *tbxs_sv_to_tuple(SV *sv, tbtupleconf_t *conf, SV *errsv) {
    if (SvROK(sv) && SvTYPE(SvRV(sv)) == SVt_PVHV) {
        if (!conf->fields)
            croak("\"fields\" in namespace should be defined");
        return tbxs_hv_to_tuple((HV *)SvRV(sv), conf->fields, conf->format, errsv);
    } else if (SvROK(sv) && SvTYPE(SvRV(sv)) == SVt_PVAV) {
        return tbxs_av_to_tuple((AV *)SvRV(sv), conf->format, errsv);
    } else {
        croak("\"tuple\" should be a HASHREF or an ARRAYREF");
    }
}

AV *tbxs_tuple_to_av(tarantoolbox_tuple_t *tuple, SV *formatsv) {
    STRLEN formatlen;
    char *format = formatsv ? SvPV(formatsv, formatlen) : NULL;
    AV *tupleav = newAV();
    uint32_t cardinality = tarantoolbox_tuple_get_cardinality(tuple);
    av_extend(tupleav, cardinality - 1);
    for (uint32_t j = 0; j < cardinality; j++) {
        size_t size;
        void *data = tarantoolbox_tuple_get_field(tuple, j, &size);
        SV *sv;
        if (format && j < formatlen) {
            sv = tbxs_field_to_sv(format[j], data, size);
        } else {
            sv = newSVpvn(data, size);
        }
        (void)av_store(tupleav, j, sv);
    }
    return tupleav;
}

HV *tbxs_tuple_av_to_hv(AV *tupleav, AV *fields) {
    HV *tuplehv = newHV();
    SV **val;
    I32 j;
    for (j = 0; j <= av_len(fields); j++) {
        SV **name = av_fetch(fields, j, 0);
        if (!SvPOK(*name)) croak("field name should be a string");
        if ((val = av_fetch(tupleav, j, 0)))
            (void)hv_store_ent(tuplehv, *name, SvREFCNT_inc(*val), 0);
    }
    if (j <= av_len(tupleav)) {
        val = av_fetch(tupleav, j, 0);
        AV *extra = av_make(av_len(tupleav) - j + 1, val);
        (void)hv_store(tuplehv, "extra_fields", 12, newRV_noinc((SV *)extra), 0);
    }
    sv_2mortal((SV *)tupleav);
    return tuplehv;
}

tarantoolbox_tuple_t *tbxs_sv_to_key(uint32_t index, SV *sv, tbns_t *ns, SV *errsv) {
    SV **val = av_fetch(ns->index_format, index, 0);
    if (!val) croak("invalid index %u", index);
    SV *format = *val;
    if (SvFieldOK(sv)) {
        size_t size;
        void *data = tbxs_sv_to_field(SvPVX(format)[0], sv, &size, errsv, true);
        if (data == NULL) return NULL;
        tarantoolbox_tuple_t *tuple = tarantoolbox_tuple_init(1);
        tarantoolbox_tuple_set_field(tuple, 0, data, size);
        return tuple;
    } else if (SvROK(sv) && SvTYPE(SvRV(sv)) == SVt_PVAV) {
        AV *av = (AV *)SvRV(sv);
        if (av_len(av) + 1 > SvCUR(format))
            croak("\"key\" should be an ARRAYREF of size %d or less", SvCUR(format));
        return tbxs_av_to_tuple(av, format, errsv);
    } else if (SvROK(sv) && SvTYPE(SvRV(sv)) == SVt_PVHV) {
        HV *hv = (HV *)SvRV(sv);
        if (!ns->index_fields) croak("\"fields\" in namespace should be specified");
        SV **val = av_fetch(ns->index_fields, index, 0);
        return tbxs_hv_to_tuple(hv, (AV *)SvRV(*val), format, errsv);
    } else {
        croak("\"key\" should be a SCALAR, an ARRAYREF or a HASHREF");
    }
    return NULL;
}

tarantoolbox_tuples_t *tbxs_av_to_keys(uint32_t index, AV *av, tbns_t *ns, SV *errsv) {
    tarantoolbox_tuples_t *tuples = tarantoolbox_tuples_init(av_len(av) + 1, true);
    for (I32 i = 0; i <= av_len(av); i++) {
        SV **val = av_fetch(av, i, 0);
        tarantoolbox_tuple_t *tuple = tbxs_sv_to_key(index, *val, ns, errsv);
        if (tuple == NULL) {
            tarantoolbox_tuples_free(tuples);
            return NULL;
        }
        tarantoolbox_tuples_set_tuple(tuples, i, tuple);
    }
    return tuples;
}

static SV *tbns_instance(SV *sv) {
    if (!sv_derived_from(sv, "MR::Tarantool::Box::XS")) {
        croak("\"%s\" is not of type MR::Tarantool::Box::XS", SvPV_nolen(sv));
    } else if (SvPOK(sv)) {
        dMY_CXT;
        HE *he = hv_fetch_ent(MY_CXT.namespaces, sv, 0, 0);
        return he ? HeVAL(he) : NULL;
    } else {
        return sv;
    }
}

static SV *tbfunc_instance(SV *sv) {
    if (!sv_derived_from(sv, "MR::Tarantool::Box::XS::Function")) {
        croak("\"%s\" is not of type MR::Tarantool::Box::XS::Function", SvPV_nolen(sv));
    } else if (SvPOK(sv)) {
        dMY_CXT;
        HE *he = hv_fetch_ent(MY_CXT.functions, sv, 0, 0);
        return he ? HeVAL(he) : NULL;
    } else {
        return sv;
    }
}

static tbinst_t *tbns_inst(SV *sv) {
    if (!sv)
        return NULL;
    SV *instance = tbns_instance(sv);
    if (!instance)
        return NULL;
    tbinst_t *inst = INT2PTR(tbinst_t *, SvIV((SV*)SvRV(instance)));
    assert(!inst->is_func);
    return inst;
}

static tbinst_t *tbfunc_inst(SV *sv) {
    if (!sv)
        return NULL;
    SV *instance = tbfunc_instance(sv);
    if (!instance)
        return NULL;
    tbinst_t *inst = INT2PTR(tbinst_t *, SvIV((SV*)SvRV(instance)));
    assert(inst->is_func);
    return inst;
}

static tarantoolbox_message_type_t tbxs_fetch_type(SV *request) {
    HV *reqhv = (HV *)SvRV(request);
    SV **val = hv_fetch(reqhv, "type", 4, 0);
    if (!val)
        croak("\"type\" is required");
    if (!SvPOK(*val))
        croak("\"type\" should be a string");
    char *name = SvPV_nolen(*val);
    tarantoolbox_message_type_t type;
    if (strcmp(name, "select") == 0) {
        type = SELECT;
    } else if (strcmp(name, "insert") == 0) {
        type = INSERT;
    } else if (strcmp(name, "update") == 0) {
        type = UPDATE_FIELDS;
    } else if (strcmp(name, "delete") == 0) {
        type = DELETE;
    } else if (strcmp(name, "call") == 0) {
        type = EXEC_LUA;
    } else {
        croak("unknown message type: \"%s\"", name);
    }
    return type;
}

tarantoolbox_tuple_t *tbxs_fetch_key(HV *request, tbns_t *ns, SV *errsv) {
    SV **val = hv_fetch(request, "key", 3, 0);
    if (!val) croak("\"key\" is required");
    return tbxs_sv_to_key(0, *val, ns, errsv);
}

uint32_t tbxs_fetch_want_result(HV *request) {
    SV **val = hv_fetch(request, "want_result", 11, 0);
    return (val && SvTRUE(*val)) ? WANT_RESULT : 0;
}

bool tbxs_fetch_raw(HV *request) {
    SV **val = hv_fetch(request, "raw", 3, 0);
    return val ? SvTRUE(*val) : false;
}

static SV *tbxs_fetch_hash_by(HV *request, tbtupleconf_t *conf) {
    SV **val = hv_fetch(request, "hash_by", 7, 0);
    if (val) {
        if (SvPOK(*val)) {
            HE *he = hv_fetch_ent(conf->field_id_by_name, *val, 0, 0);
            if (!he) croak("no such field: \"%s\"", SvPV_nolen(*val));
            return HeVAL(he);
        } else if (SvIOK(*val)) {
            if (SvIV(*val) > av_len(conf->fields))
                croak("field %"IVdf" is out of range", SvIV(*val));
            return *val;
        } else {
            croak("\"hash_by\" should be a string or an integer");
        }
    }
    return NULL;
}

static SV *tbxs_fetch_callback(SV *request) {
    HV *reqhv = (HV *)SvRV(request);
    SV **val = hv_fetch(reqhv, "callback", 8, 0);
    if (val) {
        if (!(SvROK(*val) || SvTYPE(SvRV(*val)) == SVt_PVCV))
            croak("\"callback\" should be a CODEREF");
        return *val;
    }
    return NULL;
}

static void tbxs_set_message_cluster(tarantoolbox_message_t *message, SV *clustersv) {
    iproto_cluster_t *cluster = iprotoxs_instance_to_cluster(clustersv);
    iproto_message_t *imessage = tarantoolbox_message_get_iproto_message(message);
    iproto_message_set_cluster(imessage, cluster);
}

static tarantoolbox_message_t *tbxs_select_message_init(tbxs_data_t *context) {
    tbns_t *ns = &context->inst->ns;
    HV *request = (HV *)SvRV(context->request);

    SV **val;
    uint32_t index;
    if ((val = hv_fetch(request, "use_index", 9, 0))) {
        if (SvIOK(*val)) {
            index = SvUV(*val);
        } else if (SvPOK(*val)) {
            STRLEN namelen;
            char *name = SvPV(*val, namelen);
            val = hv_fetch(ns->index_id_by_name, name, namelen, 0);
            if (!val)
                croak("no such index: \"%s\"", name);
            index = SvUV(*val);
        } else {
            croak("\"use_index\" shouls be a string or an integer");
        }
    } else if (ns) {
        index = ns->default_index;
    } else {
        croak("\"use_index\" should be specified");
    }

    uint32_t offset = 0;
    if ((val = hv_fetch(request, "offset", 6, 0))) {
        if (!(SvIOK(*val) || looks_like_number(*val)))
            croak("\"offset\" should be an integer");
        offset = SvUV(*val);
    }

    uint32_t limit = 0;
    if ((val = hv_fetch(request, "limit", 5, 0))) {
        if (!SvOK(*val))
            limit = UINT32_MAX;
        else if (SvIOK(*val) || looks_like_number(*val))
            limit = SvUV(*val);
        else
            croak("\"limit\" should be an integer");
    }

    val = hv_fetch(request, "keys", 4, 0);
    if (!val) croak("\"keys\" should be specified");
    if (!(SvROK(*val) && SvTYPE(SvRV(*val)) == SVt_PVAV)) croak("\"keys\" should be an ARRAYREF");
    AV *keysav = (AV *)SvRV(*val);
    if (av_len(keysav) == -1) croak("\"keys\" should be non-empty");
    val = av_fetch(keysav, 0, 0);
    int nfields;
    if (SvFieldOK(*val)) {
        nfields = 1;
    } else if (SvROK(*val) && SvTYPE(SvRV(*val)) == SVt_PVAV) {
        nfields = av_len((AV *)SvRV(*val)) + 1;
        if (nfields == 0) croak("values of \"keys\" should be non-empty ARRAYREFs");
    } else {
        croak("values of \"keys\" should be ARRAY references or SCALARs");
    }
    tarantoolbox_tuples_t *keys = tbxs_av_to_keys(index, keysav, ns, context->error);
    if (keys == NULL) return NULL;
    tarantoolbox_message_t *message = tarantoolbox_select_init(ns->namespace, index, keys, offset, limit);
    tarantoolbox_tuples_free(keys);
    return message;
}

static tarantoolbox_message_t *tbxs_insert_message_init(tbxs_data_t * context) {
    tbns_t *ns = &context->inst->ns;
    HV *request = (HV *)SvRV(context->request);

    SV **val = hv_fetch(request, "tuple", 5, 0);
    if (!val) croak("\"tuple\" is required");
    SV *tuplesv = *val;

    uint32_t flags = tbxs_fetch_want_result(request);
    if ((val = hv_fetch(request, "action", 6, 0))) {
        if (!SvPOK(*val))
            croak("\"action\" should be a string");
        char *action = SvPV_nolen(*val);
        if (strcmp(action, "add") == 0) {
            flags |= INSERT_ADD;
        } else if (strcmp(action, "replace") == 0) {
            flags |= INSERT_REPLACE;
        } else if (strcmp(action, "set") != 0) {
            croak("\"action\" should be \"set\", \"add\" or \"replace\"");
        }
    }

    tarantoolbox_tuple_t *tuple = tbxs_sv_to_tuple(tuplesv, &ns->tuple, context->error);
    if (tuple == NULL) return NULL;
    tarantoolbox_message_t *message = tarantoolbox_insert_init(ns->namespace, tuple, flags);
    tarantoolbox_tuple_free(tuple);
    return message;
}

tarantoolbox_update_ops_t *tbxs_fetch_update_ops(HV *request, tbtupleconf_t *conf, SV *errsv) {
    SV **val = hv_fetch(request, "ops", 3, 0);
    if (!val)
        croak("\"ops\" are required");
    if (!(SvROK(*val) && SvTYPE(SvRV(*val)) == SVt_PVAV))
        croak("\"ops\" should be an ARRAYREF");
    AV *av = (AV *)SvRV(*val);
    tarantoolbox_update_ops_t *ops = tarantoolbox_update_ops_init(av_len(av) + 1);
    for (I32 i = 0; i <= av_len(av); i++) {
        val = av_fetch(av, i, 0);
        if (!(SvROK(*val) && SvTYPE(SvRV(*val)) == SVt_PVAV))
            croak("each op should be an ARRAYREF");
        AV *opav = (AV *)SvRV(*val);
        if (av_len(opav) < 1)
            croak("each op should contain at least 2 elements: field_num and op");

        uint32_t field_num;
        val = av_fetch(opav, 0, 0);
        if (SvIOK(*val)) {
            field_num = SvUV(*val);
        } else if (SvPOK(*val)) {
            if (!conf->field_id_by_name)
                croak("you should configure namespace if you want to use names instead of ids");
            HE *he = hv_fetch_ent(conf->field_id_by_name, *val, 0, 0);
            if (!he)
                croak("no such field: \"%s\"", SvPV_nolen(*val));
            field_num = SvUV(HeVAL(he));
        } else {
            croak("field_num should be an integer or a string");
        }

        uint8_t op;
        val = av_fetch(opav, 1, 0);
        if (!SvPOK(*val))
            croak("op should be a string");
        char *opstr = SvPV_nolen(*val);

        STRLEN formatlen;
        char *format = SvPV(conf->format, formatlen);
        char f = field_num < formatlen ? format[field_num] : '&';

        SV *value;
        int32_t offset;
        int32_t length;
        if (strcmp(opstr, "delete") == 0) {
            if (!(av_len(opav) == 1 || (av_len(opav) == 2 && !SvOK(*av_fetch(opav, 2, 0)))))
                croak("each delete op should contain 2 elements: field_num and op");
            op = UPDATE_OP_DELETE;
        } else {
            if (av_len(opav) != 2)
                croak("each op should contain 3 elements: field_num, op and value");
            value = *av_fetch(opav, 2, 0);

            if (strcmp(opstr, "set") == 0) {
                op = UPDATE_OP_SET;
            } else if (strcmp(opstr, "add") == 0) {
                op = UPDATE_OP_ADD;
            } else if (strcmp(opstr, "and") == 0) {
                op = UPDATE_OP_AND;
            } else if (strcmp(opstr, "xor") == 0) {
                op = UPDATE_OP_XOR;
            } else if (strcmp(opstr, "or") == 0) {
                op = UPDATE_OP_OR;
            } else if (strcmp(opstr, "num_add") == 0) {
                op = UPDATE_OP_ADD;
            } else if (strcmp(opstr, "num_sub") == 0) {
                op = UPDATE_OP_ADD;
                value = sv_2mortal(newSViv(-SvIV(value)));
            } else if (strcmp(opstr, "bit_set") == 0) {
                op = UPDATE_OP_OR;
            } else if (strcmp(opstr, "bit_clear") == 0) {
                op = UPDATE_OP_AND;
                value = sv_2mortal(newSVuv(~SvUV(value)));
            } else if (strcmp(opstr, "insert") == 0) {
                op = UPDATE_OP_INSERT;
            } else if (strcmp(opstr, "splice") == 0 || strcmp(opstr, "substr") == 0) {
                op = UPDATE_OP_SPLICE;
                if (!(SvROK(value) && SvTYPE(SvRV(value)) == SVt_PVAV))
                    croak("value for op %s should be an ARRAYREF of <int[, int[, string]]>", opstr);
                AV *sp = (AV *)SvRV(value);
                val = av_fetch(sp, 0, 0);
                offset = val && SvOK(*val) ? SvIV(*val) : 0;
                val = av_fetch(sp, 1, 0);
                length = val && SvOK(*val) ? SvIV(*val) : INT32_MAX;
                val = av_fetch(sp, 2, 0);
                value = val ? *val : NULL;
            } else if (strcmp(opstr, "append") == 0) {
                op = UPDATE_OP_SPLICE;
                offset = INT32_MAX;
                length = 0;
            } else if (strcmp(opstr, "prepend") == 0) {
                op = UPDATE_OP_SPLICE;
                offset = 0;
                length = 0;
            } else if (strcmp(opstr, "cutbeg") == 0) {
                op = UPDATE_OP_SPLICE;
                offset = 0;
                length = SvIV(value);
                value = NULL;
            } else if (strcmp(opstr, "cutend") == 0) {
                op = UPDATE_OP_SPLICE;
                offset = -SvIV(value);
                length = SvIV(value);
                value = NULL;
            } else {
                croak("unknown op \"%s\"", opstr);
            }
        }

        bool range_check;
        switch (op) {
            case UPDATE_OP_AND:
            case UPDATE_OP_XOR:
            case UPDATE_OP_OR:
                f = toUPPER(f);
                range_check = false;
                break;
            case UPDATE_OP_ADD:
                f = toLOWER(f);
            default:
                range_check = true;
        }

        size_t size;
        void *data;
        switch (op) {
            case UPDATE_OP_SPLICE:
                if (value && SvOK(value)) {
                    data = SvPV(value, size);
                } else {
                    data = NULL;
                    size = 0;
                }
                break;
            case UPDATE_OP_DELETE:
                data = NULL;
                size = 0;
                break;
            default: {
                data = tbxs_sv_to_field(f, value, &size, errsv, range_check);
                if (data == NULL) {
                    tarantoolbox_update_ops_free(ops);
                    return NULL;
                }
            }
        }
        switch (op) {
            case UPDATE_OP_SPLICE:
                tarantoolbox_update_ops_add_splice(ops, field_num, offset, length, data, size);
                break;
            default:
                tarantoolbox_update_ops_add_op(ops, field_num, op, data, size);
        }
    }
    return ops;
}

static tarantoolbox_message_t *tbxs_update_message_init(tbxs_data_t *context) {
    tbns_t *ns = &context->inst->ns;
    HV *request = (HV *)SvRV(context->request);

    uint32_t flags = tbxs_fetch_want_result(request);
    SV **val;
    if ((val = hv_fetch(request, "_flags", 6, 0))) {
        if (!SvIOK(*val))
            croak("\"_flags\" should be an integer");
        flags |= SvUV(*val);
    }
    tarantoolbox_message_t *message = NULL;
    tarantoolbox_update_ops_t *ops = tbxs_fetch_update_ops(request, &ns->tuple, context->error);
    if (ops) {
        tarantoolbox_tuple_t *key = tbxs_fetch_key(request, ns, context->error);
        if (key) {
            message = tarantoolbox_update_init(ns->namespace, key, ops, flags);
            tarantoolbox_tuple_free(key);
        }
        tarantoolbox_update_ops_free(ops);
    }
    return message;
}

static tarantoolbox_message_t *tbxs_delete_message_init(tbxs_data_t *context) {
    tbns_t *ns = &context->inst->ns;
    HV *request = (HV *)SvRV(context->request);
    uint32_t flags = tbxs_fetch_want_result(request);
    tarantoolbox_tuple_t *key = tbxs_fetch_key(request, ns, context->error);
    if (key == NULL) return NULL;
    tarantoolbox_message_t *message = tarantoolbox_delete_init(ns->namespace, key, flags);
    tarantoolbox_tuple_free(key);
    return message;
}

static tarantoolbox_message_t *tbxs_call_message_init(tbxs_data_t *context) {
    tbfunc_t *func = &context->inst->func;
    HV *request = (HV *)SvRV(context->request);

    SV **val = hv_fetch(request, "tuple", 5, 0);
    if (!val) croak("\"tuple\" is required");
    SV *tuplesv = *val;

    tarantoolbox_tuple_t *tuple = tbxs_sv_to_tuple(tuplesv, &func->in, context->error);
    if (tuple == NULL) return NULL;
    tarantoolbox_message_t *message = tarantoolbox_call_init(func->name, tuple, 0);
    tarantoolbox_tuple_free(tuple);
    return message;
}

static tarantoolbox_message_t *tbxs_message_init(tbxs_data_t *context) {
    tarantoolbox_message_t *message;
    switch (context->type) {
        case SELECT:
            message = tbxs_select_message_init(context);
            break;
        case INSERT:
            message = tbxs_insert_message_init(context);
            break;
        case UPDATE_FIELDS:
            message = tbxs_update_message_init(context);
            break;
        case DELETE:
            message = tbxs_delete_message_init(context);
            break;
        case EXEC_LUA:
            message = tbxs_call_message_init(context);
            break;
        default:
            message = NULL;
    }
    if (message != NULL) {
        HV *reqhv = (HV *)SvRV(context->request);

        unsigned shard_num = 0;
        uint16_t microsharding = context->inst->microsharding;
        if (microsharding) {
            SV **val = hv_fetch(reqhv, "shard_num", 9, 0);
            if (!val)
                croak("\"shard_num\" should exist if microsharding is used");
            if (!(SvIOK(*val) || looks_like_number(*val)))
                croak("Invalid \"shard_num\" value: \"%s\"", SvPV_nolen(*val));
            UV num = SvUV(*val);
            if (num < 1 || num > microsharding)
                croak("\"shard_num\" should be between 1 and %u", microsharding);
            tarantoolbox_message_set_microshard(message, num);
            uint32_t shards = iproto_cluster_get_shard_count(iprotoxs_instance_to_cluster(context->inst->cluster));
            shard_num = (num % shards) + 1;
        }

        tbxs_set_message_cluster(message, context->inst->cluster);

        iproto_message_t *imessage = tarantoolbox_message_get_iproto_message(message);
        iproto_message_opts_t *opts = iproto_message_options(imessage);
        iprotoxs_parse_opts(opts, reqhv);
        if (shard_num)
            opts->shard_num = shard_num;

        if (context->callback)
            opts->callback = tbxs_callback;

        opts->data = context;
    }
    return message;
}

static void tbxs_selected_message_response(tbxs_data_t *context, HV *result) {
    HV *reqhv = (HV *)SvRV(context->request);
    bool replica;
    tarantoolbox_tuples_t *tuples = tarantoolbox_message_response(context->message, &replica);
    uint32_t ntuples = tarantoolbox_tuples_get_count(tuples);
    if (replica)
        (void)hv_store(result, "replica", 7, &PL_sv_yes, 0);

    uint32_t conf_count;
    tbtupleconf_t *conf_start;
    tarantoolbox_message_type_t type = tarantoolbox_message_type(context->message);
    if (type == EXEC_LUA) {
        tbfunc_t *func = &context->inst->func;
        conf_count = func->out_count;
        conf_start = func->out;
    } else {
        tbns_t *ns = &context->inst->ns;
        conf_count = 1;
        conf_start = &ns->tuple;
    }

    SV *tuplessv;
    SV *hash_by = conf_count == 1 ? tbxs_fetch_hash_by(reqhv, conf_start) : NULL;
    if (hash_by) {
        tuplessv = (SV *)newHV();
    } else {
        tuplessv = (SV *)newAV();
        av_extend((AV *)tuplessv, ntuples - 1);
    }

    bool want_raw = tbxs_fetch_raw(reqhv);
    uint32_t conf_num = 0;
    for (uint32_t i = 0; i < ntuples; i++) {
        tbtupleconf_t *conf = &conf_start[conf_num];
        tarantoolbox_tuple_t *tuple = tarantoolbox_tuples_get_tuple(tuples, i);
        AV *tupleav = tbxs_tuple_to_av(tuple, conf->format);

        SV *tuplesv;
        if (conf->fields && !want_raw) {
            tuplesv = (SV *)tbxs_tuple_av_to_hv(tupleav, conf->fields);
        } else {
            tuplesv = (SV *)tupleav;
        }

        if (hash_by) {
            IV id = SvIV(hash_by);
            SV **val = av_fetch(tupleav, id, 0);
            if (!val) croak("tuple is too short to contain \"hash_by\" keys");
            (void)hv_store_ent((HV *)tuplessv, *val, newRV_noinc(tuplesv), 0);
        } else {
            (void)av_store((AV *)tuplessv, i, newRV_noinc(tuplesv));
        }

        if (conf_num < conf_count - 1)
            conf_num++;
    }
    SV *tuplesrv = newRV_noinc(tuplessv);
    if (!hv_store(result, "tuples", 6, tuplesrv, 0))
        SvREFCNT_dec(tuplesrv);
}

static void tbxs_affected_message_response(tbxs_data_t *context, HV *result) {
    tbns_t *ns = &context->inst->ns;
    HV *reqhv = (HV *)SvRV(context->request);
    tarantoolbox_tuples_t *tuples = tarantoolbox_message_response(context->message, NULL);
    uint32_t ntuples = tarantoolbox_tuples_get_count(tuples);
    SV *tuplesv;
    if (tarantoolbox_tuples_has_tuples(tuples) && ntuples == 1) {
        AV *tupleav = tbxs_tuple_to_av(tarantoolbox_tuples_get_tuple(tuples, 0), ns->tuple.format);
        SV *tupleahv = ns->tuple.fields && !tbxs_fetch_raw(reqhv) ? (SV *)tbxs_tuple_av_to_hv(tupleav, ns->tuple.fields) : (SV *)tupleav;
        tuplesv = newRV_noinc(tupleahv);
    } else {
        tuplesv = newSVuv(ntuples);
    }
    if (!hv_store(result, "tuple", 5, tuplesv, 0))
        SvREFCNT_dec(tuplesv);
}

static tbxs_data_t *tbxs_context_init(SV *instance, SV *request) {
    if (!(SvROK(request) && SvTYPE(SvRV(request)) == SVt_PVHV))
        croak("Message should be a HASHREF");

    tbxs_data_t *context;
    Newxz(context, 1, tbxs_data_t);
    context->request = SvREFCNT_inc(request);
    context->error = newSV(0);

    context->type = tbxs_fetch_type(request);

    const char *instkey = context->type == EXEC_LUA ? "function" : "namespace";
    int instlen = context->type == EXEC_LUA ? 8 : 9;

    SV **val = hv_fetch((HV *)SvRV(request), instkey, instlen, 0);
    SV *inst = val ? *val : instance;
    if (!instance) croak("\"%s\" should be specified", instkey);
    context->instance = SvREFCNT_inc(inst);
    context->inst = context->type == EXEC_LUA ? tbfunc_inst(inst) : tbns_inst(inst);

    SV *callback = tbxs_fetch_callback(request);
    if (callback)
        context->callback = SvREFCNT_inc(callback);

    context->message = tbxs_message_init(context);

    return context;
}

static void tbxs_context_free(tbxs_data_t *context) {
    if (context->message)
        tarantoolbox_message_free(context->message);
    SvREFCNT_dec(context->instance);
    SvREFCNT_dec(context->request);
    if (context->callback)
        SvREFCNT_dec(context->callback);
    Safefree(context);
}

static SV *tbxs_context_response(tbxs_data_t *context) {
    HV *reqhv = (HV *)SvRV(context->request);
    SV **val = hv_fetch(reqhv, "inplace", 7, 0);
    SV *result = val && SvTRUE(*val) ? SvREFCNT_inc(context->request) : newRV_noinc((SV *)newHV());
    HV *reshv = (HV *)SvRV(result);
    SV *errsv = context->error;
    tarantoolbox_message_t * message = context->message;
    if (message) {
        const char *error_string;
        tarantoolbox_error_t error = tarantoolbox_message_error(message, &error_string);
        sv_setpv(errsv, error_string);
        sv_setuv(errsv, error);
        SvPOK_on(errsv);

        if (error == ERR_CODE_OK) {
            tarantoolbox_message_type_t type = tarantoolbox_message_type(message);
            switch (type) {
                case SELECT:
                case EXEC_LUA:
                    tbxs_selected_message_response(context, reshv);
                    break;
                default:
                    tbxs_affected_message_response(context, reshv);
            }
        }
    } else if (!SvIOK(errsv)) {
        sv_setuv(errsv, ERR_CODE_INVALID_REQUEST);
        SvPOK_on(errsv);
    }
    if (!hv_store(reshv, "error", 5, errsv, 0))
        SvREFCNT_dec(errsv);
    return result;
}

static SV *tbxs_context_retval(tbxs_data_t *context) {
    if (context->callback) {
        if (!context->message)
            tbxs_context_callback(context);
        return &PL_sv_undef;
    }
    SV *retval = tbxs_context_response(context);
    tbxs_context_free(context);
    return retval;
}

static void tbtupleconf_set_format(tbtupleconf_t *conf, SV *value) {
    if (!SvOK(value))
        return;
    else if (!SvPOK(value))
        croak("\"format\" should be string");
    char *format0 = SvPV_nolen(value);
    SV *formatsv = newSVpv("", 0);
    SvGROW(formatsv, SvCUR(value) + 1);
    char *format = SvPV_nolen(formatsv);
    SV *test = newSViv(0);
    STRLEN j = 0;
    for (STRLEN i = 0; i < SvCUR(value); i++) {
        if (!isSPACE(format0[i])) {
            size_t size;
            tbxs_sv_to_field(format0[i], test, &size, NULL, true);
            format[j++] = format0[i];
        }
    }
    SvCUR_set(formatsv, j);
    *SvEND(formatsv) = '\0';
    SvREFCNT_dec(test);
    conf->format = formatsv;
}

static void tbtupleconf_set_fields(tbtupleconf_t *conf, SV *value) {
    if (!SvOK(value))
        return;
    else if (!(SvROK(value) && SvTYPE(SvRV(value)) == SVt_PVAV))
        croak("\"fields\" should be an ARRAYREF");
    AV *fields = (AV *)SvREFCNT_inc(SvRV(value));
    HV *field_id_by_name = newHV();
    for (I32 j = 0; j <= av_len(fields); j++) {
        SV **val = av_fetch(fields, j, 0);
        if (!SvPOK(*val))
            croak("field's name should be a string");
        STRLEN namelen;
        char *name = SvPV(*val, namelen);
        if (hv_exists(field_id_by_name, name, namelen))
            croak("field's name should be unique");
        (void)hv_store(field_id_by_name, name, namelen, newSViv(j), 0);
    }
    conf->fields = fields;
    conf->field_id_by_name = field_id_by_name;
}

void tbns_set_indexes(tbns_t *ns, AV *indexes) {
    ns->indexes = newAV();
    av_extend(ns->indexes, av_len(indexes));
    ns->index_id_by_name = newHV();
    if (ns->tuple.format) {
        ns->index_format = newAV();
        av_extend(ns->index_format, av_len(indexes));
    }
    if (ns->tuple.fields) {
        ns->index_fields = newAV();
        av_extend(ns->index_fields, av_len(indexes));
    }
    bool has_default = false;
    for (I32 i = 0; i <= av_len(indexes); i++) {
        SV **val = av_fetch(indexes, i, 0);
        if (!(SvROK(*val) && SvTYPE(SvRV(*val)) == SVt_PVHV))
            croak("each index should be a HASHREF");
        HV *index = (HV *)SvRV(*val);

        val = hv_fetch(index, "name", 4, 0);
        if (!val)
            croak("index should contain a \"name\"");
        if (!SvPOK(*val))
            croak("index's \"name\" should be a string");
        STRLEN namelen;
        char *name = SvPV(*val, namelen);
        if (hv_exists(ns->index_id_by_name, name, namelen))
            croak("index's \"name\" should be unique");

        val = hv_fetch(index, "keys", 4, 0);
        if (!val)
            croak("index should contain \"keys\"");
        if (!(SvROK(*val) && SvTYPE(SvRV(*val)) == SVt_PVAV))
            croak("index \"keys\" should be an ARRAYREF");
        AV *keys = (AV *)SvRV(*val);

        AV *idkeys = newAV();
        av_extend(idkeys, av_len(keys));
        for (I32 j = 0; j <= av_len(keys); j++) {
            val = av_fetch(keys, j, 0);
            if (SvIOK(*val)) {
                (void)av_store(idkeys, j, SvREFCNT_inc(*val));
            } else if (SvPOK(*val)) {
                if (ns->tuple.field_id_by_name == NULL)
                    croak("\"fields\" are required if you use names instead of ids in \"indexes\"");
                STRLEN namelen;
                char *name = SvPV(*val, namelen);
                val = hv_fetch(ns->tuple.field_id_by_name, name, namelen, 0);
                if (!val)
                    croak("no \"%s\" in \"fields\"", name);
                (void)av_store(idkeys, j, SvREFCNT_inc(*val));
            } else {
                croak("each key should be a string or an integer");
            }
        }

        (void)av_store(ns->indexes, i, newRV_noinc((SV *)idkeys));
        (void)hv_store(ns->index_id_by_name, name, namelen, newSVuv(i), 0);

        if ((val = hv_fetch(index, "default", 7, 0))) {
            if (SvTRUE(*val)) {
                if (has_default)
                    croak("only one default index allowed");
                ns->default_index = i;
                has_default = true;
            }
        }

        if (ns->tuple.format) {
            SV *index_format = newSVpvn("", 0);
            char *fmt = SvGROW(index_format, av_len(idkeys) + 2);
            STRLEN formatlen;
            char *format = SvPV(ns->tuple.format, formatlen);
            for (I32 j = 0; j <= av_len(idkeys); j++) {
                SV **val = av_fetch(idkeys, j, 0);
                IV id = SvIV(*val);
                fmt[j] = '&';
                for (STRLEN f = 0; f < formatlen; f++) {
                    if (format[f] == ' ') {
                        id++;
                    } else if (f == id) {
                        fmt[j] = format[f];
                    }
                }
            }
            SvCUR_set(index_format, av_len(idkeys) + 1);
            *SvEND(index_format) = '\0';
            (void)av_store(ns->index_format, i, index_format);
        }

        if (ns->tuple.fields) {
            AV *fields = newAV();
            av_extend(fields, av_len(idkeys));
            for (I32 j = 0; j <= av_len(idkeys); j++) {
                SV **val = av_fetch(ns->tuple.fields, j, 0);
                if (!val) croak("field in index %"IVdf" has no name", i);
                (void)av_store(fields, j, SvREFCNT_inc(*val));
            }
            (void)av_store(ns->index_fields, i, newRV_noinc((SV *)fields));
        }
    }
}

static void tbfunc_set_out(tbfunc_t *func, SV *format, SV *fields) {
    if (format == NULL && fields == NULL) {
        func->out_count = 1;
        Newxz(func->out, 1, tbtupleconf_t);
    } else if (format) {
        if (SvPOK(format)) {
            func->out_count = 1;
            Newxz(func->out, 1, tbtupleconf_t);
            tbtupleconf_set_format(func->out, format);
            if (fields)
                tbtupleconf_set_fields(func->out, fields);
        } else if (SvROK(format) && SvTYPE(SvRV(format)) == SVt_PVAV) {
            AV *formatav = (AV *)SvRV(format);
            AV *fieldsav = NULL;
            I32 max_len = av_len(formatav);
            if (fields) {
                if (!(SvROK(fields) && SvTYPE(SvRV(fields)) == SVt_PVAV))
                    croak("\"out_fields\" should be an ARRAYREF");
                fieldsav = (AV *)SvRV(fields);
                I32 len = av_len(fieldsav);
                if (len > max_len) max_len = len;
            }
            max_len++;
            func->out_count = max_len;
            Newxz(func->out, max_len, tbtupleconf_t);
            for (uint32_t i = 0; i < max_len; i++) {
                SV **sv = av_fetch(formatav, i, 0);
                if (sv) tbtupleconf_set_format(&func->out[i], *sv);
                if (fieldsav) {
                    sv = av_fetch(fieldsav, i, 0);
                    if (sv) tbtupleconf_set_fields(&func->out[i], *sv);
                }
            }
        } else {
            croak("\"out_format\" should be a string or an ARRAYREF");
        }
    } else {
        if (!(SvROK(fields) && SvTYPE(SvRV(fields)) == SVt_PVAV))
            croak("\"out_fields\" should be an ARRAYREF");
        AV *fieldsav = (AV *)SvRV(fields);
        I32 len = av_len(fieldsav) + 1;
        func->out_count = len;
        Newxz(func->out, len, tbtupleconf_t);
        for (uint32_t i = 0; i < len; i++) {
            SV **sv = av_fetch(fieldsav, i, 0);
            if (sv) tbtupleconf_set_fields(&func->out[i], *sv);
        }
    }
}

static AV *tbxs_bulk(SV *instance, AV *list, struct timeval *timeout) {
    I32 listsize = av_len(list) + 1;
    tbxs_data_t **contexts;
    Newx(contexts, listsize, tbxs_data_t *);
    iproto_message_t **imessages;
    Newx(imessages, listsize, iproto_message_t *);
    uint32_t nmessages = 0;
    for (I32 i = 0; i < listsize; i++) {
        SV **sv = av_fetch(list, i, 0);
        tbxs_data_t *context = tbxs_context_init(instance, *sv);
        if (context->message)
            imessages[nmessages++] = tarantoolbox_message_get_iproto_message(context->message);
        contexts[i] = context;
    }
    iproto_bulk(imessages, nmessages, timeout);
    AV *result = newAV();
    for (I32 i = 0; i < listsize; i++)
        av_push(result, tbxs_context_retval(contexts[i]));
    Safefree(imessages);
    Safefree(contexts);
    return (AV *)sv_2mortal((SV *)result);
}

static SV *tbxs_do(SV *instance, SV *request, struct timeval *timeout) {
    tbxs_data_t *context = tbxs_context_init(instance, request);
    if (context->message) {
        iproto_message_t *imessage = tarantoolbox_message_get_iproto_message(context->message);
        iproto_do(imessage, timeout);
    }
    return tbxs_context_retval(context);
}

MODULE = MR::Tarantool::Box::XS		PACKAGE = MR::Tarantool::Box::XS		PREFIX = ns_

PROTOTYPES: ENABLE

BOOT:
    load_module(0, newSVpvn("MR::IProto::XS", 14), NULL, NULL);
    tarantoolbox_initialize();
    HV *stash = gv_stashpv("MR::Tarantool::Box::XS", 1);
#define TBXS_CONST(s, ...) do { \
        SV *sv = newSVuv(s); \
        sv_setpv(sv, tarantoolbox_error_string(s)); \
        SvIOK_on(sv); \
        newCONSTSUB(stash, #s, sv); \
    } while (0);
    TARANTOOLBOX_ALL_ERROR_CODES(TBXS_CONST);
#undef TBXS_CONST
#define TBXS_CONST(s, ...) newCONSTSUB(stash, #s, newSVuv(s));
    TARANTOOLBOX_LOGMASK(TBXS_CONST);
#undef TBXS_CONST
    MY_CXT_INIT;
    MY_CXT.namespaces = newHV();
    MY_CXT.functions = newHV();
#ifdef WITH_CP1251
    MY_CXT.cp1251 = tbxs_find_cp1251();
#endif
#ifdef WITH_MATH_INT64
    PERL_MATH_INT64_LOAD_OR_CROAK;
#endif

void
ns_set_logmask(klass, mask)
        unsigned mask
    CODE:
        tarantoolbox_set_logmask(mask);

MR::Tarantool::Box::XS
ns_new(klass, ...)
        SV *klass
    ALIAS:
        create_singleton = 1
    CODE:
        if (items % 2 == 0)
            croak("Odd number of elements in hash assignment");
        tbinst_t *self;
        Newxz(self, 1, tbinst_t);
        bool has_namespace = false;
        AV *indexes = NULL;
        for (int i = 1; i < items; i += 2) {
            char *key = SvPV_nolen(ST(i));
            SV *value = ST(i + 1);
            if (strcmp(key, "iproto") == 0) {
                SV *instance = iprotoxs_instance(value);
                if (!instance)
                    croak("\"iproto\" should be an instance or a singleton of type MR::IProto::XS");
                self->cluster = SvREFCNT_inc(instance);
            } else if (strcmp(key, "microsharding") == 0) {
                if (!(SvIOK(value) || looks_like_number(value)))
                    croak("\"microsharding\" should be an integer");
                self->microsharding = SvUV(value);
            } else if (strcmp(key, "namespace") == 0) {
                if (!(SvIOK(value) || looks_like_number(value)))
                    croak("\"namespace\" should be an integer");
                self->ns.namespace = SvUV(value);
                has_namespace = true;
            } else if (strcmp(key, "format") == 0) {
                tbtupleconf_set_format(&self->ns.tuple, value);
            } else if (strcmp(key, "fields") == 0) {
                tbtupleconf_set_fields(&self->ns.tuple, value);
            } else if (strcmp(key, "indexes") == 0) {
                if (!(SvROK(value) && SvTYPE(SvRV(value)) == SVt_PVAV))
                    croak("\"indexes\" should be an ARRAYREF");
                indexes = (AV *)SvRV(value);
            }
        }
        if (!self->cluster)
            croak("\"iproto\" is required");
        if (!has_namespace)
            croak("\"namespace\" is required");
        if (indexes)
            tbns_set_indexes(&self->ns, indexes);
        RETVAL = newSV(0);
        sv_setref_pv(RETVAL, SvPV_nolen(klass), self);
        if (ix == 1) {
            dMY_CXT;
            if (hv_exists_ent(MY_CXT.namespaces, klass, 0))
                croak("singleton %s already initialized", SvPV_nolen(klass));
            (void)hv_store_ent(MY_CXT.namespaces, klass, SvREFCNT_inc(RETVAL), 0);
        }
    OUTPUT:
        RETVAL

void
ns_DESTROY(namespace)
        MR::Tarantool::Box::XS namespace
    CODE:
        tbinst_t *self = tbns_inst(namespace);
        if (!self) croak("DESTROY should be called as an instance method");
        if (!PL_dirty) {
            SvREFCNT_dec(self->cluster);
            SvREFCNT_dec(self->ns.tuple.fields);
            SvREFCNT_dec(self->ns.tuple.field_id_by_name);
            SvREFCNT_dec(self->ns.tuple.format);
            SvREFCNT_dec(self->ns.indexes);
            SvREFCNT_dec(self->ns.index_id_by_name);
            SvREFCNT_dec(self->ns.index_format);
            SvREFCNT_dec(self->ns.index_fields);
        }
        Safefree(self);

MR::Tarantool::Box::XS
ns_remove_singleton(klass)
        SV *klass
    CODE:
        if (!SvPOK(klass))
            croak("remove_singleton() should be called as a class method");
        dMY_CXT;
        SV *namespace = hv_delete_ent(MY_CXT.namespaces, klass, 0, 0);
        RETVAL = namespace ? SvREFCNT_inc(namespace) : &PL_sv_undef;
    OUTPUT:
        RETVAL

MR::Tarantool::Box::XS
ns_instance(klass)
        SV *klass
    CODE:
        SV *instance = tbns_instance(klass);
        RETVAL = instance ? SvREFCNT_inc(instance) : &PL_sv_undef;
    OUTPUT:
        RETVAL

AV *
ns_bulk(namespace, list, ...)
        MR::Tarantool::Box::XS namespace
        AV *list
    CODE:
        iprotoxs_call_timeout(timeout, 2);
        RETVAL = tbxs_bulk(namespace, list, timeout);
    OUTPUT:
        RETVAL

SV *
ns_do(namespace, request, ...)
        MR::Tarantool::Box::XS namespace
        SV *request
    CODE:
        iprotoxs_call_timeout(timeout, 2);
        RETVAL = tbxs_do(namespace, request, timeout);
    OUTPUT:
        RETVAL

MR::IProto::XS
ns_iproto(namespace)
        MR::Tarantool::Box::XS namespace
    CODE:
        tbinst_t *self = tbns_inst(namespace);
        if (!self)
            croak("iproto() should be called as an instance or a singleton method");
        RETVAL = SvREFCNT_inc(self->cluster);
    OUTPUT:
        RETVAL


MODULE = MR::Tarantool::Box::XS		PACKAGE = MR::Tarantool::Box::XS::Function		PREFIX = fn_

MR::Tarantool::Box::XS::Function
fn_new(klass, ...)
        SV *klass
    ALIAS:
        create_singleton = 1
    CODE:
        if (items % 2 == 0)
            croak("Odd number of elements in hash assignment");
        tbinst_t *self;
        Newxz(self, 1, tbinst_t);
        self->is_func = true;
        SV *out_format = NULL;
        SV *out_fields = NULL;
        for (int i = 1; i < items; i += 2) {
            char *key = SvPV_nolen(ST(i));
            SV *value = ST(i + 1);
            if (strcmp(key, "iproto") == 0) {
                SV *instance = iprotoxs_instance(value);
                if (!instance)
                    croak("\"iproto\" should be an instance or a singleton of type MR::IProto::XS");
                self->cluster = SvREFCNT_inc(instance);
            } else if (strcmp(key, "microsharding") == 0) {
                if (!(SvIOK(value) || looks_like_number(value)))
                    croak("\"microsharding\" should be an integer");
                self->microsharding = SvUV(value);
            } else if (strcmp(key, "name") == 0) {
                if (!SvPOK(value))
                    croak("\"name\" should be a string");
                self->func.name = savepv(SvPV_nolen(value));
            } else if (strcmp(key, "in_format") == 0) {
                tbtupleconf_set_format(&self->func.in, value);
            } else if (strcmp(key, "in_fields") == 0) {
                tbtupleconf_set_fields(&self->func.in, value);
            } else if (strcmp(key, "out_format") == 0) {
                out_format = value;
            } else if (strcmp(key, "out_fields") == 0) {
                out_fields = value;
            }
        }
        if (!self->cluster)
            croak("\"iproto\" is required");
        if (!self->func.name)
            croak("\"name\" is required");
        tbfunc_set_out(&self->func, out_format, out_fields);
        RETVAL = newSV(0);
        sv_setref_pv(RETVAL, SvPV_nolen(klass), self);
        if (ix == 1) {
            dMY_CXT;
            if (hv_exists_ent(MY_CXT.functions, klass, 0)) // FIXME not klass, but caller::method
                croak("singleton %s already initialized", SvPV_nolen(klass));
            (void)hv_store_ent(MY_CXT.functions, klass, SvREFCNT_inc(RETVAL), 0);
        }
    OUTPUT:
        RETVAL

void
fn_DESTROY(function)
        MR::Tarantool::Box::XS::Function function
    CODE:
        tbinst_t *self = tbfunc_inst(function);
        if (!self)
            croak("DESTROY should be called as an instance method");
        if (!PL_dirty) {
            SvREFCNT_dec(self->cluster);
            SvREFCNT_dec(self->func.in.fields);
            SvREFCNT_dec(self->func.in.field_id_by_name);
            SvREFCNT_dec(self->func.in.format);
            for (uint32_t i = 0; i < self->func.out_count; i++) {
                SvREFCNT_dec(self->func.out[i].fields);
                SvREFCNT_dec(self->func.out[i].field_id_by_name);
                SvREFCNT_dec(self->func.out[i].format);
            }
        }
        Safefree(self->func.name);
        Safefree(self->func.out);
        Safefree(self);

MR::Tarantool::Box::XS::Function
fn_remove_singleton(klass)
        SV *klass
    CODE:
        if (!SvPOK(klass))
            croak("instance() should be called as a class method");
        dMY_CXT;
        SV *function = hv_delete_ent(MY_CXT.functions, klass, 0, 0);
        RETVAL = function ? SvREFCNT_inc(function) : &PL_sv_undef;
    OUTPUT:
        RETVAL

MR::Tarantool::Box::XS::Function
fn_instance(klass)
        SV *klass
    CODE:
        SV *instance = tbfunc_instance(klass);
        RETVAL = instance ? SvREFCNT_inc(instance) : &PL_sv_undef;
    OUTPUT:
        RETVAL

AV *
fn_bulk(function, list, ...)
        MR::Tarantool::Box::XS::Function function
        AV *list
    CODE:
        iprotoxs_call_timeout(timeout, 2);
        RETVAL = tbxs_bulk(function, list, timeout);
    OUTPUT:
        RETVAL

SV *
fn_do(function, request, ...)
        MR::Tarantool::Box::XS::Function function
        SV *request
    CODE:
        iprotoxs_call_timeout(timeout, 2);
        RETVAL = tbxs_do(function, request, timeout);
    OUTPUT:
        RETVAL

MR::IProto::XS
fn_iproto(function)
        MR::Tarantool::Box::XS::Function function
    CODE:
        tbinst_t *self = tbfunc_inst(function);
        if (!self)
            croak("iproto() should be called as an instance or a singleton method");
        RETVAL = SvREFCNT_inc(self->cluster);
    OUTPUT:
        RETVAL
