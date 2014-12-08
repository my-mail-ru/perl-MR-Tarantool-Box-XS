%bcond_with range_check
%ifarch i386
%bcond_without math_int64
%else
%bcond_with math_int64
%endif
%bcond_without cp1251

%define _prefix /usr/local

%define __libtarantoolbox_version 20130905.1741
%define __libtarantoolboxdevel_version 20131118.1649
%define __iprotoxs_version 20130911.1746

Name:           perl-MR-Tarantool-Box-XS
Version:        %{__version}
Release:        1%{?dist}

Summary:        high performance tarantool/octopus box client
License:        BSD
Group:          MAIL.RU

BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-buildroot
BuildRequires:  git
BuildRequires:  perl(ExtUtils::MakeMaker), perl(Test::More)
BuildRequires:  libtarantoolbox-devel >= %{__libtarantoolboxdevel_version}
BuildRequires:  libtarantoolbox >= %{__libtarantoolbox_version}
BuildRequires:  perl-MR-IProto-XS-devel >= %{__iprotoxs_version}
BuildRequires:  perl-MR-IProto-XS >= %{__iprotoxs_version}
Requires:       perl(:MODULE_COMPAT_%(eval "`%{__perl} -V:version`"; echo $version))
Requires:       libtarantoolbox >= %{__libtarantoolbox_version}
Requires:       perl-MR-IProto-XS >= %{__iprotoxs_version}
%if %{with math_int64}
BuildRequires:  perl-Math-Int64-devel
Requires:       perl-Math-Int64
%endif
AutoReq:        0

%description
high performance tarantool/octopus box client. Built from revision %{__revision}.

%prep
%setup -n iproto/tarantool/xs
sed -i "s/^our \$VERSION = '[0-9\.]\+';$/our \$VERSION = '%{version}';/" lib/MR/Tarantool/Box/XS.pm

%build
%{__perl} Makefile.PL %{?with_math_int64:--math-int64} %{?with_range_check:--range-check} %{?with_cp1251:--cp1251} INSTALLDIRS=vendor
make %{?_smp_mflags}

%install
make pure_install PERL_INSTALL_ROOT=$RPM_BUILD_ROOT
find $RPM_BUILD_ROOT -type f -name .packlist -exec rm -f {} ';'
find $RPM_BUILD_ROOT -depth -type d -exec rmdir {} 2>/dev/null ';'
chmod -R u+w $RPM_BUILD_ROOT/*

%files
%defattr(-,root,root,-)
%{perl_vendorarch}/*
%{_mandir}/*/*

%changelog
* Fri Dec 07 2012 Aleksey Mashanov <a.mashanov@corp.mail.ru>
- initial version
