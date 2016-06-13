%{!?__python2: %global __python2 %__python}
%{!?python2_sitelib: %global python2_sitelib %(%{__python2} -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")}

%if 0%{?fedora}
%bcond_without python3
%else
%bcond_with python3
%endif

%if %{?rhel:1}%{!?rhel:0}
# On RedHat Enterprise Linux, there is no python3, and the py2_build
# and py2_install macros are also not available.  We define them here.
%define py_setup setup.py
%define py_shbang_opts -s
%define py2_build %{expand:\
CFLAGS="%{optflags}" %{__python2} %{py_setup} %{?py_setup_args} build --executable="%{__python2} %{py2_shbang_opts}" %{?1}\
}
%define py2_install %{expand:\
CFLAGS="%{optflags}" %{__python2} %{py_setup} %{?py_setup_args} install -O1 --skip-build --root %{buildroot} %{?1}\
}
# We need an extra dependecy
BuildRequires:	python-setuptools
%endif

Name:		python-monetdb
Epoch:		1
Version:	1.0
Release:	1%{?dist}
Summary:	Pure Python database driver for MonetDB/SQL

License:	MPLv2.0
URL:		http://www.monetdb.org/
Source0:	http://dev.monetdb.org/downloads/python/pymonetdb-%{version}.tar.gz

BuildArch:	noarch
BuildRequires:	python2-devel
BuildRequires:	python-six
%if %{with python3}
BuildRequires:	python3-devel
BuildRequires:	python3-six
%endif # with python3

Requires:	python-six

%description
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL frontend.

This package contains the files needed to use MonetDB from a Python
program.  This package is for Python version 2.  If you want to use
Python version 3, you need python3-monetdb.

%if %{with python3}
%package     -n python3-monetdb
Summary:	Pure Python database driver for MonetDB/SQL
Requires:	python3-six

%description -n python3-monetdb
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL frontend.

This package contains the files needed to use MonetDB from a Python3
program.  This package is for Python version 3.  If you want to use
Python version 2, you need %{name}.

%endif # with python3

%prep
%autosetup -n pymonetdb-%{version}


%build
%py2_build

%if %{with python3}
%py3_build
%endif # with python3


%install
rm -rf %{buildroot}
# Must do the python3 install first because the scripts in /usr/bin are
# overwritten with every setup.py install (and we want the python2 version
# to be the default for now).
%if %{with python3}
%py3_install
%endif # with python3

%py2_install


%files
%doc
%{python2_sitelib}/*

%if %{with python3}
%files -n python3-monetdb
%doc
%{python3_sitelib}/*
%endif # with python3


%changelog
* Tue Mar  8 2016 Sjoerd Mullender <sjoerd@acm.org>
- The Python interface to MonetDB is now a separate package.
