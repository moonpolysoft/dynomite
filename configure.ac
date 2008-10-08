dnl - The second argument of AC_INIT is the package version number.
dnl - It should be specified as major.minor.revision [1.3.0]
dnl - The third argument, should be replaced with the package 
dnl - maintainers email address.

AC_INIT([dynomite], [0.3.0], [cliff@powerset.com])
SHORT_DESCRIPTION="The dynomite datastore.  It is a highly available, low latency key value store.  Word."

dnl - The following prerequisites line can be used to specify
dnl - the exact, or minimum, versions of packages on which this
dnl - package will depend (for build _and_ runtime)

RUNTIME_PREREQUISITES="powerset_erlang powerset_ruby"

dnl - The following prerequisites line can be used to specify
dnl - the exact, or minimum, versions of packages on which this
dnl - package will depend (for build only)

#BUILD_PREREQUISITES=""


dnl -- [ end basic configuration ]



AC_CONFIG_AUX_DIR([bones-common])

AC_CANONICAL_SYSTEM
AC_PREFIX_DEFAULT([/p])

AX_INIT

AM_INIT_AUTOMAKE
AM_MAINTAINER_MODE

AC_SUBST(SHORT_DESCRIPTION)



dnl
dnl - check for common utilities
dnl

AC_PROG_AWK
AC_PROG_EGREP

AC_PATH_PROG(TEE, tee)

AC_CHECK_PROG(HAVE_PERL, perl, yes, 
              [AC_MSG_ERROR([the Powerset build environment requires perl])])


dnl
dnl - determine whether or not to use pkg-config
dnl

PKG_PROG_PKG_CONFIG

AC_ARG_ENABLE([pkgconfig],
  AC_HELP_STRING([--enable-pkgconfig],
                 [check build deps with pkg-config (default: auto)]),
                 [dopkgconfig="$enableval"], [dopkgconfig="maybe"])

case "$dopkgconfig" in
    yes) if test -n "$PKG_CONFIG"; then
           using_pkgconfig=yes
         else
           AC_MSG_ERROR([pkg-config not found])
         fi
         ;;
  maybe) if test -n "$PKG_CONFIG"; then
           using_pkgconfig=yes
         fi
         ;;
esac
    
AM_CONDITIONAL([HAVE_PKGCONFIG], [test -n "$PKG_CONFIG"])

dnl
dnl - extract build pkgconfig and non-pkgconfig prereqs
dnl

AX_PACKAGE_LIST([BUILD], [$BUILD_PREREQUISITES])

if test -n "$using_pkgconfig"; then
  AX_SPLIT_PREREQUISITES([BUILD_PREREQUISITES], [$BUILD_PREREQUISITES])
  if test -n "$BUILD_PREREQUISITES_PKGCONFIG"; then
    PKG_CHECK_MODULES([BUILD_PREREQUISITES], [$BUILD_PREREQUISITES_PKGCONFIG])
  fi
fi

AC_SUBST(BUILD_PREREQUISITES_NONPKGCONFIG)
AC_SUBST(BUILD_PREREQUISITES_PKGCONFIG)

dnl
dnl - extract runtime pkgconfig and non-pkgconfig prereqs
dnl

AX_PACKAGE_LIST([RUNTIME], [$RUNTIME_PREREQUISITES])

if test -n "$using_pkgconfig"; then
  AX_SPLIT_PREREQUISITES([RUNTIME_PREREQUISITES], [$RUNTIME_PREREQUISITES])
  if test -n "$RUNTIME_PREREQUISITES_PKGCONFIG"; then
    PKG_CHECK_MODULES([RUNTIME_PREREQUISITES], [$RUNTIME_PREREQUISITES_PKGCONFIG])
  fi
fi

AC_SUBST(RUNTIME_PREREQUISITES_NONPKGCONFIG)
AC_SUBST(RUNTIME_PREREQUISITES_PKGCONFIG)



dnl
dnl - see if we can create packages
dnl

AC_CHECK_PROG(HAVE_PP_CREATE, pp_create.rb, yes, no)
AM_CONDITIONAL(HAVE_PP_CREATE, test $HAVE_PP_CREATE = yes)

dnl
dnl - Allow override of auto-detect package architecture
dnl

AC_ARG_WITH([package-target],
            AC_HELP_STRING([--with-package-target=ARG],
                           [override the detected rpm architecture]),
            [cv_force_arch="$withval"],
            [cv_force_arch="$PACKAGE_TARGET"])

AM_CONDITIONAL(FORCE_PKG_ARCH, test -n "$cv_force_arch")
AC_SUBST(cv_force_arch)


dnl
dnl - internal use only
dnl

AC_ARG_WITH([internal-release],
            AC_HELP_STRING([--with-internal-release=ARG],
                           [internal use only]),
            [PACKAGE_RELEASE=$withval],
            [PACKAGE_RELEASE=0])

AC_SUBST(PACKAGE_RELEASE)

dnl
dnl - build the PKG_DEPENDS list for the yaml
dnl

AX_INIT_DEPENDENCY_SOLVER
                       

AX_GENERATE_PKG_DEPENDS([RUNTIME_DEPENDS_LIST],
                        [$RUNTIME_PACKAGES],
                        [$pkg_name: {min: '$pkg_version', max: '$pkg_version_max'}])

AX_GENERATE_PKG_DEPENDS([BUILD_DEPENDS_LIST],
                        [$RUNTIME_PACKAGES $BUILD_PACKAGES],
                        [$pkg_name: {eq: '$pkg_version-$pkg_release'}],
                        [want-closure])




dnl - Uncomment if you need ruby support
#AM_PATH_RUBY

dnl - Uncomment if you need python support
#AM_PATH_PYTHON


AC_CONFIG_FILES([
    Makefile
    build-yaml-template.yaml
    yaml-template.yaml
    pkgconfig-template.pc
])

AC_OUTPUT
AX_FINI
