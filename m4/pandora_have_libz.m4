dnl  Copyright (C) 2010 Trond Norbye
dnl This file is free software; Trond Norbye
dnl gives unlimited permission to copy and/or distribute it,
dnl with or without modifications, as long as this notice is preserved.

AC_DEFUN([_PANDORA_SEARCH_LIBZ],[
  AC_REQUIRE([AC_LIB_PREFIX])

  dnl --------------------------------------------------------------------
  dnl  Check for libz
  dnl --------------------------------------------------------------------

  AC_ARG_ENABLE([libz],
    [AS_HELP_STRING([--disable-libz],
      [Build with libz support @<:@default=on@:>@])],
    [ac_enable_libz="$enableval"],
    [ac_enable_libz="yes"])

  AS_IF([test "x$ac_enable_libz" = "xyes"],[
    AC_LIB_HAVE_LINKFLAGS(z,,[
      #include <zlib.h>
    ],[
      z_stream stream = { .zalloc = Z_NULL,.zfree = Z_NULL, .opaque = Z_NULL };
    ])
  ],[
    ac_cv_libz="no"
  ])

  AM_CONDITIONAL(HAVE_LIBZ, [test "x${ac_cv_libz}" = "xyes"])
])

AC_DEFUN([PANDORA_HAVE_LIBZ],[
  AC_REQUIRE([_PANDORA_SEARCH_LIBZ])
])

AC_DEFUN([PANDORA_REQUIRE_LIBZ],[
  AC_REQUIRE([_PANDORA_SEARCH_LIBZ])
  AS_IF([test "x${ac_cv_libz}" = "xno"],
    AC_MSG_ERROR([libz is required for ${PACKAGE}]))
])
