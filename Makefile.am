ACLOCAL_AMFLAGS = -I m4 --force

lib_LTLIBRARIES = stl_engine.la

if HAVE_LIBSQLITE3
lib_LTLIBRARIES += persistent_engine.la
endif

if HAVE_LIBZ
lib_LTLIBRARIES += compress_engine.la
endif

stl_engine_la_CXXFLAGS = ${NO_ERROR}
stl_engine_la_LDFLAGS = -module -dynamic
stl_engine_la_SOURCES = src/stl/stl_engine.cc src/stl/stl_engine.h

persistent_engine_la_CFLAGS = ${NO_ERROR}
persistent_engine_la_CXXFLAGS = ${NO_ERROR}
persistent_engine_la_LDFLAGS = -module -dynamic ${LIBSQLITE3}
persistent_engine_la_SOURCES = \
                 src/persistent/assoc.c src/persistent/assoc.h \
                 src/persistent/items.c src/persistent/items.h \
                 src/persistent/persistent_engine.c src/persistent/persistent_engine.h \
                 src/persistent/slabs.c src/persistent/slabs.h \
                 src/persistent/sqlite.cc src/persistent/sqlite.h

compress_engine_la_LDFLAGS = -module -dynamic ${LIBZ}
compress_engine_la_CFLAGS = ${NO_ERROR}
compress_engine_la_SOURCES = \
                 src/compress/assoc.c src/compress/assoc.h \
                 src/compress/items.c src/compress/items.h \
                 src/compress/compress_engine.c src/compress/compress_engine.h \
                 src/compress/slabs.c src/compress/slabs.h
