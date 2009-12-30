ACLOCAL_AMFLAGS = -I m4 --force

lib_LTLIBRARIES = stl_engine.la

if HAVE_LIBSQLITE3
lib_LTLIBRARIES += persistent_engine.la
endif

stl_engine_la_LDFLAGS = -module -dynamic
stl_engine_la_SOURCES = src/stl/stl_engine.cc src/stl/stl_engine.h

persistent_engine_la_LDFLAGS = -module -dynamic ${LIBSQLITE3}
persistent_engine_la_SOURCES = \
                 src/persistent/assoc.c src/persistent/assoc.h \
                 src/persistent/items.c src/persistent/items.h \
                 src/persistent/persistent_engine.c src/persistent/persistent_engine.h \
                 src/persistent/slabs.c src/persistent/slabs.h \
                 src/persistent/sqlite.cc src/persistent/sqlite.h
