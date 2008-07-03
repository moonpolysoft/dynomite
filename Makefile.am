include $(srcdir)/bones-common/powerset-automake.am

ACLOCAL_AMFLAGS = -I bones-common

EXTRA_DIST = yaml-template.yicf.in

install-data-local:
        printf "\033[33mWARNING: Dave is awesome!\033[0m\n"
        $(mkdir_p) $(DESTDIR)$(prefix)/bin
        cp -R src/bin/* $(DESTDIR)$(prefix)/bin
        $(mkdir_p) $(DESTDIR)$(prefix)/libexec/erlang/dynomite
        cp -R src/* $(DESTDIR)$(prefix)/libexec/erlang/dynomite
        cd $(DESTDIR)$(prefix)/libexec/erlang/dynomite && rake

