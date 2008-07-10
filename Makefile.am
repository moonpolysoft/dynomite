include $(srcdir)/bones-common/powerset-automake.am

ACLOCAL_AMFLAGS = -I bones-common

EXTRA_DIST = yaml-template.yicf.in

install-data-local:
	$(mkdir_p) $(DESTDIR)$(prefix)/share/dynomite
	cp -R src/* $(DESTDIR)$(prefix)/share/dynomite
	cd $(DESTDIR)$(prefix)/share/dynomite && rake
