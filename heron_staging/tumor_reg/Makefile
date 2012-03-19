# see install-dependencies-ubuntu below
CURL=curl
PERL=perl
PS2PDF=pdf2ps
PS2ASCII=ps2ascii
TIDY=tidy

naaccr_extract.sql naaccr_extract.ctl: naaccr_explore.py naaccr12_1.txt

naaccr12_1.txt: naaccr12_1.ps
	$(PS2ASCII) <naaccr12_1.ps >$@

naaccr12_1.ps: Vol\ II\ Version\ 12.1\ with\ cover.pdf
	$(PS2PDF) Vol\ II\ Version\ 12.1\ with\ cover.pdf $@

Vol\ II\ Version\ 12.1\ with\ cover.pdf:
	$(CURL) --output "$@" 'http://www.naaccr.org/LinkClick.aspx?fileticket=LJJNRVo4lT4%3d&tabid=133&mid=473'


# We loaded the tables from this into Oracle:
naaccr12_1.mdb:
	$(CURL) --output $@ 'http://www.naaccr.org/LinkClick.aspx?fileticket=h6_MTYvGFLU%3d&tabid=133&mid=473'


,seer_site_recode_t.html: ,seer_site_recode.html
	$(TIDY) -q -i -n -ascii -asxml $< > $@

,seer_site_recode.html:
	$(CURL) --output "$@" 'http://seer.cancer.gov/siterecode/icdo3_d01272003/'




# Tested with the following:

# Package: ghostscript
# Version: 9.01~dfsg-1ubuntu5
# MD5sum: e228395b03c994c9da04d2035301571f
# Homepage: http://www.ghostscript.com/

# Package: perl-base
# Essential: yes
# Version: 5.10.1-17ubuntu4.1
# MD5sum: e298441de5991c2ab8cc2840074d619c

# Package: curl
# Version: 7.21.3-1ubuntu1.2
# MD5sum: faedeb169143706c8cd3ebcd647c0657
# Homepage: http://curl.haxx.se

install-dependencies-ubuntu:
	sudo apt-get install perl-base ghostscript 