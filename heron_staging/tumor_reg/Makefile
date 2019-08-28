# see install-dependencies-ubuntu below
CURL=curl
PERL=perl
PS2PDF=pdf2ps
PS2ASCII=ps2ascii
PYTHON=python

## NAACCR table

naaccr_extract.sql naaccr_extract.ctl: naaccr_explore.py naaccr12_1.txt
	$(PYTHON) naaccr_explore.py

naaccr12_1.txt: naaccr12_1.ps
	$(PS2ASCII) <naaccr12_1.ps >$@

naaccr12_1.ps: Vol\ II\ Version\ 12.1\ with\ cover.pdf
	$(PS2PDF) Vol\ II\ Version\ 12.1\ with\ cover.pdf $@

NAACCR_SPEC=Vol\ II\ Version\ 12.1\ with\ cover.pdf
$(NAACCR_SPEC):
	$(CURL) --output "$@" 'http://www.naaccr.org/LinkClick.aspx?fileticket=LJJNRVo4lT4%3d&tabid=133&mid=473'


# We loaded the tables from this into Oracle:
naaccr12_1.mdb:
	$(CURL) --output $@ 'http://www.naaccr.org/LinkClick.aspx?fileticket=h6_MTYvGFLU%3d&tabid=133&mid=473'


## SEER Recode

seer-recode: seer_recode_terms.csv seer_recode.sql

seer_recode_terms.csv seer_recode.sql: seer_recode.py ,seer_site_recode.txt
	$(PYTHON) seer_recode.py ,seer_site_recode.txt seer_recode_terms.csv seer_recode.sql

,seer_site_recode.txt:
	$(CURL) --output "$@" https://seer.cancer.gov/siterecode/icdo3_dwhoheme/index.txt

## CS Site-Specific Factors

CSTABLES=3_CSTables(HTMLandXML)
csterms: $(CSTABLES)
	find "3_CS Tables (HTML and XML)/XML Format/" -type f -print0 | \
		xargs -0 perl -i.bak -pe 's,\\,/,g'

$(CSTABLES): $(CSTABLES).zip
	unzip -q -o "$(CSTABLES).zip"

$(CSTABLES).zip:
	$(CURL) --output "$@" 'https://cancerstaging.org/cstage/software/Documents/3_CSTables(HTMLandXML).zip'


clean:
	rm -rf \
	    naaccr12_1.txt naaccr12_1.ps \
            seer_recode_terms.csv seer_recode.sql

distclean:
	rm -rf \
            naaccr_extract.sql naaccr_extract.ctl naaccr12_1.txt

clear-cache:
	rm -rf ,* \
            $(CSTABLES) $(CSTABLES).zip $(NAACCR_SPEC)


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
