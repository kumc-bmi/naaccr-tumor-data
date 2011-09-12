PERL=perl

record_layout.csv: Vol\ II\ Version\ 12.1\ with\ cover.xlsx
	echo paste Tables 7, 8, 9 at the bottom of Table 6 and export as CSV

Vol\ II\ Version\ 12.1\ with\ cover.xlsx: Vol\ II\ Version\ 12.1\ with\ cover.xls
	perl -pe 's/&#10/&#10;/g' Vol\ II\ Version\ 12.1\ with\ cover.xls >"$@"

Vol\ II\ Version\ 12.1\ with\ cover.xls: Vol\ II\ Version\ 12.1\ with\ cover.pdf
	echo "use http://www.pdftoexcelonline.com/"

Vol\ II\ Version\ 12.1\ with\ cover.pdf:
	curl --output "$@" 'http://www.naaccr.org/LinkClick.aspx?fileticket=LJJNRVo4lT4%3d&tabid=133&mid=473'
