# Flips one byte inside the data section of a chunk of table test.t.
# $CORRUPT_WHICH: index into the chunk list sorted by name (0 = oldest, -1 = newest).
--perl
use strict;
my $dir = "$ENV{MYSQLD_DATADIR}/test/t";
opendir(my $dh, $dir) or die "opendir $dir: $!";
my @chunks = sort grep { /^chunk_.*\.tfl$/ } readdir($dh);
closedir($dh);
die "no chunk found" unless @chunks;
my $file = "$dir/$chunks[$ENV{CORRUPT_WHICH}]";
open(my $fh, '+<:raw', $file) or die "open $file: $!";
seek($fh, 70, 0);
read($fh, my $byte, 1);
seek($fh, 70, 0);
print $fh chr(ord($byte) ^ 0xff);
close($fh);
EOF
