# Fails if any file of $MYSQLD_DATADIR/$SEARCH_DIR contains "SECRET" or
# "CONFIDENTIAL" in plaintext; prints how many files were checked.
--perl
use strict;
my $dir = "$ENV{MYSQLD_DATADIR}/$ENV{SEARCH_DIR}";
opendir(my $dh, $dir) or die "opendir $dir: $!";
my @files = grep { -f "$dir/$_" } readdir($dh);
closedir($dh);
my $leaks = 0;
foreach my $f (@files) {
  open(my $fh, '<:raw', "$dir/$f") or die "open $f: $!";
  local $/;
  my $data = <$fh>;
  close($fh);
  $leaks++ if $data =~ /SECRET|CONFIDENTIAL/;
}
print "plaintext found in $leaks file(s)\n";
EOF
