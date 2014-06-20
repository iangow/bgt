#!/usr/bin/perl
use DBI;
use POSIX qw(strftime);

$dbname = "crsp";
my $dbh = DBI->connect("dbi:Pg:dbname=$dbname", 'igow')	
	or die "Cannot connect: " . $DBI::errstr;

$sql = "
  -- CREATE SCHEMA bgt;

  DROP TABLE IF EXISTS bgt.speakers;
  CREATE TABLE bgt.speakers
(
  file_name text,
  speaker_name text,
  employer text,
  role text, 
  speaker_number integer,
  context text,
  fog double precision,
  num_words	integer,
  num_sentences	integer,
  percent_complex_words float8,
  fl_count integer
);
";

# Run SQL to create the table
$dbh->do($sql);

for ($i = 0; $i <= 9; $i++) {
  # Use PostgreSQL's COPY function to get data into the database
  $time = localtime; 
  $now_string = strftime "%a %b %e %H:%M:%S %Y", localtime;
  $filename = "../data/fog_" . $i . ".txt.gz";
  printf "Beginning import of $filename at $now_string\n";  

  $cmd  = "gunzip -c \"$filename\" | sed 's/\\\"//g'  ";
  $cmd .=  "| psql -U igow ";
  $cmd .= "-d $dbname -c \"COPY bgt.speakers FROM STDIN CSV HEADER ";
  $cmd .= "DELIMITER '\t' \";";
  print "$cmd\n";
  $result = system($cmd);
  print "Result of system command: $result\n";

  $now_string = strftime "%a %b %e %H:%M:%S %Y", localtime;
  printf "Completed import of $filename at $now_string\n"; 
}

# Fix permissions and set up indexes
#$sql = "ALTER TABLE issvoting.npx OWNER TO activism";
# $dbh->do($sql);

$sql = "
  SET maintenance_work_mem='10GB';
  CREATE INDEX ON bgt.speakers (file_name);";
#  UPDATE bgt.speakers SET employer=trim(employer);";
$dbh->do($sql);

$dbh->disconnect();
