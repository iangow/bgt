#!/opt/local/bin/perl
use DBI;
use POSIX qw(strftime);

$dbname = "crsp";
my $dbh = DBI->connect("dbi:Pg:dbname=$dbname", 'igow')	
	or die "Cannot connect: " . $DBI::errstr;

$sql = "
  -- CREATE SCHEMA bgt;

  DROP TABLE IF EXISTS bgt.lambda_all;
  CREATE TABLE bgt.lambda_all
(
  symbol text,
  date date,
  ntrades integer,
  ewsize float8,
  twspread float8,
  twqspread float8,
  twprice float8,
  twespread  float8,
  ewspread float8,
  ewqspread float8,
  ewprice float8,
  ewespread float8,
  ewdepth float8,
  bin1 int4,
  bin2 int4,
  bin3 int4,
  bin4 int4,
  bin5 int4,
  lambda_gh float8,
  lambda_ghother float8,
  first_date int4,
  last_date int4,
  lambda_mrr float8);
";

# Run SQL to create the table
$dbh->do($sql);

# Use PostgreSQL's COPY function to get data into the database
$time = localtime; 
$now_string = strftime "%a %b %e %H:%M:%S %Y", localtime;
$filename = "../data/lambda_all.csv.gz";
printf "Beginning import of $filename at $now_string\n";  

$cmd  = "gunzip -c \"$filename\" | sed 's/\\\"//g'  ";
$cmd .=  "| psql -U igow ";
$cmd .= "-d $dbname -c \"COPY bgt.lambda_all FROM STDIN CSV HEADER ";
$cmd .= " \";";
print "$cmd\n";
$result = system($cmd);
print "Result of system command: $result\n";

$now_string = strftime "%a %b %e %H:%M:%S %Y", localtime;
printf "Completed import of $filename at $now_string\n"; 

# Fix permissions and set up indexes
#$sql = "ALTER TABLE issvoting.npx OWNER TO activism";
# $dbh->do($sql);

$sql = "
  SET maintenance_work_mem='10GB';
  CREATE INDEX ON bgt.lambda_all (symbol, date);
  ALTER TABLE bgt.lambda_all DROP COLUMN last_date;  
  ALTER TABLE bgt.lambda_all DROP COLUMN first_date;";
$dbh->do($sql);

$dbh->disconnect();
