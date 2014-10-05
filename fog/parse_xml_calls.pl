#!/usr/bin/perl

## TODO(igow): I need to fix "HTML entities" (e.g., &amp;) before putting the data
##             into my database

# use module
use XML::LibXML;
use IO::Uncompress::Gunzip qw(gunzip $GunzipError);
use Lingua::Identify qw(:language_identification);
use utf8; # does not enable Unicode output - it enables you to type Unicode in your program.
use File::Basename;
use XML::Entities qw( decode);

# Add this to the program, before your print() statement:
binmode(STDOUT, ":utf8");

# Load Perl module that calculates fog, etc.
use Lingua::EN::Fathom;
use Lingua::EN::Sentence qw( get_sentences add_acronyms );

# Output a header row
print "file\tname\temployer\trole\tnumber\tcontext\tfog\tnum_words\tnum_sentences";
print "\tpercent_complex\tfl_count\n";

# Get a list of files to parse
$call_directory = "/Volumes/2TB/data/streetevents2013/";
$i = $ARGV[0];
$file_list = $call_directory . "dir_" . $i . "/*.xml";
@file_list = <"$file_list">;

# Open each file and extract the contents into a string $lines
foreach $gz_file (@file_list) {
  # Remove file extensions from basename
  $basename = basename($gz_file, @suffixlist);
  $basename =~ s/\.xml(\.gz)?$//g;
  
  # initialize the parser
  my $parser = new XML::LibXML;
  
  # open a filehandle and parse
  open($fh, "<", $gz_file) or die;
  my $doc = $parser->parse_fh( $fh );
  close $fh;
  foreach my $event ($doc->findnodes('/Event')) {
    my $type = $event->findvalue('./@eventTypeId');
    
    if ($type ne '1') {
      next;
    }
  
    my $ticker = $event->findnodes('./companyTicker');
    my $lines = $event->findnodes('./EventStory/Body');
    
    # Skip calls without tickers
    if (!defined $ticker or $ticker =~ /^\s*$/) { next; } 
   
    # Fix Windows-style line endings. 
    $lines =~ s/\r\n/\n/g;
    
    # access XML data
    # I'm currently not doing anything with this data, though it may be useful to identify
    # who belongs to the company, and who doesn't.
    if ($lines =~ /={3,}\nCorporate Participants\n={3,}(.*?)={3,}/s) {
      $corp_parts = $1;
    }

    if ($lines =~ /={3,}\nConference Call Participants\n={3,}(.*?)={3,}/s) {
      $conf_parts = $1;
    }

    # $searchText =~ s/.*={3,}\n(?:Presentation|Transcript)\n-{3,}(.*?)(?:={3,}).*/$1/gs;
    # Look for  the word "Presentation" between a row of ===s a row of ---s and 
    # then text followed by a row of ===s. Capture the latter text.  
    if ($lines =~ /={3,}\n(?:Presentation|Transcript)\n-{3,}(.*?)(?:={3,})/s) {
      $pres = $1;
    }
    
    # Skip file if language isn't English 
    $language = langof($pres); # gives the most probable language
    if ($language ne "en") { next; }

    sub analyse_text {
        # Look for consecutive portions of text in this format:
        # -----------
        #  Something
        # -----------
        #  Something else
        # -----------
        # 
        # The something is the speaker, the something else is what they said.
        # Split using the lines of ---s and then process each portion.
        my %values = split(/---{3,}/, $_[0]);
        foreach my $speaker (keys %values) {
          my $the_text = $values{$speaker};
          $speaker =~ s/\n/ /g;
          $speaker =~ s/\s{2,}/ /g;
          $speaker =~ s/^\s+//g;
          $speaker =~ s/\t+//g;
          
          $the_text =~ s/\n/ /g;
          $the_text =~ s/^\s+//g;
          $the_text =~ s/\s{2,}/ /g;
          if ($the_text =~ /\?$/) { 
            $context = "qa";
          }
          # Calculate fog
          my $text = new Lingua::EN::Fathom;
          $fog = "";
     
          $accumulate = 0;
          if (defined($the_text)) {
            $text->analyse_block($the_text,$accumulate);
            $fog   = $text->fog;
            $num_words = $text->num_words;
            $percent_complex = $text->percent_complex_words;  
            $num_sentences = $text->num_sentences;
          }
          
          $speaker =~ /^(.*)\s+\[(\d+)\]\s+$/;
          $full_name = $1;
          $number = $2;
          $full_name =~ /^([^,]*),\s*(.*)\s+-\s+(.*)$/;
          $name = $1;
          $employer = $2;
          $role = $3;
          
          $name =~ s/^\s+//g;
          $name =~ s/\s+$//g;

          if (!defined $role) {
            $employer= "";
            $role ="";
          }
          
          # Count forward-looking sentences.
          # From Li, Feng (JAR 2010):
          # I define forward-looking statements as all those sentences that contain: 
          # “will,” “should,” “can,” “could,” “may,” “might,” “expect,” “anticipate,” “believe,” 
          # “plan,” “hope,” “intend,” “seek,” “project,” “forecast,” “objective,” or “goal.
          #  I exclude all sentences that contain “expected,” “anticipated,” “forecasted,” “projected,”
          # or “believed” when such words follow “was,” “were,” “had,” and “had been.”
          $fl_regex = "will|should|can|could|may|might|expect|anticipate|";
          $fl_regex .= "believe|plan|hope|intend|seek|project|forecast|objective|goal";

          $fl_pp = "(?:expected|anticipated|forecasted|projected|believed)";
          $fl_be_pp = "(?:was|were|had|had been)";
          
          # add_acronyms('lt','gen');               ## adding support for 'Lt. Gen.'
          my $sentences=get_sentences($the_text);   ## Get the sentences.
          $num_sentences =0;
          $fl_count = 0;
          foreach my $sentence (@$sentences) {
            if ($sentence =~ /\b(?:$fl_regex)/) {
              $fl=1;
            }
            if ($sentence =~ /\b$fl_be_pp\s\$fl_pp/) {
              $fl=0;
            }
            $num_sentences++;
            $fl_count += $fl;
          }
        
        # Output results num_sentences
        print "$basename\t$name\t$employer\t$role\t$number\t$context\t$fog\t$num_words";
        print "\t$num_sentences\t$percent_complex\t$fl_count\n";
      }
    }

    $context = "pres";
    analyse_text(XML::Entities::decode('all', $pres));

    # Now do the same thing for Q&A as was done for the presentation
    if ($lines =~ /={3,}\nQuestions and Answers\n-{3,}(.*)$/s) {
      $qa = $1;
    }

    $context = "qa";
    analyse_text(XML::Entities::decode('all', $qa));
  }
}

