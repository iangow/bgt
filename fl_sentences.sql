CREATE OR REPLACE FUNCTION prop_fl_sents(sentences text[])
  RETURNS float8 AS
$BODY$

    """Function to return proportion of sentences than contain
        forward-looking terms."""

    if 're' in SD:
        re = SD['re']
    else:
        import re
        SD['re'] = re

    if SD.has_key("fl_regex"):
        fl_regex = SD["fl_regex"]
        nfl_regex = SD["nfl_regex"]
    else:

        # Pre-compile regular expressions.
        fl_regex = r"will|should|can|could|may|might|expect|anticipate|"
        fl_regex += r"believe|plan|hope|intend|seek|project|forecast|objective|goal"
        fl_regex = re.compile(r"(?:\b(" + fl_regex + r"))", re.I)

        fl_pp = r"(?:expected|anticipated|forecasted|projected|believed)"
        fl_be_pp = r"(?:was|were|had|had been)"
        nfl_regex = re.compile(r"\b" + fl_be_pp + r"\s" + wfl_pp, re.I)

        SD["fl_regex"] = fl_regex
        SD["nfl_regex"] = nfl_regex

    # rest of function
    fl_sents = [sent for sent in sentences if re.findall(fl_regex, sent) and not re.findall(nfl_regex, sent)]
    if len(sentences) > 0:
        return(len(fl_sents)*1.0/len(sentences))

$BODY$
  LANGUAGE plpythonu VOLATILE
  COST 100;


  CREATE OR REPLACE FUNCTION prop_fl_sents(text)
  RETURNS double precision AS
$BODY$ 

    # Load Perl modules that calculate fog, etc.
    use Lingua::EN::Sentence qw( get_sentences add_acronyms );

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

    my $sentences=get_sentences($_[0]);   ## Get the sentences.
    my $num_sentences =0;
    my $fl_count = 0;
    
    foreach my $sentence (@$sentences) {
        my $fl=0;

        if ($sentence =~ /\b(?:$fl_regex)/i) {
            $fl=1;
        }

        if ($sentence =~ /\b$fl_be_pp\s\$fl_pp/i) {
            $fl=0;
        }

        $num_sentences++;
        $fl_count += $fl;
    }

    if ($num_sentences>0) {
        return($fl_count/$num_sentences);
    } else {
        return undef;
    }
$BODY$ LANGUAGE plperlu;