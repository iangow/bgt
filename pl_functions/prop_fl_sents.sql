CREATE OR REPLACE FUNCTION public.prop_fl_sents(sentences text[])
  RETURNS double precision AS
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
        nfl_regex = re.compile(r"\b" + fl_be_pp + r"\s" + fl_pp, re.I)

        SD["fl_regex"] = fl_regex
        SD["nfl_regex"] = nfl_regex

    # rest of function
    fl_sents = [sent for sent in sentences if re.findall(fl_regex, sent) and not re.findall(nfl_regex, sent)]
    if len(sentences) > 0:
        return(len(fl_sents)*1.0/len(sentences))

$BODY$
  LANGUAGE plpythonu IMMUTABLE STRICT;
