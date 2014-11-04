-- CREATE EXTENSION plperlu;
DROP FUNCTION IF EXISTS fog_data(text);

DROP TYPE fog_stats;

CREATE TYPE fog_stats AS (
    fog float8, num_words integer, 
    percent_complex float8, num_sentences integer);

CREATE OR REPLACE FUNCTION fog_data(text) RETURNS fog_stats AS $$ 

    # Load Perl module that calculates fog, etc.
    use Lingua::EN::Fathom;
    # use Lingua::EN::Sentence qw( get_sentences add_acronyms );

    if (defined($_[0])) {
        my $text = new Lingua::EN::Fathom;

        # my $sentences=get_sentences($_[0]);   ## Get the sentences.
        # $num_sentences =@$sentences;
    
        $text->analyse_block($_[0]);
        $num_sentences = $text->num_sentences;
        $num_words = $text->num_words;
        $percent_complex = $text->percent_complex_words;  
        # $fog = 0.4 * ($num_words/$num_sentences + $percent_complex);
        $fog = $text->fog;
    }

  return {fog => $fog, num_words => $num_words, percent_complex => $percent_complex, 
	num_sentences => $num_sentences}; # , fog_original => $fog_original};

$$ LANGUAGE plperlu;

CREATE OR REPLACE FUNCTION fog(text) RETURNS float8 AS $$ 

  # Load Perl modules that calculate fog, etc.
  use Lingua::EN::Fathom;
  use Lingua::EN::Sentence qw( get_sentences add_acronyms );

  my $text = new Lingua::EN::Fathom;
  if (defined($_[0])) {
    $text->analyse_block($_[0]);
    return($text->fog);
  }

$$ LANGUAGE plperlu;

-- CREATE OR REPLACE FUNCTION fog(text)
--   RETURNS double precision AS
-- $BODY$ 
-- 
--     use Lingua::EN::Fathom;
--     use Lingua::EN::Sentence qw( get_sentences add_acronyms );
-- 
--     if (defined($_[0])) {
--         my $text = new Lingua::EN::Fathom;
-- 
--         my $sentences=get_sentences($_[0]);   ## Get the sentences.
--         $num_sentences =@$sentences;
--     
--         $text->analyse_block($_[0]);
--         $num_words = $text->num_words;
--         $percent_complex = $text->percent_complex_words;  
--         $fog = 0.4 * ($num_words/$num_sentences + $percent_complex);
--         return($fog);
--     }
-- 
-- $BODY$
--   LANGUAGE plperlu VOLATILE
--   COST 100;

