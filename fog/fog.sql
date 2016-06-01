-- CREATE EXTENSION plperlu;
-- CREATE EXTENSION plperlu;
DROP TYPE fog_stats CASCADE;

CREATE TYPE fog_stats AS (fog float8, num_words integer, percent_complex float8, 
                                num_sentences integer, fog_original float8, num_sentences_original integer);

CREATE OR REPLACE FUNCTION fog_data(text) RETURNS fog_stats AS $$ 

    # Load Perl module that calculates fog, etc.
    use Lingua::EN::Fathom;
    use Lingua::EN::Sentence qw( get_sentences add_acronyms );

    if (defined($_[0])) {
        my $text = new Lingua::EN::Fathom;

        my $sentences=get_sentences($_[0]);   ## Get the sentences.
        $num_sentences =@$sentences;
    
        $text->analyse_block($_[0]);
        $num_sentences_original = $text->num_sentences;
        $num_words = $text->num_words;
        $percent_complex = $text->percent_complex_words;  
        $fog = 0.4 * ($num_words/$num_sentences + $percent_complex);
        $fog_original = $text->fog;
    }

  return {fog => $fog, num_words => $num_words, percent_complex => $percent_complex, 
	num_sentences => $num_sentences, fog_original => $fog_original, num_sentences_original => $num_sentences_original};

$$ LANGUAGE plperlu;

CREATE OR REPLACE FUNCTION fog(text) RETURNS float8 AS $$ 

    use Lingua::EN::Fathom;
    use Lingua::EN::Sentence qw( get_sentences add_acronyms );

    if (defined($_[0])) {
        my $text = new Lingua::EN::Fathom;

        my $sentences=get_sentences($_[0]);   ## Get the sentences.
        $num_sentences =@$sentences;
	
        $text->analyse_block($_[0]);
        $num_words = $text->num_words;
        $percent_complex = $text->percent_complex_words;  
        $fog = 0.4 * ($num_words/$num_sentences + $percent_complex);
        return($fog);
    }
    
$$ LANGUAGE plperlu;
