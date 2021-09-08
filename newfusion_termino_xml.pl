#!/usr/bin/perl

#-- ------------------------------------------------------------------------- --
#-- Script: fusion_termino_xml.pl                                             --
#--      fusion de termes à l'interieur d'une liste en sortie de YaTeA        --
#-- Auteur: Sophie Aubin                                                      --
#-- Date: 21/09/2009                                                          --
#--                                                                           --
#-- update: 08/11/2010                                                        --
#--  modifications pour effectuer le traitement en 2 passes afin de           --
#--  minimiser la consomation memoire.                                        --
#--                                                                           --
#-- ------------------------------------------------------------------------- --

use XML::Twig;
use Getopt::Long;
use strict;

#-- ------------------------------------------------------------------------- --
#TermCandidates non filtres (!= DISMISSED) lus lors de la premiere passe, (indexe par ID)
my %CANDIDATES;        
#ID des TermCandidates filtres (= DISMISSED) ou non-fusionne
my %UNIQCANDIDATES;
#TermCandidates indexes par les clefs permettant le regroupement
my %CANDIDATESBYKEY;

my %options;
my $log;

my ($ss,$mm,$hh,$jj,$mo,$aa,$js,$ja,$st) = localtime(time);
$log="fusion_termes".$ss.$mm.$hh.".log";

unless (&GetOptions(\%options,"help", "configuration=s")	
	&& (scalar @ARGV == 2)
	&& (defined $options{'configuration'})
	&& (!defined $options{'help'})
	&& (!defined $options{'h'})
	) {
    &usage($0);
}


my $in_list= $ARGV[0];
my $out_list= $ARGV[1];
open(OUT, ">".$out_list) || die "Cannot open " . $out_list . "\n";
open (LOG, ">" . $log)  || die "Cannot open $log\n";
my $tc_counter = 0;
my $tcg_counter = 0;
my $max_ID = 0;



print $in_list . " -> " . $out_list . "\n";
&load_configuration;

# First Pass : retrieve all term candidates which may be merged
print "Phase 1\n";
my $start_handlers = { 'TERM_CANDIDATE' => \&phaseOne };
my $twig = XML::Twig->new(TwigHandlers => $start_handlers, keep_spaces_in => [''], pretty_print => 'indented', load_DTD=>1, keep_encoding=>1);
$twig->parsefile($in_list);
$twig->purge();

print "Merging\n";
computeMerging(\*LOG);

# Second Pass : apply modification in stream mode
print "Phase 2\n";
$start_handlers = { 'TERM_CANDIDATE' => \&phaseTwo };
$twig = XML::Twig->new(TwigHandlers => $start_handlers, keep_spaces_in => [''], pretty_print => 'indented', load_DTD=>1, keep_encoding=>1);
$twig->parsefile($in_list);
$twig->flush(\*OUT);
close OUT;


print STDERR $tc_counter . " termes au départ\n";
print STDERR $tcg_counter . " candidats termes à l'arrivée\n";
close LOG;

#-- ------------------------------------------------------------------------- --
sub computeMerging
{
    my $LOG = shift;

    foreach my $termKGroup (values %CANDIDATESBYKEY) {
	foreach my $distKGroup (values %{$termKGroup}) {
	    &create_merged_tc($distKGroup, $LOG);
	}
    }
}
#-- ------------------------------------------------------------------------- --

sub create_merged_tc
{
    my ($tc_list_a, $LOG) = @_;
    
    my $merging_tc;
    my $official;
    my %param;
    $tcg_counter++;
    if(scalar @$tc_list_a == 1)  {
	# no merge needed: only one element in the class
	my $termCandidate = @{$tc_list_a}[0];
	$termCandidate->isUniq(1);
	$UNIQCANDIDATES{$termCandidate->id()} = 1;
	
    } else {
	
	$official = &choose_official($tc_list_a);
	$official->isMergingTemplate(1);
	my $mergingIdRef = "term" . ++$max_ID;
        print $LOG "Representant: " . $official->form() . "\n";	
	foreach my $tc (@$tc_list_a) {
	    print $LOG "\t- ". $tc->form() . "\n";
	    $tc->mergingIdRef($mergingIdRef);
	    push @{$official->mergedIdRefs()}, $tc->id();
	}
	print $LOG "\n";
    }
}
#-- ------------------------------------------------------------------------- --

sub choose_official
{
    my ($tc_list_a) = @_;
   
    if($options{favorite} eq "NB_OCC")
    {
	return &get_official_nbocc($tc_list_a);
    }
    else
    {
	if($options{favorite} eq "SHORTEST")
	{
	    return &get_official_shortest($tc_list_a);
	}
    }
   
}
#-- ------------------------------------------------------------------------- --

sub get_official_shortest
{
    my ($tc_list_a) = @_;
    my $tc;
    my $nb_occ = 0;
    my $tc_length ;
    my $min_length = 10000;
    my $tc_occ;
    my $official;
    foreach $tc (@$tc_list_a)
    {
	$tc_length = length($tc->form());
  # 	print STDERR $tc_length . "\n";
	if($tc_length < $min_length)
	{
	    $official = $tc;
	    $min_length =  $tc_length;
	    $nb_occ = scalar @{$tc->occurrences()};
	    #	    print "OFFICIEL\n";
	}
	else
 	{
	    if($tc_length == $min_length)
	    {
		$tc_occ = scalar @{$tc->occurrences()};
		if($tc_occ > $nb_occ)
		{
		    $official = $tc;
		    $nb_occ =  $tc_occ;
#		    print "occ\n";
		}
		else
		{
		    if($tc_occ == $nb_occ)
		    {
#			print "TEST lc\n";
			if($tc->form() eq lc($tc->form()))
			{
			    $official = $tc;
			}
		    }
		}
	    }
    	}
    }
    return $official;
}
#-- ------------------------------------------------------------------------- --


sub get_official_nbocc
{
    my ($tc_list_a) = @_;
    my $tc;
    my $nb_occ = 0;
    my $tc_occ;
    my $official;
    foreach $tc (@$tc_list_a)
    {
	$tc_occ = scalar @{$tc->occurrences()};
	if($tc_occ > $nb_occ)
	{
	    $official = $tc;
	    $nb_occ =  $tc_occ;
#	    print "OFFICIEL\n";
	}
# 	else
# 	{
# 	    print "NON\n";
# 	}
    }
    return $official;
}
#-- ------------------------------------------------------------------------- --

sub computeDistKey
{
    my $tc = shift;
    my $key;
    my $criterion;
    my $elt;
   # print $tc->first_child_text("FORM") . "\t";
    foreach $criterion (keys %{$options{distinct}})
    {
	$elt = $tc->first_descendant($criterion);
	$key.= $elt->text;
    }
   # print "(" . $key .")";
    return $key;
}
#-- ------------------------------------------------------------------------- --

sub computeTermKey
{
    my $data = shift;
    my $key;
    foreach my $field (@{$options{merge}})
    {
	$key .= $data->first_child_text($field);
    }
    &apply_options(\$key);
    return $key;
}
#-- ------------------------------------------------------------------------- --

sub process_id
{
    my ($ID) = @_;
    $ID =~ s/^term//;
    if($ID > $max_ID)
    {
	$max_ID = $ID;
    }
}
#-- ------------------------------------------------------------------------- --

sub apply_options
{
    my ($key) = @_;
    my $first;
    my $last;
    my $between;
    if($options{case_sens} == 0)
    {
	$$key = lc($$key);
    }
    if(
       ($options{typo_free} == 1)
       &&
       (length($$key)>2)
       )
   {
       $$key =~ /^(.)(.+)(.)$/;
       $first = $1;
       $between = $2;
       $last = $3;
       $between =~ s/[$options{qm_chars}]//g;
       $$key = $first.$between.$last;
   }    
}
#-- ------------------------------------------------------------------------- --

sub load_configuration
{
    my $line;
    my $chars;
    
    open (CONFIG, "<". $options{"configuration"}) || die "cannot open configutation file : ". $options{"configuration"} . "\n"; 
    while ($line = <CONFIG>)
    {
	if($line !~ /^\s*#/)
	   {
	       chomp $line;
	       $line =~ s/\s+$//;
	       
	       if($line =~ /^\s*MERGE=\s*(.+)\s*$/) # critère de fusion
	       {
		   #$merge = $1;
		   push @{$options{merge}}, $1;
	       }
	       else
	       {
		   if($line =~ /^\s*CASE_SENSITIVE=\s*([01])\s*$/) # sensibilité à la casse (1 ou 0, défaut 1)
		   {
		       $options{case_sens} = $1;
		   }
		   else
		   {
		       if($line =~ /^\s*DISTINCT=\s*(.+)\s*$/) # champ(s) necessitant une egalite de valeur pour la fusion (critère de distinction)
		       {
			   $options{distinct}{$1}++;
		       }
		       else
		       {
			   if($line =~ /^\s*TYPO_FREE=\s*(.+)\s*$/) # fusion des variantes typographiques
			   {
			       $options{typo_free} = $1;
			   }
			   else
			   {
			       if($line =~ /^\s*CHAR=(.+)$/) # caractères a normaliser pour la fusion des variantes typographiques
			       {
				   $chars = $1;
				   $options{qm_chars} = quotemeta($chars);
			       }
			       else
			       {
				   if($line =~ /^\s*FAVORITE=(.+)$/) # caractères a normaliser pour la fusion des variantes typographiques
				   {
				       $options{favorite} = $1;
				   }
			       }
			   }
		       }
		   }
	       }
	   }
    }
    close CONFIG;
    
    print STDERR "MERGE by " . join("+",@{$options{merge}}) . "\n";
}
#-- ------------------------------------------------------------------------- --

sub usage
{
    my ($program_name) = @_;
    $program_name =~ s/^\s*\.\///;

     warn "\n
           ********************************************
           *             Using " . $program_name . "               *
           ******************************************** 
\nCommand : perl ". $program_name . " OPTION input output
-configuration : configuration file
   \n";
    die "\n";
}
#-- ------------------------------------------------------------------------- --

# The YaTeA output file must be read entirely in order to retreive all term 
# candidates which can be merged.
# To reduce memory usage, just necessary data for fusion is kept during this pass.
sub phaseOne
{
    my ($thetwig, $data) = @_;
    
    my $termCandidateId = $data->first_child_text("ID");

    &process_id($termCandidateId);
    
    if(	(defined $data->att("DISMISSED")) && ($data->att("DISMISSED") eq "TRUE") ) {

	#filtered ("DISMISSED") term will be tagged as "UNIQ"
	$UNIQCANDIDATES{$termCandidateId} = 1;

    } else {
	# only non-filtered terms are counted
	$tc_counter++;  
	
	# store data useful for merging purpose
        my $termCandidate = newTermCandidateFromNode($data);
    
	my $tkey = computeTermKey($data);
	$termCandidate->termKey($tkey);
	
	my $dkey = computeDistKey($data);
	$termCandidate->distKey($dkey);
	
	# store the term candidate for further processing
	$CANDIDATES{$termCandidateId} = $termCandidate;
	
	push @{$CANDIDATESBYKEY{$tkey}->{$dkey}}, $termCandidate;
    }
    
    #free XML parser memory   
    $thetwig->purge_up_to($data);
}

# The modified YaTeA XML file can now be generated in stream mode
sub phaseTwo
{
    my ($thetwig, $data) = @_;
    
    my $termCandidateId = $data->first_child_text("ID");
    
    if ( $UNIQCANDIDATES{$termCandidateId} ) {
	
	$data->set_att("MERGE_TYPE" => "UNIQ");
	
    } else {
	my $termCandidate = $CANDIDATES{$termCandidateId};
	if ($termCandidate->isMergingTemplate()) {
	    
	    #Create the new merging as a copy from the template
	    my $merging_tc = $data->copy();
	    my $mergingIdRef = $termCandidate->mergingIdRef();
	    
	    $merging_tc->first_child("ID")->set_text($mergingIdRef);
	    $merging_tc->set_att("MERGE_TYPE" => "MERGING");
	    my $ml = XML::Twig::Elt->new("LIST_MERGED");
	    $ml->paste(after =>$merging_tc->first_child("LIST_OCCURRENCES"));
	    
	    my $numberOfOccurrences = scalar @{$termCandidate->occurrences()};
	    
	    #list of id references to the candidate merged within the new merging
	    foreach my $mergedId (@{$termCandidate->mergedIdRefs()}) {
		my $m_ed = XML::Twig::Elt->new("MERGED" => $mergedId);
		$m_ed->paste($merging_tc->first_child("LIST_MERGED"));
	    
		#add occurrences from the other merged
		my $mergedCandidate = $CANDIDATES{$mergedId};
		if ($mergedCandidate != $termCandidate) {
		    foreach my $occurrence (@{$mergedCandidate->occurrences()}) {
			$numberOfOccurrences++;
			my $occNode = XML::Twig::Elt->new("OCCURRENCE");
			my @subNodes;
			push @subNodes, XML::Twig::Elt->new("ID" => $occurrence->id());
			push @subNodes, XML::Twig::Elt->new("MNP" => $occurrence->isMnp());
			push @subNodes, XML::Twig::Elt->new("DOC" => $occurrence->documentIdRef());
			push @subNodes, XML::Twig::Elt->new("SENTENCE" => $occurrence->sentenceIdRef());
			push @subNodes, XML::Twig::Elt->new("START_POSITION" => $occurrence->startPosition());
			push @subNodes, XML::Twig::Elt->new("END_POSITION" => $occurrence->endPosition());
			foreach my $n (@subNodes) {
			    $n->paste(last_child => $occNode);
			}
			$occNode->paste($merging_tc->first_child("LIST_OCCURRENCES"));
		    }
		}
	    }
	    $merging_tc->first_child("NUMBER_OCCURRENCES")->set_text($numberOfOccurrences);
	    
	    #insert merging just before the current term candidate (which is its template)
	    $merging_tc->paste(before => $data);
	}
  
	my $mergingIdRef = $termCandidate->mergingIdRef();
	if ($mergingIdRef) {
	    # id reference to the new merging candidate replacing this merged
	    my $m_ing = XML::Twig::Elt->new("MERGING" => $mergingIdRef); 
	    $m_ing->paste(after => $data->first_child("LIST_OCCURRENCES"));
	    $data->set_att("MERGE_TYPE"=>"MERGED");
	} else {
	    # (should not happen)
	    $data->set_att("MERGE_TYPE" => "uniq");
	}
    }
    
    $thetwig->flush_up_to($data, \*OUT);
}


sub newTermCandidateFromNode {
    my $tcNode = shift;
    my $termCandidate = TermCandidate->new();
    $termCandidate->id( $tcNode->first_child_text("ID") );
    $termCandidate->form( $tcNode->first_child_text("FORM") );

    foreach my $occurNode ( $tcNode->first_child("LIST_OCCURRENCES")->children("OCCURRENCE") ) {
	my $occurrence = TermOccurrence->new();
	$occurrence->id( $occurNode->first_child_text("ID") );
	$occurrence->isMnp( $occurNode->first_child_text("MNP") );
	$occurrence->documentIdRef( $occurNode->first_child_text("DOC") );
	$occurrence->sentenceIdRef( $occurNode->first_child_text("SENTENCE") );
	$occurrence->startPosition( $occurNode->first_child_text("START_POSITION") );
	$occurrence->endPosition( $occurNode->first_child_text("END_POSITION") );
	
	push @{$termCandidate->occurrences}, $occurrence;
    }
    return $termCandidate;
}

package TermCandidate;
use strict;
    sub new {
	my $class = shift;
	my $self = {};

        $self->{ID} = undef;
	$self->{TERM_KEY} = undef;
        $self->{DIST_KEY} = undef;
	$self->{FORM} = undef;
        $self->{OCCURRENCES} = [];
	$self->{IS_MERGINGTEMPLATE} = 0;
        $self->{IS_UNIQ} = 0;
	$self->{MERGING_IDREF} = undef;
	$self->{MERGED_IDREFS} = [];

	bless ($self, $class);
	return $self;
    }
    
    sub id {
	my $self = shift;
	if (@_) { $self->{ID} = shift }
	return $self->{ID};
    }

    sub termKey {
	my $self = shift;
	if (@_) { $self->{TERM_KEY} = shift }
	return $self->{TERM_KEY};
    }
    
    sub distKey {
	my $self = shift;
	if (@_) { $self->{DIST_KEY} = shift }
	return $self->{DIST_KEY};
    }
  
    sub form {
	my $self = shift;
	if (@_) { $self->{FORM} = shift }
	return $self->{FORM};
    }

    sub occurrences {
	my $self = shift;
	if (@_) { $self->{OCCURRENCES} = shift }
	return $self->{OCCURRENCES};
    }

    sub isMergingTemplate {
	my $self = shift;
	if (@_) { $self->{IS_MERGINGTEMPLATE} = shift }
	return $self->{IS_MERGINGTEMPLATE};
    }
  
    sub isUniq {
	my $self = shift;
	if (@_) { $self->{IS_UNIQ} = shift }
	return $self->{IS_UNIQ};
    }
    
    sub mergingIdRef {
	my $self = shift;
	if (@_) { $self->{MERGING_IDREF} = shift }
	return $self->{MERGING_IDREF};
    }

    sub mergedIdRefs {
	my $self = shift;
	if (@_) { $self->{MERGED_IDREFS} = shift }
	return $self->{MERGED_IDREFS};
    }
#-- ------------------------------------------------------------------------- --

    
package TermOccurrence;
use strict;
    sub new {
	my $class = shift;
	my $self = {};

	$self->{ID} = undef;
	$self->{IS_MNP} = 0;
	$self->{DOCUMENT_IDREF} = undef;
	$self->{SENTENCE_IDREF} = undef;
	$self->{START_POSITION} = undef;
	$self->{END_POSITION} = undef;

	bless ($self, $class);
	return $self;
    }

    sub id {
	my $self = shift;
	if (@_) { $self->{ID} = shift }
	return $self->{ID};
    }

    sub isMnp {
	my $self = shift;
	if (@_) { $self->{IS_MNP} = shift }
	return $self->{IS_MNP};
    }

    sub documentIdRef {
	my $self = shift;
	if (@_) { $self->{DOCUMENT_IDREF} = shift }
	return $self->{DOCUMENT_IDREF};
    }

    sub sentenceIdRef {
	my $self = shift;
	if (@_) { $self->{SENTENCE_IDREF} = shift }
	return $self->{SENTENCE_IDREF};
    }

    sub startPosition {
	my $self = shift;
	if (@_) { $self->{START_POSITION} = shift }
	return $self->{START_POSITION};
    }

    sub endPosition {
	my $self = shift;
	if (@_) { $self->{END_POSITION} = shift }
	return $self->{END_POSITION};
    }

#-- ------------------------------------------------------------------------- --