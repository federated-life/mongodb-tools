#!/usr/bin/perl
#
# Requires:  Moose , MongoDB , MongoDB::Code, Digest::MD5, Getopt::Long
#
# Usage:
#
# perl ddb-collection-checksum.pl --uriSrc localhost:37017
#+	--uriDst localhost:37067 --limit 1 --srcDb checksum --srcCol values
#+	--dstDb checksum --dstCol values --minkey ""
#
{
	package page_atr;
	use Moose;
	
	has 'value' => (
		is => 'rw', isa => 'Str', reader => 'get_value',
		writer => 'set_value', default => '', );
	has 'position' => ( is => 'rw', isa => 'Str', reader => 'get_position',
		writer => 'set_position', default => '', );

}
{
	package page_index;
	use Moose;
	
	has 'first' => ( is => 'rw', isa => 'Str', reader => 'get_first',
		writer => 'set_first', default => '', );
	has 'final' => ( is => 'rw', isa => 'Str', reader => 'get_final',
		writer => 'set_final', default => '', );
	has 'checksum' => ( is => 'rw', isa => 'Str', reader => 'get_checksum',
		writer => 'set_checksum', default => '', );

}
{
	package page_obj;
	use Moose;
	
	has 'md5hash' => ( is => 'rw', isa => 'Str', reader => 'get_md5hash',
		writer => 'set_md5hash', default => '', );
	has 'last_id' => ( is => 'rw', isa => 'Str', reader => 'get_last_id',
		writer => 'set_last_id', default => '', );
		
	has 'object_list' => ( is => 'rw',
			traits  => ['Array'],
			isa => 'ArrayRef[page_atr]',
			lazy => 1,
			default => sub { [] },
			handles => {
				add_atr => 'push',
			},
	); 
		
}
{
	package pages;
	use Moose;
	
	has 'object_list' => ( is => 'rw',
			traits  => ['Array'],
			isa => 'ArrayRef[page_obj]',
			lazy => 1,
			default => sub { [] },
			handles => {
				add_page => 'push',
			},
	);
	
	sub getFirstAndLastId {
		my $page = $_[1];		# of type page_obj
		my ( $first_atr, $last_atr );
		my $i = 0;
		foreach my $page_atr ( @{ $page->object_list } ) {
			$first_atr = $page_atr->get_value() if ( $i == 0 );
			$i++;
			$last_atr = $page_atr->get_value();
		}
		my $index = page_index->new(
			first => "$first_atr",
			final => "$last_atr",
			checksum => $page->get_md5hash(),
		);
		return $index;
	}
	
	
}
{
	package page;
	use MongoDB;
	use MongoDB::Code;
	use Digest::MD5 qw(md5_hex);
	use Moose;
	no warnings 'recursion';		# trigger for recursive warning is 100
	
	sub processCursor {
		my $cursor = $_[1];
		my $last_id = "";
		my $stringOfIds = "";
		my $page_obj = page_obj->new();	
		my $position = 0;
		while ( my $doc = $cursor->next ) {
			$position++;
			$last_id = $doc->{'_id'};
			$stringOfIds .= $doc->{'_id'};
			$page_obj->add_atr( page_atr->new( value => $last_id,
				position => $position ) );
		}
		$page_obj->set_md5hash( md5_hex( $stringOfIds ) );
		$page_obj->set_last_id( $last_id );
		return $page_obj;			# type page_obj
		
	}
	
	sub getPageIds {
		my $minkey = $_[1];
		my $limit = $_[2];
		my $dbconn = $_[3];			# assumes that the db and coll have been set
		my $dbconnDst = $_[4];
		my $cursor = $dbconn->find({ "_id" => { '$gt' => $minkey }},
			{ "_id" => 1 })->sort({ "_id" => "1" })->limit($limit);
		my $page_obj = page->new()->processCursor( $cursor );
		my $last_id = $page_obj->get_last_id();	
		my $obj_index = pages->new()->getFirstAndLastId($page_obj) if ( $last_id ne "" );
		check->new()->getPreDefinedPageIds( $dbconnDst , $obj_index,
			$limit, $page_obj ) if ( $last_id ne "" );
		undef $cursor;			# unset objects to clear up memory
		undef $obj_index;		# manual garbage collection here
		undef $page_obj;		# more of the same
		page->new()->getPageIds(
			$last_id , $limit , $dbconn, $dbconnDst
		) if ( $last_id ne "" );
	
	}
}
{
	package check;
	use Moose;
	use MongoDB;
	use MongoDB::Code;
	use Digest::MD5 qw(md5_hex);

	sub drillDownToNonMatch {
		#use array of values, compare against new values
		my $orig_index = $_[1];
		my $new_page = $_[2];
		my $orig_page = $_[3];

		foreach my $orig_value ( @{ $orig_page->object_list }) {
			foreach my $new_value ( @{ $new_page->object_list }) {
				if ( $orig_value->get_value() eq $new_value->get_value() ) {
					$orig_value->set_position("OK");
				}
			}
		}

		foreach my $orig_value ( @{ $orig_page->object_list }) {
			if ( $orig_value->get_position() ne "OK" ) {
				print "\t\t\tMissing _id: ".$orig_value->get_value()."\n";
			}		
		}
		
	}

	sub compareMd5Hahses {
		my $index_obj = $_[1];
		my $dstHash = $_[2];
		my $srcHash = $index_obj->get_checksum();
		my $return_val = 0;
		
		if ( $srcHash eq $dstHash ) {
			print ":)\t$srcHash == $dstHash\n";
			$return_val = 1;
		} else {
			print ":(\t$srcHash != $dstHash\n";
			$return_val = 0;
		}
		return $return_val;
	}

	sub getPreDefinedPageIds {
		
		# check if compare matches, if not, pass it to drill down
		my $dbconn = $_[1];
		my $index_obj = $_[2];
		my $limit = $_[3];
		my $orig_page = $_[4];
		my $stringOfIds = "";
		my $cursor = $dbconn->find({ "_id" => { '$gte' => $index_obj->get_first(),
			'$lte' => $index_obj->get_final() }},{ "_id" => 1 })->sort({ "_id" => "1" });
		my $page_obj = page->new()->processCursor( $cursor );
		
		if ( check->new()->compareMd5Hahses(
				$index_obj, $page_obj->get_md5hash()
			) == 0 ) {
				print "\t\tdrilling down\n";
				check->new()->drillDownToNonMatch( $index_obj, $page_obj, $orig_page );
		}
	}

}
{
	package main;
	use Moose;
	use MongoDB;
	use MongoDB::Code;
	$MongoDB::Cursor::slave_okay = 1; 
	use Getopt::Long;

	my ( $uriSrc, $uriDst, $limit, $srcDb, $srcCol, $dstDb, $dstCol, $minkey ) = "";
	GetOptions(
		'uriSrc=s' => \$uriSrc,
		'uriDst=s' => \$uriDst,
		'limit=s' => \$limit,
		'srcDb=s' => \$srcDb,
		'srcCol=s' => \$srcCol,
		'dstDb=s' => \$dstDb,
		'dstCol=s' => \$dstCol,
		'minkey=s' => \$minkey
	);

	my $dbconnSrc = MongoDB::Connection->new(host => 'mongodb://'.$uriSrc,
		query_timeout => -1 );
	my $dbconnDst = MongoDB::Connection->new(host => 'mongodb://'.$uriDst,
		query_timeout => -1 );
	my $page_init = page->new();
	$page_init->getPageIds(
		$minkey , $limit , $dbconnSrc->$srcDb->$srcCol , $dbconnDst->$dstDb->$dstCol
	);

}






