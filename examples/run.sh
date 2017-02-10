#!/bin/sh

export LD_LIBRARY_PATH=/tmp/local/lib:$LD_LIBRARY_PATH
export GI_TYPELIB_PATH=/tmp/local/lib/girepository-1.0:$GI_TYPELIB_PATH

base_dir=$(dirname $0)
data_dir=$base_dir/data

mkdir -p $data_dir

for score in tf tfidf; do
    for filter in raw filtered; do
	(
	    ruby \
		-I ~/work/ruby/rarrow/lib \
		-I ~/work/ruby/rroonga/lib \
		-I ~/work/ruby/rroonga/ext/groonga \
		$base_dir/write-bow.rb \
		~/work/ruby/rurema-search/groonga-database/bitclust.db \
		$data_dir/bow.metadata.$score.$filter \
		$data_dir/bow.data.$score.$filter \
		$score \
		$filter &&
	    python \
		$base_dir/estimate-topics.py \
		$data_dir/bow.metadata.$score.$filter \
		$data_dir/bow.data.$score.$filter \
		$data_dir/topics.$score.$filter
	) &
    done
done

wait
