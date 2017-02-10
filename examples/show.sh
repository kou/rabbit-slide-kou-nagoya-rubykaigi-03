#!/bin/sh

export LD_LIBRARY_PATH=/tmp/local/lib:$LD_LIBRARY_PATH
export GI_TYPELIB_PATH=/tmp/local/lib/girepository-1.0:$GI_TYPELIB_PATH

base_dir=$(dirname $0)
data_dir=$base_dir/../data

for score in tf tfidf; do
    for filter in raw filtered; do
	echo "$score/$filter"
	ruby \
	    -I ~/work/ruby/rarrow/lib \
	    -I ~/work/ruby/rroonga/lib \
	    -I ~/work/ruby/rroonga/ext/groonga \
	    $base_dir/show-related-terms.rb \
	    ~/work/ruby/rurema-search/groonga-database/bitclust.db \
	    $data_dir/topics.$score.$filter
    done
done
