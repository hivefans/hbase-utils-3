DEBUG=true

require 'hbase-jruby'
HBase.resolve_dependency! :local
hbase = HBase.new
table = hbase[:log_search_201302]
table2 = hbase[:log_201302]

import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.filter.SubstringComparator
import org.apache.hadoop.hbase.filter.CompareFilter
import org.apache.hadoop.hbase.filter.ValueFilter
import org.apache.hadoop.hbase.filter.QualifierFilter
import org.apache.hadoop.hbase.filter.BinaryComparator 
import org.apache.hadoop.hbase.filter.RowFilter

require 'sinatra'
require 'json'

configure do
    set :bind => '192.168.1.139'
end

def column_keyword_substring_filter(keyword)
    cf = CompareFilter::CompareOp.valueOf('EQUAL')
    sc = SubstringComparator.new(keyword)
    return QualifierFilter.new(cf, sc)
end

def value_keyword_substring_filter(keyword)
    cf = CompareFilter::CompareOp.valueOf('EQUAL')
    sc = SubstringComparator.new(keyword)
    return ValueFilter.new(cf, sc)
end

def row_filter(rowkey)
    cf = CompareFilter::CompareOp.valueOf('EQUAL')
    sc = BinaryComparator.new(rowkey)
    return RowFilter.new(cf, sc)
end



get '/hi' do
  "Hello World!"
  end

get '/single_keyword3' do
    keyword = params['keyword']
    qf = column_keyword_substring_filter(keyword)
    ret = Array.new()
    table.filter(qf).to_a.each do |result|
        if DEBUG
            print result.rowkey + "\n"
        end
        result.each do |cell|
            rowkey = cell.string
            timestamp = cell.ts
            timedelta = timestamp >> 32
            seq = (timestamp & 0x00000000ffff0000) >> 16
            if DEBUG
                print cell.string + "\t" + timedelta.to_s + "\t" + seq.to_s + "\n"
            end
            packed = [seq, timedelta % 60, 2].pack('L>CC')
            row = table2.get(rowkey)
            ret.push(row.string('event:' + packed))
        end
    end
    return ret.to_json
end

def query_by_keyword_and_timerange(table, keyword, startkey=nil, endkey=nil)
    qf = column_keyword_substring_filter(keyword)
    query_hash = Hash.new()
    filtered = nil
    if startkey and endkey
        print startkey + "\n"
        print endkey + "\n"
    end
    if startkey and endkey
        filtered = table.range(startkey..endkey).filter(qf).to_a
    else
        filtered = table.filter(qf).to_a
    end
    filtered.each do |result|
        if DEBUG
            print result.rowkey + "\n"
        end
        result.each do |cell|
            rowkey = cell.string
            timestamp = cell.ts
            timedelta = timestamp >> 32
            seq = (timestamp & 0x00000000ffff0000) >> 16
            if DEBUG
                #print cell.string + "\t" + timedelta.to_s + "\t" + seq.to_s + "\n"
            end
            packed = [seq, timedelta % 60, 2].pack('L>CC')
            if query_hash.key?(rowkey)
                query_hash[rowkey].push(packed)
            else
                query_hash[rowkey] = [packed]
            end
        end
    end
    return query_hash
end

def create_query_rowkey(source, timestamp)
    rowkey = ""
    params = source.split('_')
    params.each do |mystring|
        rowkey += "%08x" % mystring.to_i
    end
    rowkey += "%08x" % timestamp
    return rowkey
end

def generate_result(table, query_hash)
    ret = []
    table.get(query_hash.keys).each do |result|
        rowkey = result.rowkey
        if DEBUG
            result.each do |cell|
                #print cell.string + cell.qualifier.gsub("event:", "").unpack('L>CC').to_s + "\n"
            end
        end
        query_hash[rowkey].each do |colkey|
            #print colkey.unpack('L>CC').to_s + "\n"
            #print result.string('event:' + colkey) + "\n"
            ret.push(result.string('event:' + colkey))
        end
    end
    return ret
end



get '/single_keyword' do
    keyword = params['keyword']
    rowkey = params['rowkey']
    sources = params['sources']
    days = params['days']
    ret = Array.new()
    total_matches = 0
    scan_start_time = Time.new()
    query_hashs = Array.new()
    end_time = (Time.now().to_i / 86400)
    start_time = end_time - (days.to_i - 1)
    if sources
        sources.each do |source|
            startkey = create_query_rowkey(source, start_time)
            endkey = create_query_rowkey(source, end_time)
            query_hashs.push(query_by_keyword_and_timerange(table, keyword, startkey, endkey))
        end
    else
        query_hashs.push(query_by_keyword_and_timerange(table, keyword))
    end
    scan_end_time = Time.new()
    print "Total matches " + total_matches.to_s + ". Total time: " + (scan_end_time - scan_start_time).to_s + "s\n"
    ret = []
    query_hashs.each do |query_hash|
        ret.push(generate_result(table2, query_hash))
    end
    return ret.to_json
end

get '/single_keyword2' do
    keyword = params['keyword']
    vf = value_keyword_substring_filter(keyword)
    ret = Array.new()
    total_matches = 0
    table2.filter(vf).to_a.each do |result|
        result.each do |cell|
            ret.push(cell.value)
            total_matches += 1
        end
    end
    print "Total matches: " + total_matches.to_s + "\n"
    return ret.to_json
end




get '/multi_keyword' do
    keywords = params['keywords']
    print keywords
    ret = Array.new()
    #keywords.each do |keyword|
    qf1 = column_keyword_substring_filter(keywords[0])
    qf2 = column_keyword_substring_filter(keywords[1])
    table.filter(qf1).filter(qf2).to_a.each do |result|
        ret.push(result.rowkey)
    end
    return ret.to_json
end
