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



get '/hi' do
  "Hello World!"
  end

get '/single_keyword3' do
    keyword = params['keyword']
    qf = column_keyword_substring_filter(keyword)
    ret = Array.new()
    table.filter(qf).to_a.each do |result|
        result.each do |cell|
            rowkey = cell.string
            timestamp = cell.ts
            timedelta = timestamp >> 32
            seq = (timestamp & 0x00000000ffff0000) >> 16
            print cell.string + "\t" + timedelta.to_s + "\t" + seq.to_s + "\n"
            packed = [seq, timedelta % 60, 2].pack('L>CC')
            row = table2.get(rowkey)
            ret.push(row.string('event:' + packed))
        end
    end
    return ret.to_json
end




get '/single_keyword' do
    keyword = params['keyword']
    qf = column_keyword_substring_filter(keyword)
    ret = Array.new()
    query_hash = Hash.new()
    total_matches = 0
    scan_start_time = Time.new()
    table.filter(qf).to_a.each do |result|
        result.each do |cell|
            rowkey = cell.string
            timestamp = cell.ts
            timedelta = timestamp >> 32
            seq = (timestamp & 0x00000000ffff0000) >> 16
            print cell.string + "\t" + timedelta.to_s + "\t" + seq.to_s + "\n"
            packed = [seq, timedelta % 60, 2].pack('L>CC')
            if query_hash.key?(rowkey)
                query_hash[rowkey].push(packed)
            else
                query_hash[rowkey] = [packed]
            end
            total_matches += 1
        end
    end
    scan_end_time = Time.new()
    print "Total matches " + total_matches.to_s + ". Total time: " + (scan_end_time - scan_start_time).to_s + "s\n"
    table2.get(query_hash.keys).each do |result|
        rowkey = result.rowkey
        query_hash[rowkey].each do |colkey|
            ret.push(result.string('event:' + colkey))
        end

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
