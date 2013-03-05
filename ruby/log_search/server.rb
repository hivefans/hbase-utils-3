require '../config'
require 'hbase-jruby'

include CONFIG
HBase.resolve_dependency! :local
hbase = HBase.new
table = hbase[:log_search_201303]
table2 = hbase[:log_201303]

import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.filter.SubstringComparator
import org.apache.hadoop.hbase.filter.CompareFilter
import org.apache.hadoop.hbase.filter.ValueFilter
import org.apache.hadoop.hbase.filter.QualifierFilter
import org.apache.hadoop.hbase.filter.BinaryComparator 
import org.apache.hadoop.hbase.filter.RowFilter

require 'sinatra'
require 'json'
require 'digest/md5'
require 'log_event'

configure do
    set :bind => LOG_SERACH_BIND_ADDRESS
    set :server => :puma 
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


def query_by_keyword_value_filter(table, keywords, startkey=nil, endkey=nil)
    filtered = table    
    filterList = FilterList.new(FilterList::Operator.valueOf("MUST_PASS_ALL"))
    if startkey and endkey and DEBUG
        print startkey + "\n"
        print endkey + "\n"
    end
    if startkey and endkey
        filtered = table.range(startkey..endkey)
    else
        filtered = table
    end
    keywords.each do |keyword|
        #print keyword + "\n"
        vf = value_keyword_substring_filter(keyword)
        #qf = column_keyword_substring_filter(keyword)
        #filtered = filtered.filter(vf)
        filterList.addFilter(vf)
    end
    filtered = filtered.filter(filterList)
    ret = Array.new()
    filtered = filtered.to_a
    filtered.each do |result|
        result.each do |cell|
            ret.push(cell.string)
            #begin
            #    mylog = LogEvent.new()
            #    mylog.parse_from_string(cell.string)
            #    ret.push(mylog.name)
            #    #ret.push([mylog.time, mylog.severity, mylog.data, mylog.name])
            #rescue Exception => e
            #    print e.message + "\n"
            #end
        end
    end
    return ret
end
     

def query_by_keyword_and_timerange(table, keywords, startkey=nil, endkey=nil)
    query_hash = Hash.new()
    filtered = nil
    if startkey and endkey and DEBUG
        print startkey + "\n"
        print endkey + "\n"
    end
    if startkey and endkey
        filtered = table.range(startkey..endkey)
    else
        filtered = table
    end
    #filterList = FilterList.new(FilterList::Operator.valueOf('MUST_PASS_ALL'));
    filterList = FilterList.new(FilterList::Operator.valueOf("MUST_PASS_ONE"))
    keywords.each do |keyword|
        print keyword + "\n"
        qf = column_keyword_substring_filter(keyword)
        #qf = column_keyword_substring_filter(keyword)
        #filtered = filtered.filter(qf)
        filterList.addFilter(qf)
    end
    filtered = filtered.filter(filterList)
    
    filtered = filtered.to_a
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

def create_event_query_rowkey(source, timestamp)
    rowkey = ""
    params = source.split('_')
    params.each do |mystring|
        rowkey += "%08x" % mystring.to_i
    end
    rowkey = Digest::MD5.hexdigest(rowkey)
    rowkey += "%08x" % (timestamp / 60).to_i
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
            query_hashs.push(query_by_keyword_and_timerange(table, [keyword], startkey, endkey))
        end
    else
        query_hashs.push(query_by_keyword_and_timerange(table, [keyword]))
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

get '/search' do
    keywords = params['keywords']
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
            query_hashs.push(query_by_keyword_and_timerange(table, keywords, startkey, endkey))
        end
    else
        query_hashs.push(query_by_keyword_and_timerange(table, keywords))
    end
    scan_end_time = Time.new()
    print "Total matches " + total_matches.to_s + ". Total time: " + (scan_end_time - scan_start_time).to_s + "s\n"
    ret = []
    query_hashs.each do |query_hash|
        ret.push(generate_result(table2, query_hash))
    end
    return ret.to_json
end


get '/search_by_keywords' do 
    keywords = params['keywords']
    sources = params['sources']
    days = params['days']
    if not days
        days = 1
    end
    end_time = Time.now().to_i
    start_time = end_time - days.to_i * 86400
    ret = Array.new()
    if sources
        sources.each do |source|
            startkey = create_event_query_rowkey(source, start_time)
            endkey = create_event_query_rowkey(source, end_time)
            ret.push(query_by_keyword_value_filter(table2, keywords, startkey, endkey))
            end
    else    
        ret = query_by_keyword_value_filter(table2, keywords)
    end
    return ret.to_json
end
