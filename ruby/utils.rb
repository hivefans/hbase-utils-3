# Uasge: hbase org.jruby.Main test.rb

include Java
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.HTableDescriptor

include_class('java.lang.Integer') {|package,name| "J#{name}" }
include_class('java.lang.Long') {|package,name| "J#{name}" }
include_class('java.lang.Boolean') {|package,name| "J#{name}" }

class Utils
    def initialize()
        config = HBaseConfiguration.new()
        @admin = HBaseAdmin.new(config)
    end

    def list()
        tables = admin.listTables()
        tables.each { |table| print table.getNameAsString() + "\n" }
    end

    def createTable(table_name, families, splits, recreate=false)
        if exists?(table_name)
            print 'table exist %s' % table_name
            if recreate
                @admin.disableTable(table_name)
                @admin.deleteTable(table_name)
            end
        end
        htd = org.apache.hadoop.hbase.HTableDescriptor.new(table_name)
        families.each do |family|
            name = family['NAME']
            hfd = org.apache.hadoop.hbase.HColumnDescriptor.new(name.to_java_bytes)
            hfd.setInMemory(JBoolean.valueOf(family["IN_MEMORY"])) if family.has_key?("IN_MEMORY")
            hfd.setBlocksize(JInteger.valueOf(family["BLOCKSIZE"])) if family.has_key?("BLOCKSIZE")
            hfd.setMaxVersions(JInteger.valueOf(family["VERSIONS"])) if family.has_key?("VERSIONS")
            if family.has_key?("BLOOMFILTER")
                bloomtype = family["BLOOMFILTER"].upcase
                if org.apache.hadoop.hbase.regionserver.StoreFile::BloomType.constants.include?(bloomtype)
                    hfd.setBloomFilterType(org.apache.hadoop.hbase.regionserver.StoreFile::BloomType.valueOf(bloomtype))
                end
            end
            if family.has_key?("COMPRESSION")
                compression = family["COMPRESSION"].upcase
                # in most recent version of hbase, org.apache.hadoop.hbase.io.hfile.Compression has been moved 
                # to org.apache.hadoop.hbase.io.compression.Compression
                if org.apache.hadoop.hbase.io.hfile.Compression::Algorithm.constants.include?(compression)
                    hfd.setCompressionType(org.apache.hadoop.hbase.io.hfile.Compression::Algorithm.valueOf(compression))
                end
            end
            htd.addFamily(hfd)
            print "Creating table with %s\n" % htd
        end
        @admin.createTable(htd, splits)
        print "finished create table %s\n" % table_name
    end

    def createMonthlySummaryTable(pod_id=1)
        time = Time.new
        year = time.year
        month = time.month
        if month == 12
            year += 1
            month = 1
        else
            month += 1
        end
        table_name = 'summary_%04d%02d' % [year, month]
        families = []
        family = Hash.new("summary")
        family['NAME'] = 'summary'
        family['COMPRESSION'] = 'LZO'
        family['BLOOMFILTER'] = 'ROWCOL'
        family['VERSIONS'] = 1
        families.push(family)
        splits = Java::byte[][20].new
        if pod_id == 1
            (0..19).each do |i|
                split = "%08x" % (i * 500) + '0' * 46
                splits[i] = org.apache.hadoop.hbase.util.Bytes.toBytesBinary(split)
            end
        else
            (0..19).each do |i|
                split = "%08x%08x" % [2, i * 800] + '0' * 38
                splits[i] = org.apache.hadoop.hbase.util.Bytes.toBytesBinary(split)
            end
        end
        createTable(table_name, families, splits, true)              
    end

    def createMonthlyApplicationsTable()
        time = Time.new
        year = time.year
        month = time.month
        if month == 12
            year += 1
            month = 1
        else
            month += 1
        end
        table_name = 'applications_%04d%02d' % [year, month]
        families = []
        family = Hash.new("")
        family['NAME'] = 'process_list'
        family['COMPRESSION'] = 'LZO'
        family['BLOOMFILTER'] = 'ROWCOL'
        family['VERSIONS'] = 1
        families.push(family)
        splits = Java::byte[][20].new
        (0..19).each do |i|
            split = "%08x" % (i * 400) + '0' * 16
            splits[i] = org.apache.hadoop.hbase.util.Bytes.toBytesBinary(split)
        end
        createTable(table_name, families, splits, true)              
    end

    def createMonthlyLogTable()
        time = Time.new
        year = time.year
        month = time.month
        if month == 12
            year += 1
            month = 1
        else
            month += 1
        end
        table_name = 'log_%04d%02d' % [year, month]
        families = []
        family = Hash.new("event")
        family['NAME'] = 'event'
        family['COMPRESSION'] = 'LZO'
        family['BLOOMFILTER'] = 'ROWCOL'
        family['VERSIONS'] = 1
        families.push(family)
        splits = Java::byte[][16].new
        (0..15).each do |i|
            split = "%x" % i + '0' * 31
            splits[i] = org.apache.hadoop.hbase.util.Bytes.toBytesBinary(split)
        end
        createTable(table_name, families, splits, true)              
    end

    def createMonthlyRawTable()
        time = Time.new
        year = time.year
        month = time.month
        if month == 12
            year += 1
            month = 1
        else
            month += 1
        end
        table_name = 'raw_%04d%02d' % [year, month]
        families = []
        family = Hash.new("snapshot")
        family['NAME'] = 'snapshot'
        family['COMPRESSION'] = 'LZO'
        family['VERSIONS'] = 1
        family['BLOCKSIZE'] = 1048576
        families.push(family)
        splits = Java::byte[][256].new
        (0..15).each do |i|
            (0..15).each do |j|
                split = i.to_s(16) +  j.to_s(16) + "0" * 30
                splits[i * 16 + j] = org.apache.hadoop.hbase.util.Bytes.toBytesBinary(split)
            end
        end
        createTable(table_name, families, splits, true)              
    end

    def exists?(table_name)
        @admin.tableExists(table_name)
    end
end
