# Uasge: hbase org.jruby.Main test.rb

include Java
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin

config = HBaseConfiguration.new()
admin = HBaseAdmin.new(config)

tables = admin.listTables()
tables.each { |table| print table.getNameAsString() + "\n" } 

