# Uasge: hbase org.jruby.Main create_monthly_table.rb 

include Java
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin

require 'utils'
require 'config'
include CONFIG

utils = Utils.new
utils.createMonthlyLogTable('log_201303_new')
