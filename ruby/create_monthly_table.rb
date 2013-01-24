# Uasge: hbase org.jruby.Main test.rb

include Java
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin

require 'utils'
require 'config'
include CONFIG

utils = Utils.new
utils.createMonthlySummaryTable(POD_ID)
utils.createMonthlyRawTable()
utils.createMonthlyLogTable()
utils.createMonthlyApplicationsTable()
