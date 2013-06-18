#
# Copyright 2011 The Apache Software Foundation
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Assign table regions to server with data locality
#
# To see usage for this script, run:
#
#  ${HBASE_HOME}/bin/hbase org.jruby.Main local_assign.rb
#     Version 0.3 (01/28/2013)
#
include Java
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.HRegionInfo
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HServerAddress
import org.apache.hadoop.hbase.regionserver.HRegion
import org.apache.hadoop.hbase.regionserver.Store
import org.apache.hadoop.hbase.regionserver.StoreFile
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.FSUtils
import org.apache.hadoop.hbase.util.Writables
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.ipc.RemoteException
import java.util.PriorityQueue
import java.util.HashMap
import java.util.ArrayList
import java.lang.Byte

# Name of this script
NAME = "local_assign"

tableName = ARGV[0]
if tableName.nil?
  puts "Usage: hbase org.jruby.Main local_assign.rb <table_name>"
  exit 1
end

class ServerWithRegions
    include java.lang.Comparable
  
  attr_accessor :server
  attr_accessor :regions
  
  def initialize(server, regions)
    self.server = server
    self.regions = regions
  end

  def add(region)
    self.regions.add(region)
  end

  def compareTo(o)
    return (self.regions.size - o.regions.size)
  end
end

def findServer(conf, tableDescriptor, region, regionServerNameMap, maxAssign, serverAssignmentCountMap)
  topHosts = HRegion::computeHDFSBlocksDistribution(
      conf, tableDescriptor, region.getEncodedName).getTopHosts
  if topHosts != nil && topHosts.size > 0
    i = 0
    while i < topHosts.size
      topHost = topHosts.get(i)
      assignCount = serverAssignmentCountMap.get(topHost)
      if regionServerNameMap.get(topHost) != nil && assignCount < maxAssign
        serverAssignmentCountMap.put(topHost, assignCount + 1)
        return topHost
      end
      i = i + 1
    end
  end
  return nil
end

LOG = LogFactory.getLog(NAME)

c = HBaseConfiguration.create()
c.set("fs.default.name", c.get(HConstants::HBASE_DIR))
admin = HBaseAdmin.new(c)
lastValue = admin.balanceSwitch(false)

serverAssignmentCountMap = HashMap.new
regionServerNameMap = HashMap.new
hostRegionsMap = HashMap.new
for clusterServer in admin.getClusterStatus.getServers
  serverAssignmentCountMap.put(clusterServer.getHostname, 0)
  regionServerNameMap.put(clusterServer.getHostname, clusterServer)
  hostRegionsMap.put(clusterServer.getHostname, ArrayList.new)
end

emptyRegions = ArrayList.new

tableDescriptor = admin.getTableDescriptor(tableName.to_java_bytes)
regions = admin.getTableRegions(tableName.to_java_bytes)

maxAssign = regions.size / regionServerNameMap.size

for region in regions
  server = findServer(c, tableDescriptor, region, regionServerNameMap, maxAssign, serverAssignmentCountMap)
  if server != nil
    regionList = hostRegionsMap.get(server)
    if regionList == nil
      regionList = ArrayList.new
      hostRegionsMap.put(server, regionList)
    end
    regionList.add(region)
  else
    puts "No region server for " + region.getEncodedName
    emptyRegions.add(region)
  end
end

serverRegionsQ = PriorityQueue.new
for server in hostRegionsMap
  serverRegionsQ.add(ServerWithRegions.new(server[0], server[1]))
end

if emptyRegions.size > 0
  for region in emptyRegions
    serverRegions = serverRegionsQ.poll()
    serverRegions.regions.add(region)
    serverRegionsQ.add(serverRegions)
  end
end

for serverRegions in serverRegionsQ
  host = serverRegions.server
  #puts "Looking for server :" + serverRegions.server
  server = regionServerNameMap.get(serverRegions.server).getServerName
  puts "Host: " + host + ", Server = " + server
  for region in serverRegions.regions
    puts "Assigning region = " + region.getEncodedName
    admin.move(region.getEncodedName.to_java_bytes, server.to_java_bytes)
  end
end

#puts "Host: " + serverRegion.server + ", regions = " + serverRegion.regions.size.to_s
admin.balanceSwitch(lastValue)
admin.close
