/*
* GangliaContext.java
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
using System.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Metrics;
using Org.Apache.Hadoop.Net;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics.Ganglia
{
	/// <summary>Context for sending metrics to Ganglia version 3.1.x.</summary>
	/// <remarks>
	/// Context for sending metrics to Ganglia version 3.1.x.
	/// 3.1.1 has a slightly different wire portal compared to 3.0.x.
	/// </remarks>
	public class GangliaContext31 : GangliaContext
	{
		internal string hostName = "UNKNOWN.example.com";

		private static readonly Log Log = LogFactory.GetLog("org.apache.hadoop.util.GangliaContext31"
			);

		public override void Init(string contextName, ContextFactory factory)
		{
			base.Init(contextName, factory);
			Log.Debug("Initializing the GangliaContext31 for Ganglia 3.1 metrics.");
			// Take the hostname from the DNS class.
			Configuration conf = new Configuration();
			if (conf.Get("slave.host.name") != null)
			{
				hostName = conf.Get("slave.host.name");
			}
			else
			{
				try
				{
					hostName = DNS.GetDefaultHost(conf.Get("dfs.datanode.dns.interface", "default"), 
						conf.Get("dfs.datanode.dns.nameserver", "default"));
				}
				catch (UnknownHostException uhe)
				{
					Log.Error(uhe);
					hostName = "UNKNOWN.example.com";
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void EmitMetric(string name, string type, string value
			)
		{
			if (name == null)
			{
				Log.Warn("Metric was emitted with no name.");
				return;
			}
			else
			{
				if (value == null)
				{
					Log.Warn("Metric name " + name + " was emitted with a null value.");
					return;
				}
				else
				{
					if (type == null)
					{
						Log.Warn("Metric name " + name + ", value " + value + " has no type.");
						return;
					}
				}
			}
			Log.Debug("Emitting metric " + name + ", type " + type + ", value " + value + " from hostname"
				 + hostName);
			string units = GetUnits(name);
			int slope = GetSlope(name);
			int tmax = GetTmax(name);
			int dmax = GetDmax(name);
			offset = 0;
			string groupName = Sharpen.Runtime.Substring(name, 0, name.LastIndexOf("."));
			// The following XDR recipe was done through a careful reading of
			// gm_protocol.x in Ganglia 3.1 and carefully examining the output of
			// the gmetric utility with strace.
			// First we send out a metadata message
			Xdr_int(128);
			// metric_id = metadata_msg
			Xdr_string(hostName);
			// hostname
			Xdr_string(name);
			// metric name
			Xdr_int(0);
			// spoof = False
			Xdr_string(type);
			// metric type
			Xdr_string(name);
			// metric name
			Xdr_string(units);
			// units
			Xdr_int(slope);
			// slope
			Xdr_int(tmax);
			// tmax, the maximum time between metrics
			Xdr_int(dmax);
			// dmax, the maximum data value
			Xdr_int(1);
			/*Num of the entries in extra_value field for
			Ganglia 3.1.x*/
			Xdr_string("GROUP");
			/*Group attribute*/
			Xdr_string(groupName);
			/*Group value*/
			foreach (EndPoint socketAddress in metricsServers)
			{
				DatagramPacket packet = new DatagramPacket(buffer, offset, socketAddress);
				datagramSocket.Send(packet);
			}
			// Now we send out a message with the actual value.
			// Technically, we only need to send out the metadata message once for
			// each metric, but I don't want to have to record which metrics we did and
			// did not send.
			offset = 0;
			Xdr_int(133);
			// we are sending a string value
			Xdr_string(hostName);
			// hostName
			Xdr_string(name);
			// metric name
			Xdr_int(0);
			// spoof = False
			Xdr_string("%s");
			// format field
			Xdr_string(value);
			// metric value
			foreach (EndPoint socketAddress_1 in metricsServers)
			{
				DatagramPacket packet = new DatagramPacket(buffer, offset, socketAddress_1);
				datagramSocket.Send(packet);
			}
		}
	}
}
