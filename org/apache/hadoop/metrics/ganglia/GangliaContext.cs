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
using Sharpen;

namespace org.apache.hadoop.metrics.ganglia
{
	/// <summary>Context for sending metrics to Ganglia.</summary>
	public class GangliaContext : org.apache.hadoop.metrics.spi.AbstractMetricsContext
	{
		private const string PERIOD_PROPERTY = "period";

		private const string SERVERS_PROPERTY = "servers";

		private const string UNITS_PROPERTY = "units";

		private const string SLOPE_PROPERTY = "slope";

		private const string TMAX_PROPERTY = "tmax";

		private const string DMAX_PROPERTY = "dmax";

		private const string MULTICAST_PROPERTY = "multicast";

		private const string MULTICAST_TTL_PROPERTY = "multicast.ttl";

		private const string DEFAULT_UNITS = string.Empty;

		private const string DEFAULT_SLOPE = "both";

		private const int DEFAULT_TMAX = 60;

		private const int DEFAULT_DMAX = 0;

		private const int DEFAULT_PORT = 8649;

		private const int BUFFER_SIZE = 1500;

		private const int DEFAULT_MULTICAST_TTL = 1;

		private readonly org.apache.commons.logging.Log LOG;

		private static readonly System.Collections.Generic.IDictionary<java.lang.Class, string
			> typeTable = new System.Collections.Generic.Dictionary<java.lang.Class, string>
			(5);

		static GangliaContext()
		{
			LOG = org.apache.commons.logging.LogFactory.getLog(Sharpen.Runtime.getClassForObject
				(this));
			// as per libgmond.c
			typeTable[Sharpen.Runtime.getClassForType(typeof(string))] = "string";
			typeTable[Sharpen.Runtime.getClassForType(typeof(byte))] = "int8";
			typeTable[Sharpen.Runtime.getClassForType(typeof(short))] = "int16";
			typeTable[Sharpen.Runtime.getClassForType(typeof(int))] = "int32";
			typeTable[Sharpen.Runtime.getClassForType(typeof(long))] = "float";
			typeTable[Sharpen.Runtime.getClassForType(typeof(float))] = "float";
		}

		protected internal byte[] buffer = new byte[BUFFER_SIZE];

		protected internal int offset;

		protected internal System.Collections.Generic.IList<java.net.SocketAddress> metricsServers;

		private System.Collections.Generic.IDictionary<string, string> unitsTable;

		private System.Collections.Generic.IDictionary<string, string> slopeTable;

		private System.Collections.Generic.IDictionary<string, string> tmaxTable;

		private System.Collections.Generic.IDictionary<string, string> dmaxTable;

		private bool multicastEnabled;

		private int multicastTtl;

		protected internal java.net.DatagramSocket datagramSocket;

		/// <summary>Creates a new instance of GangliaContext</summary>
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public GangliaContext()
		{
			LOG = org.apache.commons.logging.LogFactory.getLog(Sharpen.Runtime.getClassForObject
				(this));
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public override void init(string contextName, org.apache.hadoop.metrics.ContextFactory
			 factory)
		{
			base.init(contextName, factory);
			parseAndSetPeriod(PERIOD_PROPERTY);
			metricsServers = org.apache.hadoop.metrics.spi.Util.parse(getAttribute(SERVERS_PROPERTY
				), DEFAULT_PORT);
			unitsTable = getAttributeTable(UNITS_PROPERTY);
			slopeTable = getAttributeTable(SLOPE_PROPERTY);
			tmaxTable = getAttributeTable(TMAX_PROPERTY);
			dmaxTable = getAttributeTable(DMAX_PROPERTY);
			multicastEnabled = bool.parseBoolean(getAttribute(MULTICAST_PROPERTY));
			string multicastTtlValue = getAttribute(MULTICAST_TTL_PROPERTY);
			if (multicastEnabled)
			{
				if (multicastTtlValue == null)
				{
					multicastTtl = DEFAULT_MULTICAST_TTL;
				}
				else
				{
					multicastTtl = System.Convert.ToInt32(multicastTtlValue);
				}
			}
			try
			{
				if (multicastEnabled)
				{
					LOG.info("Enabling multicast for Ganglia with TTL " + multicastTtl);
					datagramSocket = new java.net.MulticastSocket();
					((java.net.MulticastSocket)datagramSocket).setTimeToLive(multicastTtl);
				}
				else
				{
					datagramSocket = new java.net.DatagramSocket();
				}
			}
			catch (System.IO.IOException e)
			{
				LOG.error(e);
			}
		}

		/// <summary>method to close the datagram socket</summary>
		public override void close()
		{
			base.close();
			if (datagramSocket != null)
			{
				datagramSocket.close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		protected internal override void emitRecord(string contextName, string recordName
			, org.apache.hadoop.metrics.spi.OutputRecord outRec)
		{
			// Setup so that the records have the proper leader names so they are
			// unambiguous at the ganglia level, and this prevents a lot of rework
			java.lang.StringBuilder sb = new java.lang.StringBuilder();
			sb.Append(contextName);
			sb.Append('.');
			if (contextName.Equals("jvm") && outRec.getTag("processName") != null)
			{
				sb.Append(outRec.getTag("processName"));
				sb.Append('.');
			}
			sb.Append(recordName);
			sb.Append('.');
			int sbBaseLen = sb.Length;
			// emit each metric in turn
			foreach (string metricName in outRec.getMetricNames())
			{
				object metric = outRec.getMetric(metricName);
				string type = typeTable[Sharpen.Runtime.getClassForObject(metric)];
				if (type != null)
				{
					sb.Append(metricName);
					emitMetric(sb.ToString(), type, metric.ToString());
					sb.Length = sbBaseLen;
				}
				else
				{
					LOG.warn("Unknown metrics type: " + Sharpen.Runtime.getClassForObject(metric));
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void emitMetric(string name, string type, string value
			)
		{
			string units = getUnits(name);
			int slope = getSlope(name);
			int tmax = getTmax(name);
			int dmax = getDmax(name);
			offset = 0;
			xdr_int(0);
			// metric_user_defined
			xdr_string(type);
			xdr_string(name);
			xdr_string(value);
			xdr_string(units);
			xdr_int(slope);
			xdr_int(tmax);
			xdr_int(dmax);
			foreach (java.net.SocketAddress socketAddress in metricsServers)
			{
				java.net.DatagramPacket packet = new java.net.DatagramPacket(buffer, offset, socketAddress
					);
				datagramSocket.send(packet);
			}
		}

		protected internal virtual string getUnits(string metricName)
		{
			string result = unitsTable[metricName];
			if (result == null)
			{
				result = DEFAULT_UNITS;
			}
			return result;
		}

		protected internal virtual int getSlope(string metricName)
		{
			string slopeString = slopeTable[metricName];
			if (slopeString == null)
			{
				slopeString = DEFAULT_SLOPE;
			}
			return ("zero".Equals(slopeString) ? 0 : 3);
		}

		// see gmetric.c
		protected internal virtual int getTmax(string metricName)
		{
			if (tmaxTable == null)
			{
				return DEFAULT_TMAX;
			}
			string tmaxString = tmaxTable[metricName];
			if (tmaxString == null)
			{
				return DEFAULT_TMAX;
			}
			else
			{
				return System.Convert.ToInt32(tmaxString);
			}
		}

		protected internal virtual int getDmax(string metricName)
		{
			string dmaxString = dmaxTable[metricName];
			if (dmaxString == null)
			{
				return DEFAULT_DMAX;
			}
			else
			{
				return System.Convert.ToInt32(dmaxString);
			}
		}

		/// <summary>
		/// Puts a string into the buffer by first writing the size of the string
		/// as an int, followed by the bytes of the string, padded if necessary to
		/// a multiple of 4.
		/// </summary>
		protected internal virtual void xdr_string(string s)
		{
			byte[] bytes = Sharpen.Runtime.getBytesForString(s, org.apache.commons.io.Charsets
				.UTF_8);
			int len = bytes.Length;
			xdr_int(len);
			System.Array.Copy(bytes, 0, buffer, offset, len);
			offset += len;
			pad();
		}

		/// <summary>Pads the buffer with zero bytes up to the nearest multiple of 4.</summary>
		private void pad()
		{
			int newOffset = ((offset + 3) / 4) * 4;
			while (offset < newOffset)
			{
				buffer[offset++] = 0;
			}
		}

		/// <summary>Puts an integer into the buffer as 4 bytes, big-endian.</summary>
		protected internal virtual void xdr_int(int i)
		{
			buffer[offset++] = unchecked((byte)((i >> 24) & unchecked((int)(0xff))));
			buffer[offset++] = unchecked((byte)((i >> 16) & unchecked((int)(0xff))));
			buffer[offset++] = unchecked((byte)((i >> 8) & unchecked((int)(0xff))));
			buffer[offset++] = unchecked((byte)(i & unchecked((int)(0xff))));
		}
	}
}
