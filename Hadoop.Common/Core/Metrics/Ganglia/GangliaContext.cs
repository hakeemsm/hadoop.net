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
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Metrics;
using Org.Apache.Hadoop.Metrics.Spi;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics.Ganglia
{
	/// <summary>Context for sending metrics to Ganglia.</summary>
	public class GangliaContext : AbstractMetricsContext
	{
		private const string PeriodProperty = "period";

		private const string ServersProperty = "servers";

		private const string UnitsProperty = "units";

		private const string SlopeProperty = "slope";

		private const string TmaxProperty = "tmax";

		private const string DmaxProperty = "dmax";

		private const string MulticastProperty = "multicast";

		private const string MulticastTtlProperty = "multicast.ttl";

		private const string DefaultUnits = string.Empty;

		private const string DefaultSlope = "both";

		private const int DefaultTmax = 60;

		private const int DefaultDmax = 0;

		private const int DefaultPort = 8649;

		private const int BufferSize = 1500;

		private const int DefaultMulticastTtl = 1;

		private readonly Log Log = LogFactory.GetLog(this.GetType());

		private static readonly IDictionary<Type, string> typeTable = new Dictionary<Type
			, string>(5);

		static GangliaContext()
		{
			// as per libgmond.c
			typeTable[typeof(string)] = "string";
			typeTable[typeof(byte)] = "int8";
			typeTable[typeof(short)] = "int16";
			typeTable[typeof(int)] = "int32";
			typeTable[typeof(long)] = "float";
			typeTable[typeof(float)] = "float";
		}

		protected internal byte[] buffer = new byte[BufferSize];

		protected internal int offset;

		protected internal IList<EndPoint> metricsServers;

		private IDictionary<string, string> unitsTable;

		private IDictionary<string, string> slopeTable;

		private IDictionary<string, string> tmaxTable;

		private IDictionary<string, string> dmaxTable;

		private bool multicastEnabled;

		private int multicastTtl;

		protected internal DatagramSocket datagramSocket;

		/// <summary>Creates a new instance of GangliaContext</summary>
		[InterfaceAudience.Private]
		public GangliaContext()
		{
		}

		[InterfaceAudience.Private]
		public override void Init(string contextName, ContextFactory factory)
		{
			base.Init(contextName, factory);
			ParseAndSetPeriod(PeriodProperty);
			metricsServers = Util.Parse(GetAttribute(ServersProperty), DefaultPort);
			unitsTable = GetAttributeTable(UnitsProperty);
			slopeTable = GetAttributeTable(SlopeProperty);
			tmaxTable = GetAttributeTable(TmaxProperty);
			dmaxTable = GetAttributeTable(DmaxProperty);
			multicastEnabled = System.Boolean.Parse(GetAttribute(MulticastProperty));
			string multicastTtlValue = GetAttribute(MulticastTtlProperty);
			if (multicastEnabled)
			{
				if (multicastTtlValue == null)
				{
					multicastTtl = DefaultMulticastTtl;
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
					Log.Info("Enabling multicast for Ganglia with TTL " + multicastTtl);
					datagramSocket = new MulticastSocket();
					((MulticastSocket)datagramSocket).SetTimeToLive(multicastTtl);
				}
				else
				{
					datagramSocket = new DatagramSocket();
				}
			}
			catch (IOException e)
			{
				Log.Error(e);
			}
		}

		/// <summary>method to close the datagram socket</summary>
		public override void Close()
		{
			base.Close();
			if (datagramSocket != null)
			{
				datagramSocket.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		protected internal override void EmitRecord(string contextName, string recordName
			, OutputRecord outRec)
		{
			// Setup so that the records have the proper leader names so they are
			// unambiguous at the ganglia level, and this prevents a lot of rework
			StringBuilder sb = new StringBuilder();
			sb.Append(contextName);
			sb.Append('.');
			if (contextName.Equals("jvm") && outRec.GetTag("processName") != null)
			{
				sb.Append(outRec.GetTag("processName"));
				sb.Append('.');
			}
			sb.Append(recordName);
			sb.Append('.');
			int sbBaseLen = sb.Length;
			// emit each metric in turn
			foreach (string metricName in outRec.GetMetricNames())
			{
				object metric = outRec.GetMetric(metricName);
				string type = typeTable[metric.GetType()];
				if (type != null)
				{
					sb.Append(metricName);
					EmitMetric(sb.ToString(), type, metric.ToString());
					sb.Length = sbBaseLen;
				}
				else
				{
					Log.Warn("Unknown metrics type: " + metric.GetType());
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void EmitMetric(string name, string type, string value
			)
		{
			string units = GetUnits(name);
			int slope = GetSlope(name);
			int tmax = GetTmax(name);
			int dmax = GetDmax(name);
			offset = 0;
			Xdr_int(0);
			// metric_user_defined
			Xdr_string(type);
			Xdr_string(name);
			Xdr_string(value);
			Xdr_string(units);
			Xdr_int(slope);
			Xdr_int(tmax);
			Xdr_int(dmax);
			foreach (EndPoint socketAddress in metricsServers)
			{
				DatagramPacket packet = new DatagramPacket(buffer, offset, socketAddress);
				datagramSocket.Send(packet);
			}
		}

		protected internal virtual string GetUnits(string metricName)
		{
			string result = unitsTable[metricName];
			if (result == null)
			{
				result = DefaultUnits;
			}
			return result;
		}

		protected internal virtual int GetSlope(string metricName)
		{
			string slopeString = slopeTable[metricName];
			if (slopeString == null)
			{
				slopeString = DefaultSlope;
			}
			return ("zero".Equals(slopeString) ? 0 : 3);
		}

		// see gmetric.c
		protected internal virtual int GetTmax(string metricName)
		{
			if (tmaxTable == null)
			{
				return DefaultTmax;
			}
			string tmaxString = tmaxTable[metricName];
			if (tmaxString == null)
			{
				return DefaultTmax;
			}
			else
			{
				return System.Convert.ToInt32(tmaxString);
			}
		}

		protected internal virtual int GetDmax(string metricName)
		{
			string dmaxString = dmaxTable[metricName];
			if (dmaxString == null)
			{
				return DefaultDmax;
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
		protected internal virtual void Xdr_string(string s)
		{
			byte[] bytes = Sharpen.Runtime.GetBytesForString(s, Charsets.Utf8);
			int len = bytes.Length;
			Xdr_int(len);
			System.Array.Copy(bytes, 0, buffer, offset, len);
			offset += len;
			Pad();
		}

		/// <summary>Pads the buffer with zero bytes up to the nearest multiple of 4.</summary>
		private void Pad()
		{
			int newOffset = ((offset + 3) / 4) * 4;
			while (offset < newOffset)
			{
				buffer[offset++] = 0;
			}
		}

		/// <summary>Puts an integer into the buffer as 4 bytes, big-endian.</summary>
		protected internal virtual void Xdr_int(int i)
		{
			buffer[offset++] = unchecked((byte)((i >> 24) & unchecked((int)(0xff))));
			buffer[offset++] = unchecked((byte)((i >> 16) & unchecked((int)(0xff))));
			buffer[offset++] = unchecked((byte)((i >> 8) & unchecked((int)(0xff))));
			buffer[offset++] = unchecked((byte)(i & unchecked((int)(0xff))));
		}
	}
}
