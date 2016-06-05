using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Org.Apache.Commons.Configuration;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Util;
using Org.Apache.Hadoop.Net;


namespace Org.Apache.Hadoop.Metrics2.Sink.Ganglia
{
	/// <summary>This the base class for Ganglia sink classes using metrics2.</summary>
	/// <remarks>
	/// This the base class for Ganglia sink classes using metrics2. Lot of the code
	/// has been derived from org.apache.hadoop.metrics.ganglia.GangliaContext.
	/// As per the documentation, sink implementations doesn't have to worry about
	/// thread safety. Hence the code wasn't written for thread safety and should
	/// be modified in case the above assumption changes in the future.
	/// </remarks>
	public abstract class AbstractGangliaSink : MetricsSink
	{
		public readonly Log Log = LogFactory.GetLog(this.GetType());

		public const string DefaultUnits = string.Empty;

		public const int DefaultTmax = 60;

		public const int DefaultDmax = 0;

		public static readonly AbstractGangliaSink.GangliaSlope DefaultSlope = AbstractGangliaSink.GangliaSlope
			.both;

		public const int DefaultPort = 8649;

		public const bool DefaultMulticastEnabled = false;

		public const int DefaultMulticastTtl = 1;

		public const string ServersProperty = "servers";

		public const string MulticastEnabledProperty = "multicast";

		public const string MulticastTtlProperty = "multicast.ttl";

		public const int BufferSize = 1500;

		public const string SupportSparseMetricsProperty = "supportsparse";

		public const bool SupportSparseMetricsDefault = false;

		public const string Equal = "=";

		private string hostName = "UNKNOWN.example.com";

		private DatagramSocket datagramSocket;

		private IList<EndPoint> metricsServers;

		private bool multicastEnabled;

		private int multicastTtl;

		private byte[] buffer = new byte[BufferSize];

		private int offset;

		private bool supportSparseMetrics = SupportSparseMetricsDefault;

		/// <summary>Used for visiting Metrics</summary>
		protected internal readonly GangliaMetricVisitor gangliaMetricVisitor = new GangliaMetricVisitor
			();

		private SubsetConfiguration conf;

		private IDictionary<string, GangliaConf> gangliaConfMap;

		private GangliaConf DefaultGangliaConf = new GangliaConf();

		/// <summary>ganglia slope values which equal the ordinal</summary>
		public enum GangliaSlope
		{
			zero,
			positive,
			negative,
			both
		}

		/// <summary>define enum for various type of conf</summary>
		public enum GangliaConfType
		{
			slope,
			units,
			dmax,
			tmax
		}

		/*
		* Output of "gmetric --help" showing allowable values
		* -t, --type=STRING
		*     Either string|int8|uint8|int16|uint16|int32|uint32|float|double
		* -u, --units=STRING Unit of measure for the value e.g. Kilobytes, Celcius
		*     (default='')
		* -s, --slope=STRING Either zero|positive|negative|both
		*     (default='both')
		* -x, --tmax=INT The maximum time in seconds between gmetric calls
		*     (default='60')
		*/
		// as per libgmond.c
		// 0
		// 1
		// 2
		// 3
		/*
		* (non-Javadoc)
		*
		* @see
		* org.apache.hadoop.metrics2.MetricsPlugin#init(org.apache.commons.configuration
		* .SubsetConfiguration)
		*/
		public virtual void Init(SubsetConfiguration conf)
		{
			Log.Debug("Initializing the GangliaSink for Ganglia metrics.");
			this.conf = conf;
			// Take the hostname from the DNS class.
			if (conf.GetString("slave.host.name") != null)
			{
				hostName = conf.GetString("slave.host.name");
			}
			else
			{
				try
				{
					hostName = DNS.GetDefaultHost(conf.GetString("dfs.datanode.dns.interface", "default"
						), conf.GetString("dfs.datanode.dns.nameserver", "default"));
				}
				catch (UnknownHostException uhe)
				{
					Log.Error(uhe);
					hostName = "UNKNOWN.example.com";
				}
			}
			// load the gannglia servers from properties
			metricsServers = Servers.Parse(conf.GetString(ServersProperty), DefaultPort);
			multicastEnabled = conf.GetBoolean(MulticastEnabledProperty, DefaultMulticastEnabled
				);
			multicastTtl = conf.GetInt(MulticastTtlProperty, DefaultMulticastTtl);
			// extract the Ganglia conf per metrics
			gangliaConfMap = new Dictionary<string, GangliaConf>();
			LoadGangliaConf(AbstractGangliaSink.GangliaConfType.units);
			LoadGangliaConf(AbstractGangliaSink.GangliaConfType.tmax);
			LoadGangliaConf(AbstractGangliaSink.GangliaConfType.dmax);
			LoadGangliaConf(AbstractGangliaSink.GangliaConfType.slope);
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
			// see if sparseMetrics is supported. Default is false
			supportSparseMetrics = conf.GetBoolean(SupportSparseMetricsProperty, SupportSparseMetricsDefault
				);
		}

		/*
		* (non-Javadoc)
		*
		* @see org.apache.hadoop.metrics2.MetricsSink#flush()
		*/
		public virtual void Flush()
		{
		}

		// nothing to do as we are not buffering data
		// Load the configurations for a conf type
		private void LoadGangliaConf(AbstractGangliaSink.GangliaConfType gtype)
		{
			string[] propertyarr = conf.GetStringArray(gtype.ToString());
			if (propertyarr != null && propertyarr.Length > 0)
			{
				foreach (string metricNValue in propertyarr)
				{
					string[] metricNValueArr = metricNValue.Split(Equal);
					if (metricNValueArr.Length != 2 || metricNValueArr[0].Length == 0)
					{
						Log.Error("Invalid propertylist for " + gtype.ToString());
					}
					string metricName = metricNValueArr[0].Trim();
					string metricValue = metricNValueArr[1].Trim();
					GangliaConf gconf = gangliaConfMap[metricName];
					if (gconf == null)
					{
						gconf = new GangliaConf();
						gangliaConfMap[metricName] = gconf;
					}
					switch (gtype)
					{
						case AbstractGangliaSink.GangliaConfType.units:
						{
							gconf.SetUnits(metricValue);
							break;
						}

						case AbstractGangliaSink.GangliaConfType.dmax:
						{
							gconf.SetDmax(System.Convert.ToInt32(metricValue));
							break;
						}

						case AbstractGangliaSink.GangliaConfType.tmax:
						{
							gconf.SetTmax(System.Convert.ToInt32(metricValue));
							break;
						}

						case AbstractGangliaSink.GangliaConfType.slope:
						{
							gconf.SetSlope(AbstractGangliaSink.GangliaSlope.ValueOf(metricValue));
							break;
						}
					}
				}
			}
		}

		/// <summary>Lookup GangliaConf from cache.</summary>
		/// <remarks>Lookup GangliaConf from cache. If not found, return default values</remarks>
		/// <param name="metricName"/>
		/// <returns>looked up GangliaConf</returns>
		protected internal virtual GangliaConf GetGangliaConfForMetric(string metricName)
		{
			GangliaConf gconf = gangliaConfMap[metricName];
			return gconf != null ? gconf : DefaultGangliaConf;
		}

		/// <returns>the hostName</returns>
		protected internal virtual string GetHostName()
		{
			return hostName;
		}

		/// <summary>
		/// Puts a string into the buffer by first writing the size of the string as an
		/// int, followed by the bytes of the string, padded if necessary to a multiple
		/// of 4.
		/// </summary>
		/// <param name="s">the string to be written to buffer at offset location</param>
		protected internal virtual void Xdr_string(string s)
		{
			byte[] bytes = Runtime.GetBytesForString(s, Charsets.Utf8);
			int len = bytes.Length;
			Xdr_int(len);
			System.Array.Copy(bytes, 0, buffer, offset, len);
			offset += len;
			Pad();
		}

		// Pads the buffer with zero bytes up to the nearest multiple of 4.
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

		/// <summary>Sends Ganglia Metrics to the configured hosts</summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void EmitToGangliaHosts()
		{
			try
			{
				foreach (EndPoint socketAddress in metricsServers)
				{
					if (socketAddress == null || !(socketAddress is IPEndPoint))
					{
						throw new ArgumentException("Unsupported Address type");
					}
					IPEndPoint inetAddress = (IPEndPoint)socketAddress;
					if (inetAddress.IsUnresolved())
					{
						throw new UnknownHostException("Unresolved host: " + inetAddress);
					}
					DatagramPacket packet = new DatagramPacket(buffer, offset, socketAddress);
					datagramSocket.Send(packet);
				}
			}
			finally
			{
				// reset the buffer for the next metric to be built
				offset = 0;
			}
		}

		/// <summary>Reset the buffer for the next metric to be built</summary>
		internal virtual void ResetBuffer()
		{
			offset = 0;
		}

		/// <returns>whether sparse metrics are supported</returns>
		protected internal virtual bool IsSupportSparseMetrics()
		{
			return supportSparseMetrics;
		}

		/// <summary>Used only by unit test</summary>
		/// <param name="datagramSocket">the datagramSocket to set.</param>
		internal virtual void SetDatagramSocket(DatagramSocket datagramSocket)
		{
			this.datagramSocket = datagramSocket;
		}

		/// <summary>Used only by unit tests</summary>
		/// <returns>the datagramSocket for this sink</returns>
		internal virtual DatagramSocket GetDatagramSocket()
		{
			return datagramSocket;
		}

		public abstract void PutMetrics(MetricsRecord arg1);
	}
}
