using Sharpen;

namespace org.apache.hadoop.metrics2.sink.ganglia
{
	/// <summary>This the base class for Ganglia sink classes using metrics2.</summary>
	/// <remarks>
	/// This the base class for Ganglia sink classes using metrics2. Lot of the code
	/// has been derived from org.apache.hadoop.metrics.ganglia.GangliaContext.
	/// As per the documentation, sink implementations doesn't have to worry about
	/// thread safety. Hence the code wasn't written for thread safety and should
	/// be modified in case the above assumption changes in the future.
	/// </remarks>
	public abstract class AbstractGangliaSink : org.apache.hadoop.metrics2.MetricsSink
	{
		public readonly org.apache.commons.logging.Log LOG;

		public const string DEFAULT_UNITS = string.Empty;

		public const int DEFAULT_TMAX = 60;

		public const int DEFAULT_DMAX = 0;

		public static readonly org.apache.hadoop.metrics2.sink.ganglia.AbstractGangliaSink.GangliaSlope
			 DEFAULT_SLOPE = org.apache.hadoop.metrics2.sink.ganglia.AbstractGangliaSink.GangliaSlope
			.both;

		public const int DEFAULT_PORT = 8649;

		public const bool DEFAULT_MULTICAST_ENABLED = false;

		public const int DEFAULT_MULTICAST_TTL = 1;

		public const string SERVERS_PROPERTY = "servers";

		public const string MULTICAST_ENABLED_PROPERTY = "multicast";

		public const string MULTICAST_TTL_PROPERTY = "multicast.ttl";

		public const int BUFFER_SIZE = 1500;

		public const string SUPPORT_SPARSE_METRICS_PROPERTY = "supportsparse";

		public const bool SUPPORT_SPARSE_METRICS_DEFAULT = false;

		public const string EQUAL = "=";

		private string hostName = "UNKNOWN.example.com";

		private java.net.DatagramSocket datagramSocket;

		private System.Collections.Generic.IList<java.net.SocketAddress> metricsServers;

		private bool multicastEnabled;

		private int multicastTtl;

		private byte[] buffer = new byte[BUFFER_SIZE];

		private int offset;

		private bool supportSparseMetrics = SUPPORT_SPARSE_METRICS_DEFAULT;

		/// <summary>Used for visiting Metrics</summary>
		protected internal readonly org.apache.hadoop.metrics2.sink.ganglia.GangliaMetricVisitor
			 gangliaMetricVisitor = new org.apache.hadoop.metrics2.sink.ganglia.GangliaMetricVisitor
			();

		private org.apache.commons.configuration.SubsetConfiguration conf;

		private System.Collections.Generic.IDictionary<string, org.apache.hadoop.metrics2.sink.ganglia.GangliaConf
			> gangliaConfMap;

		private org.apache.hadoop.metrics2.sink.ganglia.GangliaConf DEFAULT_GANGLIA_CONF = 
			new org.apache.hadoop.metrics2.sink.ganglia.GangliaConf();

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
		public virtual void init(org.apache.commons.configuration.SubsetConfiguration conf
			)
		{
			LOG.debug("Initializing the GangliaSink for Ganglia metrics.");
			this.conf = conf;
			// Take the hostname from the DNS class.
			if (conf.getString("slave.host.name") != null)
			{
				hostName = conf.getString("slave.host.name");
			}
			else
			{
				try
				{
					hostName = org.apache.hadoop.net.DNS.getDefaultHost(conf.getString("dfs.datanode.dns.interface"
						, "default"), conf.getString("dfs.datanode.dns.nameserver", "default"));
				}
				catch (java.net.UnknownHostException uhe)
				{
					LOG.error(uhe);
					hostName = "UNKNOWN.example.com";
				}
			}
			// load the gannglia servers from properties
			metricsServers = org.apache.hadoop.metrics2.util.Servers.parse(conf.getString(SERVERS_PROPERTY
				), DEFAULT_PORT);
			multicastEnabled = conf.getBoolean(MULTICAST_ENABLED_PROPERTY, DEFAULT_MULTICAST_ENABLED
				);
			multicastTtl = conf.getInt(MULTICAST_TTL_PROPERTY, DEFAULT_MULTICAST_TTL);
			// extract the Ganglia conf per metrics
			gangliaConfMap = new System.Collections.Generic.Dictionary<string, org.apache.hadoop.metrics2.sink.ganglia.GangliaConf
				>();
			loadGangliaConf(org.apache.hadoop.metrics2.sink.ganglia.AbstractGangliaSink.GangliaConfType
				.units);
			loadGangliaConf(org.apache.hadoop.metrics2.sink.ganglia.AbstractGangliaSink.GangliaConfType
				.tmax);
			loadGangliaConf(org.apache.hadoop.metrics2.sink.ganglia.AbstractGangliaSink.GangliaConfType
				.dmax);
			loadGangliaConf(org.apache.hadoop.metrics2.sink.ganglia.AbstractGangliaSink.GangliaConfType
				.slope);
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
			// see if sparseMetrics is supported. Default is false
			supportSparseMetrics = conf.getBoolean(SUPPORT_SPARSE_METRICS_PROPERTY, SUPPORT_SPARSE_METRICS_DEFAULT
				);
		}

		/*
		* (non-Javadoc)
		*
		* @see org.apache.hadoop.metrics2.MetricsSink#flush()
		*/
		public virtual void flush()
		{
		}

		// nothing to do as we are not buffering data
		// Load the configurations for a conf type
		private void loadGangliaConf(org.apache.hadoop.metrics2.sink.ganglia.AbstractGangliaSink.GangliaConfType
			 gtype)
		{
			string[] propertyarr = conf.getStringArray(gtype.ToString());
			if (propertyarr != null && propertyarr.Length > 0)
			{
				foreach (string metricNValue in propertyarr)
				{
					string[] metricNValueArr = metricNValue.split(EQUAL);
					if (metricNValueArr.Length != 2 || metricNValueArr[0].Length == 0)
					{
						LOG.error("Invalid propertylist for " + gtype.ToString());
					}
					string metricName = metricNValueArr[0].Trim();
					string metricValue = metricNValueArr[1].Trim();
					org.apache.hadoop.metrics2.sink.ganglia.GangliaConf gconf = gangliaConfMap[metricName
						];
					if (gconf == null)
					{
						gconf = new org.apache.hadoop.metrics2.sink.ganglia.GangliaConf();
						gangliaConfMap[metricName] = gconf;
					}
					switch (gtype)
					{
						case org.apache.hadoop.metrics2.sink.ganglia.AbstractGangliaSink.GangliaConfType.
							units:
						{
							gconf.setUnits(metricValue);
							break;
						}

						case org.apache.hadoop.metrics2.sink.ganglia.AbstractGangliaSink.GangliaConfType.
							dmax:
						{
							gconf.setDmax(System.Convert.ToInt32(metricValue));
							break;
						}

						case org.apache.hadoop.metrics2.sink.ganglia.AbstractGangliaSink.GangliaConfType.
							tmax:
						{
							gconf.setTmax(System.Convert.ToInt32(metricValue));
							break;
						}

						case org.apache.hadoop.metrics2.sink.ganglia.AbstractGangliaSink.GangliaConfType.
							slope:
						{
							gconf.setSlope(org.apache.hadoop.metrics2.sink.ganglia.AbstractGangliaSink.GangliaSlope
								.valueOf(metricValue));
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
		protected internal virtual org.apache.hadoop.metrics2.sink.ganglia.GangliaConf getGangliaConfForMetric
			(string metricName)
		{
			org.apache.hadoop.metrics2.sink.ganglia.GangliaConf gconf = gangliaConfMap[metricName
				];
			return gconf != null ? gconf : DEFAULT_GANGLIA_CONF;
		}

		/// <returns>the hostName</returns>
		protected internal virtual string getHostName()
		{
			return hostName;
		}

		/// <summary>
		/// Puts a string into the buffer by first writing the size of the string as an
		/// int, followed by the bytes of the string, padded if necessary to a multiple
		/// of 4.
		/// </summary>
		/// <param name="s">the string to be written to buffer at offset location</param>
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

		// Pads the buffer with zero bytes up to the nearest multiple of 4.
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

		/// <summary>Sends Ganglia Metrics to the configured hosts</summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void emitToGangliaHosts()
		{
			try
			{
				foreach (java.net.SocketAddress socketAddress in metricsServers)
				{
					if (socketAddress == null || !(socketAddress is java.net.InetSocketAddress))
					{
						throw new System.ArgumentException("Unsupported Address type");
					}
					java.net.InetSocketAddress inetAddress = (java.net.InetSocketAddress)socketAddress;
					if (inetAddress.isUnresolved())
					{
						throw new java.net.UnknownHostException("Unresolved host: " + inetAddress);
					}
					java.net.DatagramPacket packet = new java.net.DatagramPacket(buffer, offset, socketAddress
						);
					datagramSocket.send(packet);
				}
			}
			finally
			{
				// reset the buffer for the next metric to be built
				offset = 0;
			}
		}

		/// <summary>Reset the buffer for the next metric to be built</summary>
		internal virtual void resetBuffer()
		{
			offset = 0;
		}

		/// <returns>whether sparse metrics are supported</returns>
		protected internal virtual bool isSupportSparseMetrics()
		{
			return supportSparseMetrics;
		}

		/// <summary>Used only by unit test</summary>
		/// <param name="datagramSocket">the datagramSocket to set.</param>
		internal virtual void setDatagramSocket(java.net.DatagramSocket datagramSocket)
		{
			this.datagramSocket = datagramSocket;
		}

		/// <summary>Used only by unit tests</summary>
		/// <returns>the datagramSocket for this sink</returns>
		internal virtual java.net.DatagramSocket getDatagramSocket()
		{
			return datagramSocket;
		}

		public abstract void putMetrics(org.apache.hadoop.metrics2.MetricsRecord arg1);

		public AbstractGangliaSink()
		{
			LOG = org.apache.commons.logging.LogFactory.getLog(Sharpen.Runtime.getClassForObject
				(this));
		}
	}
}
