using Sharpen;

namespace org.apache.hadoop.metrics2.sink
{
	/// <summary>A metrics sink that writes to a Graphite server</summary>
	public class GraphiteSink : org.apache.hadoop.metrics2.MetricsSink, java.io.Closeable
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.metrics2.sink.GraphiteSink
			)));

		private const string SERVER_HOST_KEY = "server_host";

		private const string SERVER_PORT_KEY = "server_port";

		private const string METRICS_PREFIX = "metrics_prefix";

		private string metricsPrefix = null;

		private org.apache.hadoop.metrics2.sink.GraphiteSink.Graphite graphite = null;

		public virtual void init(org.apache.commons.configuration.SubsetConfiguration conf
			)
		{
			// Get Graphite host configurations.
			string serverHost = conf.getString(SERVER_HOST_KEY);
			int serverPort = System.Convert.ToInt32(conf.getString(SERVER_PORT_KEY));
			// Get Graphite metrics graph prefix.
			metricsPrefix = conf.getString(METRICS_PREFIX);
			if (metricsPrefix == null)
			{
				metricsPrefix = string.Empty;
			}
			graphite = new org.apache.hadoop.metrics2.sink.GraphiteSink.Graphite(serverHost, 
				serverPort);
			graphite.connect();
		}

		public virtual void putMetrics(org.apache.hadoop.metrics2.MetricsRecord record)
		{
			java.lang.StringBuilder lines = new java.lang.StringBuilder();
			java.lang.StringBuilder metricsPathPrefix = new java.lang.StringBuilder();
			// Configure the hierarchical place to display the graph.
			metricsPathPrefix.Append(metricsPrefix).Append(".").Append(record.context()).Append
				(".").Append(record.name());
			foreach (org.apache.hadoop.metrics2.MetricsTag tag in record.tags())
			{
				if (tag.value() != null)
				{
					metricsPathPrefix.Append(".");
					metricsPathPrefix.Append(tag.name());
					metricsPathPrefix.Append("=");
					metricsPathPrefix.Append(tag.value());
				}
			}
			// The record timestamp is in milliseconds while Graphite expects an epoc time in seconds.
			long timestamp = record.timestamp() / 1000L;
			// Collect datapoints.
			foreach (org.apache.hadoop.metrics2.AbstractMetric metric in record.metrics())
			{
				lines.Append(metricsPathPrefix.ToString() + "." + metric.name().Replace(' ', '.')
					).Append(" ").Append(metric.value()).Append(" ").Append(timestamp).Append("\n");
			}
			try
			{
				graphite.write(lines.ToString());
			}
			catch (System.Exception e)
			{
				LOG.warn("Error sending metrics to Graphite", e);
				try
				{
					graphite.close();
				}
				catch (System.Exception e1)
				{
					throw new org.apache.hadoop.metrics2.MetricsException("Error closing connection to Graphite"
						, e1);
				}
			}
		}

		public virtual void flush()
		{
			try
			{
				graphite.flush();
			}
			catch (System.Exception e)
			{
				LOG.warn("Error flushing metrics to Graphite", e);
				try
				{
					graphite.close();
				}
				catch (System.Exception e1)
				{
					throw new org.apache.hadoop.metrics2.MetricsException("Error closing connection to Graphite"
						, e1);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void close()
		{
			graphite.close();
		}

		public class Graphite
		{
			private const int MAX_CONNECTION_FAILURES = 5;

			private string serverHost;

			private int serverPort;

			private System.IO.TextWriter writer = null;

			private java.net.Socket socket = null;

			private int connectionFailures = 0;

			public Graphite(string serverHost, int serverPort)
			{
				this.serverHost = serverHost;
				this.serverPort = serverPort;
			}

			public virtual void connect()
			{
				if (isConnected())
				{
					throw new org.apache.hadoop.metrics2.MetricsException("Already connected to Graphite"
						);
				}
				if (tooManyConnectionFailures())
				{
					// return silently (there was ERROR in logs when we reached limit for the first time)
					return;
				}
				try
				{
					// Open a connection to Graphite server.
					socket = new java.net.Socket(serverHost, serverPort);
					writer = new java.io.OutputStreamWriter(socket.getOutputStream(), org.apache.commons.io.Charsets
						.UTF_8);
				}
				catch (System.Exception e)
				{
					connectionFailures++;
					if (tooManyConnectionFailures())
					{
						// first time when connection limit reached, report to logs
						LOG.error("Too many connection failures, would not try to connect again.");
					}
					throw new org.apache.hadoop.metrics2.MetricsException("Error creating connection, "
						 + serverHost + ":" + serverPort, e);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void write(string msg)
			{
				if (!isConnected())
				{
					connect();
				}
				if (isConnected())
				{
					writer.write(msg);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void flush()
			{
				if (isConnected())
				{
					writer.flush();
				}
			}

			public virtual bool isConnected()
			{
				return socket != null && socket.isConnected() && !socket.isClosed();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void close()
			{
				try
				{
					if (writer != null)
					{
						writer.close();
					}
				}
				catch (System.IO.IOException)
				{
					if (socket != null)
					{
						socket.close();
					}
				}
				finally
				{
					socket = null;
					writer = null;
				}
			}

			private bool tooManyConnectionFailures()
			{
				return connectionFailures > MAX_CONNECTION_FAILURES;
			}
		}
	}
}
