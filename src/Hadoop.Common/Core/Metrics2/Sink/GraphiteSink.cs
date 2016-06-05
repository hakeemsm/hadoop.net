using System;
using System.IO;
using System.Net.Sockets;
using System.Text;
using Org.Apache.Commons.Configuration;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Metrics2;


namespace Org.Apache.Hadoop.Metrics2.Sink
{
	/// <summary>A metrics sink that writes to a Graphite server</summary>
	public class GraphiteSink : MetricsSink, IDisposable
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(GraphiteSink));

		private const string ServerHostKey = "server_host";

		private const string ServerPortKey = "server_port";

		private const string MetricsPrefix = "metrics_prefix";

		private string metricsPrefix = null;

		private GraphiteSink.Graphite graphite = null;

		public virtual void Init(SubsetConfiguration conf)
		{
			// Get Graphite host configurations.
			string serverHost = conf.GetString(ServerHostKey);
			int serverPort = System.Convert.ToInt32(conf.GetString(ServerPortKey));
			// Get Graphite metrics graph prefix.
			metricsPrefix = conf.GetString(MetricsPrefix);
			if (metricsPrefix == null)
			{
				metricsPrefix = string.Empty;
			}
			graphite = new GraphiteSink.Graphite(serverHost, serverPort);
			graphite.Connect();
		}

		public virtual void PutMetrics(MetricsRecord record)
		{
			StringBuilder lines = new StringBuilder();
			StringBuilder metricsPathPrefix = new StringBuilder();
			// Configure the hierarchical place to display the graph.
			metricsPathPrefix.Append(metricsPrefix).Append(".").Append(record.Context()).Append
				(".").Append(record.Name());
			foreach (MetricsTag tag in record.Tags())
			{
				if (tag.Value() != null)
				{
					metricsPathPrefix.Append(".");
					metricsPathPrefix.Append(tag.Name());
					metricsPathPrefix.Append("=");
					metricsPathPrefix.Append(tag.Value());
				}
			}
			// The record timestamp is in milliseconds while Graphite expects an epoc time in seconds.
			long timestamp = record.Timestamp() / 1000L;
			// Collect datapoints.
			foreach (AbstractMetric metric in record.Metrics())
			{
				lines.Append(metricsPathPrefix.ToString() + "." + metric.Name().Replace(' ', '.')
					).Append(" ").Append(metric.Value()).Append(" ").Append(timestamp).Append("\n");
			}
			try
			{
				graphite.Write(lines.ToString());
			}
			catch (Exception e)
			{
				Log.Warn("Error sending metrics to Graphite", e);
				try
				{
					graphite.Close();
				}
				catch (Exception e1)
				{
					throw new MetricsException("Error closing connection to Graphite", e1);
				}
			}
		}

		public virtual void Flush()
		{
			try
			{
				graphite.Flush();
			}
			catch (Exception e)
			{
				Log.Warn("Error flushing metrics to Graphite", e);
				try
				{
					graphite.Close();
				}
				catch (Exception e1)
				{
					throw new MetricsException("Error closing connection to Graphite", e1);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			graphite.Close();
		}

		public class Graphite
		{
			private const int MaxConnectionFailures = 5;

			private string serverHost;

			private int serverPort;

			private TextWriter writer = null;

			private Socket socket = null;

			private int connectionFailures = 0;

			public Graphite(string serverHost, int serverPort)
			{
				this.serverHost = serverHost;
				this.serverPort = serverPort;
			}

			public virtual void Connect()
			{
				if (IsConnected())
				{
					throw new MetricsException("Already connected to Graphite");
				}
				if (TooManyConnectionFailures())
				{
					// return silently (there was ERROR in logs when we reached limit for the first time)
					return;
				}
				try
				{
					// Open a connection to Graphite server.
					socket = Extensions.CreateSocket(serverHost, serverPort);
					writer = new OutputStreamWriter(socket.GetOutputStream(), Charsets.Utf8);
				}
				catch (Exception e)
				{
					connectionFailures++;
					if (TooManyConnectionFailures())
					{
						// first time when connection limit reached, report to logs
						Log.Error("Too many connection failures, would not try to connect again.");
					}
					throw new MetricsException("Error creating connection, " + serverHost + ":" + serverPort
						, e);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(string msg)
			{
				if (!IsConnected())
				{
					Connect();
				}
				if (IsConnected())
				{
					writer.Write(msg);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Flush()
			{
				if (IsConnected())
				{
					writer.Flush();
				}
			}

			public virtual bool IsConnected()
			{
				return socket != null && socket.Connected && !socket.IsClosed();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				try
				{
					if (writer != null)
					{
						writer.Close();
					}
				}
				catch (IOException)
				{
					if (socket != null)
					{
						socket.Close();
					}
				}
				finally
				{
					socket = null;
					writer = null;
				}
			}

			private bool TooManyConnectionFailures()
			{
				return connectionFailures > MaxConnectionFailures;
			}
		}
	}
}
