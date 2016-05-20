using Sharpen;

namespace org.apache.hadoop.tracing
{
	/// <summary>
	/// This class provides functions for reading the names of SpanReceivers from
	/// the Hadoop configuration, adding those SpanReceivers to the Tracer,
	/// and closing those SpanReceivers when appropriate.
	/// </summary>
	/// <remarks>
	/// This class provides functions for reading the names of SpanReceivers from
	/// the Hadoop configuration, adding those SpanReceivers to the Tracer,
	/// and closing those SpanReceivers when appropriate.
	/// This class does nothing If no SpanReceiver is configured.
	/// </remarks>
	public class SpanReceiverHost : org.apache.hadoop.tracing.TraceAdminProtocol
	{
		public const string SPAN_RECEIVERS_CONF_SUFFIX = "spanreceiver.classes";

		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.tracing.SpanReceiverHost
			)));

		private static readonly System.Collections.Generic.Dictionary<string, org.apache.hadoop.tracing.SpanReceiverHost
			> hosts = new System.Collections.Generic.Dictionary<string, org.apache.hadoop.tracing.SpanReceiverHost
			>(1);

		private readonly System.Collections.Generic.SortedDictionary<long, org.apache.htrace.SpanReceiver
			> receivers = new System.Collections.Generic.SortedDictionary<long, org.apache.htrace.SpanReceiver
			>();

		private readonly string confPrefix;

		private org.apache.hadoop.conf.Configuration config;

		private bool closed = false;

		private long highestId = 1;

		private const string LOCAL_FILE_SPAN_RECEIVER_PATH_SUFFIX = "local-file-span-receiver.path";

		public static org.apache.hadoop.tracing.SpanReceiverHost get(org.apache.hadoop.conf.Configuration
			 conf, string confPrefix)
		{
			lock (Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.tracing.SpanReceiverHost
				)))
			{
				org.apache.hadoop.tracing.SpanReceiverHost host = hosts[confPrefix];
				if (host != null)
				{
					return host;
				}
				org.apache.hadoop.tracing.SpanReceiverHost newHost = new org.apache.hadoop.tracing.SpanReceiverHost
					(confPrefix);
				newHost.loadSpanReceivers(conf);
				org.apache.hadoop.util.ShutdownHookManager.get().addShutdownHook(new _Runnable_79
					(newHost), 0);
				hosts[confPrefix] = newHost;
				return newHost;
			}
		}

		private sealed class _Runnable_79 : java.lang.Runnable
		{
			public _Runnable_79(org.apache.hadoop.tracing.SpanReceiverHost newHost)
			{
				this.newHost = newHost;
			}

			public void run()
			{
				newHost.closeReceivers();
			}

			private readonly org.apache.hadoop.tracing.SpanReceiverHost newHost;
		}

		private static System.Collections.Generic.IList<org.apache.hadoop.tracing.SpanReceiverInfo.ConfigurationPair
			> EMPTY = java.util.Collections.emptyList();

		private static string getUniqueLocalTraceFileName()
		{
			string tmp = Sharpen.Runtime.getProperty("java.io.tmpdir", "/tmp");
			string nonce = null;
			java.io.BufferedReader reader = null;
			try
			{
				// On Linux we can get a unique local file name by reading the process id
				// out of /proc/self/stat.  (There isn't any portable way to get the
				// process ID from Java.)
				reader = new java.io.BufferedReader(new java.io.InputStreamReader(new java.io.FileInputStream
					("/proc/self/stat"), org.apache.commons.io.Charsets.UTF_8));
				string line = reader.readLine();
				if (line == null)
				{
					throw new java.io.EOFException();
				}
				nonce = line.split(" ")[0];
			}
			catch (System.IO.IOException)
			{
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.cleanup(LOG, reader);
			}
			if (nonce == null)
			{
				// If we can't use the process ID, use a random nonce.
				nonce = java.util.UUID.randomUUID().ToString();
			}
			return new java.io.File(tmp, nonce).getAbsolutePath();
		}

		private SpanReceiverHost(string confPrefix)
		{
			this.confPrefix = confPrefix;
		}

		/// <summary>
		/// Reads the names of classes specified in the
		/// "hadoop.htrace.spanreceiver.classes" property and instantiates and registers
		/// them with the Tracer as SpanReceiver's.
		/// </summary>
		/// <remarks>
		/// Reads the names of classes specified in the
		/// "hadoop.htrace.spanreceiver.classes" property and instantiates and registers
		/// them with the Tracer as SpanReceiver's.
		/// The nullary constructor is called during construction, but if the classes
		/// specified implement the Configurable interface, setConfiguration() will be
		/// called on them. This allows SpanReceivers to use values from the Hadoop
		/// configuration.
		/// </remarks>
		public virtual void loadSpanReceivers(org.apache.hadoop.conf.Configuration conf)
		{
			lock (this)
			{
				config = new org.apache.hadoop.conf.Configuration(conf);
				string receiverKey = confPrefix + SPAN_RECEIVERS_CONF_SUFFIX;
				string[] receiverNames = config.getTrimmedStrings(receiverKey);
				if (receiverNames == null || receiverNames.Length == 0)
				{
					if (LOG.isTraceEnabled())
					{
						LOG.trace("No span receiver names found in " + receiverKey + ".");
					}
					return;
				}
				// It's convenient to have each daemon log to a random trace file when
				// testing.
				string pathKey = confPrefix + LOCAL_FILE_SPAN_RECEIVER_PATH_SUFFIX;
				if (config.get(pathKey) == null)
				{
					string uniqueFile = getUniqueLocalTraceFileName();
					config.set(pathKey, uniqueFile);
					if (LOG.isTraceEnabled())
					{
						LOG.trace("Set " + pathKey + " to " + uniqueFile);
					}
				}
				foreach (string className in receiverNames)
				{
					try
					{
						org.apache.htrace.SpanReceiver rcvr = loadInstance(className, EMPTY);
						org.apache.htrace.Trace.addReceiver(rcvr);
						receivers[highestId++] = rcvr;
						LOG.info("Loaded SpanReceiver " + className + " successfully.");
					}
					catch (System.IO.IOException e)
					{
						LOG.error("Failed to load SpanReceiver", e);
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private org.apache.htrace.SpanReceiver loadInstance(string className, System.Collections.Generic.IList
			<org.apache.hadoop.tracing.SpanReceiverInfo.ConfigurationPair> extraConfig)
		{
			lock (this)
			{
				org.apache.htrace.SpanReceiverBuilder builder = new org.apache.htrace.SpanReceiverBuilder
					(org.apache.hadoop.tracing.TraceUtils.wrapHadoopConf(confPrefix, config, extraConfig
					));
				org.apache.htrace.SpanReceiver rcvr = builder.spanReceiverClass(className.Trim())
					.build();
				if (rcvr == null)
				{
					throw new System.IO.IOException("Failed to load SpanReceiver " + className);
				}
				return rcvr;
			}
		}

		/// <summary>Calls close() on all SpanReceivers created by this SpanReceiverHost.</summary>
		public virtual void closeReceivers()
		{
			lock (this)
			{
				if (closed)
				{
					return;
				}
				closed = true;
				foreach (org.apache.htrace.SpanReceiver rcvr in receivers.Values)
				{
					try
					{
						rcvr.close();
					}
					catch (System.IO.IOException e)
					{
						LOG.warn("Unable to close SpanReceiver correctly: " + e.Message, e);
					}
				}
				receivers.clear();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.tracing.SpanReceiverInfo[] listSpanReceivers()
		{
			lock (this)
			{
				org.apache.hadoop.tracing.SpanReceiverInfo[] info = new org.apache.hadoop.tracing.SpanReceiverInfo
					[receivers.Count];
				int i = 0;
				foreach (System.Collections.Generic.KeyValuePair<long, org.apache.htrace.SpanReceiver
					> entry in receivers)
				{
					info[i] = new org.apache.hadoop.tracing.SpanReceiverInfo(entry.Key, Sharpen.Runtime.getClassForObject
						(entry.Value).getName());
					i++;
				}
				return info;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override long addSpanReceiver(org.apache.hadoop.tracing.SpanReceiverInfo info
			)
		{
			lock (this)
			{
				java.lang.StringBuilder configStringBuilder = new java.lang.StringBuilder();
				string prefix = string.Empty;
				foreach (org.apache.hadoop.tracing.SpanReceiverInfo.ConfigurationPair pair in info
					.configPairs)
				{
					configStringBuilder.Append(prefix).Append(pair.getKey()).Append(" = ").Append(pair
						.getValue());
					prefix = ", ";
				}
				org.apache.htrace.SpanReceiver rcvr = null;
				try
				{
					rcvr = loadInstance(info.getClassName(), info.configPairs);
				}
				catch (System.IO.IOException e)
				{
					LOG.info("Failed to add SpanReceiver " + info.getClassName() + " with configuration "
						 + configStringBuilder.ToString(), e);
					throw;
				}
				catch (System.Exception e)
				{
					LOG.info("Failed to add SpanReceiver " + info.getClassName() + " with configuration "
						 + configStringBuilder.ToString(), e);
					throw;
				}
				org.apache.htrace.Trace.addReceiver(rcvr);
				long newId = highestId++;
				receivers[newId] = rcvr;
				LOG.info("Successfully added SpanReceiver " + info.getClassName() + " with configuration "
					 + configStringBuilder.ToString());
				return newId;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void removeSpanReceiver(long spanReceiverId)
		{
			lock (this)
			{
				org.apache.htrace.SpanReceiver rcvr = Sharpen.Collections.Remove(receivers, spanReceiverId
					);
				if (rcvr == null)
				{
					throw new System.IO.IOException("There is no span receiver with id " + spanReceiverId
						);
				}
				org.apache.htrace.Trace.removeReceiver(rcvr);
				rcvr.close();
				LOG.info("Successfully removed SpanReceiver " + spanReceiverId + " with class " +
					 Sharpen.Runtime.getClassForObject(rcvr).getName());
			}
		}
	}
}
