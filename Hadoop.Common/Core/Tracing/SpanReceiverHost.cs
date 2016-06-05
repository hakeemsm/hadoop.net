using System.Collections.Generic;
using System.IO;
using System.Text;
using Hadoop.Common.Core.Conf;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Org.Apache.Htrace;


namespace Org.Apache.Hadoop.Tracing
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
	public class SpanReceiverHost : TraceAdminProtocol
	{
		public const string SpanReceiversConfSuffix = "spanreceiver.classes";

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Tracing.SpanReceiverHost
			));

		private static readonly Dictionary<string, Org.Apache.Hadoop.Tracing.SpanReceiverHost
			> hosts = new Dictionary<string, Org.Apache.Hadoop.Tracing.SpanReceiverHost>(1);

		private readonly SortedDictionary<long, SpanReceiver> receivers = new SortedDictionary
			<long, SpanReceiver>();

		private readonly string confPrefix;

		private Configuration config;

		private bool closed = false;

		private long highestId = 1;

		private const string LocalFileSpanReceiverPathSuffix = "local-file-span-receiver.path";

		public static Org.Apache.Hadoop.Tracing.SpanReceiverHost Get(Configuration conf, 
			string confPrefix)
		{
			lock (typeof(Org.Apache.Hadoop.Tracing.SpanReceiverHost))
			{
				Org.Apache.Hadoop.Tracing.SpanReceiverHost host = hosts[confPrefix];
				if (host != null)
				{
					return host;
				}
				Org.Apache.Hadoop.Tracing.SpanReceiverHost newHost = new Org.Apache.Hadoop.Tracing.SpanReceiverHost
					(confPrefix);
				newHost.LoadSpanReceivers(conf);
				ShutdownHookManager.Get().AddShutdownHook(new _Runnable_79(newHost), 0);
				hosts[confPrefix] = newHost;
				return newHost;
			}
		}

		private sealed class _Runnable_79 : Runnable
		{
			public _Runnable_79(Org.Apache.Hadoop.Tracing.SpanReceiverHost newHost)
			{
				this.newHost = newHost;
			}

			public void Run()
			{
				newHost.CloseReceivers();
			}

			private readonly Org.Apache.Hadoop.Tracing.SpanReceiverHost newHost;
		}

		private static IList<SpanReceiverInfo.ConfigurationPair> Empty = Collections
			.EmptyList();

		private static string GetUniqueLocalTraceFileName()
		{
			string tmp = Runtime.GetProperty("java.io.tmpdir", "/tmp");
			string nonce = null;
			BufferedReader reader = null;
			try
			{
				// On Linux we can get a unique local file name by reading the process id
				// out of /proc/self/stat.  (There isn't any portable way to get the
				// process ID from Java.)
				reader = new BufferedReader(new InputStreamReader(new FileInputStream("/proc/self/stat"
					), Charsets.Utf8));
				string line = reader.ReadLine();
				if (line == null)
				{
					throw new EOFException();
				}
				nonce = line.Split(" ")[0];
			}
			catch (IOException)
			{
			}
			finally
			{
				IOUtils.Cleanup(Log, reader);
			}
			if (nonce == null)
			{
				// If we can't use the process ID, use a random nonce.
				nonce = UUID.RandomUUID().ToString();
			}
			return new FilePath(tmp, nonce).GetAbsolutePath();
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
		public virtual void LoadSpanReceivers(Configuration conf)
		{
			lock (this)
			{
				config = new Configuration(conf);
				string receiverKey = confPrefix + SpanReceiversConfSuffix;
				string[] receiverNames = config.GetTrimmedStrings(receiverKey);
				if (receiverNames == null || receiverNames.Length == 0)
				{
					if (Log.IsTraceEnabled())
					{
						Log.Trace("No span receiver names found in " + receiverKey + ".");
					}
					return;
				}
				// It's convenient to have each daemon log to a random trace file when
				// testing.
				string pathKey = confPrefix + LocalFileSpanReceiverPathSuffix;
				if (config.Get(pathKey) == null)
				{
					string uniqueFile = GetUniqueLocalTraceFileName();
					config.Set(pathKey, uniqueFile);
					if (Log.IsTraceEnabled())
					{
						Log.Trace("Set " + pathKey + " to " + uniqueFile);
					}
				}
				foreach (string className in receiverNames)
				{
					try
					{
						SpanReceiver rcvr = LoadInstance(className, Empty);
						Trace.AddReceiver(rcvr);
						receivers[highestId++] = rcvr;
						Log.Info("Loaded SpanReceiver " + className + " successfully.");
					}
					catch (IOException e)
					{
						Log.Error("Failed to load SpanReceiver", e);
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private SpanReceiver LoadInstance(string className, IList<SpanReceiverInfo.ConfigurationPair
			> extraConfig)
		{
			lock (this)
			{
				SpanReceiverBuilder builder = new SpanReceiverBuilder(TraceUtils.WrapHadoopConf(confPrefix
					, config, extraConfig));
				SpanReceiver rcvr = builder.SpanReceiverClass(className.Trim()).Build();
				if (rcvr == null)
				{
					throw new IOException("Failed to load SpanReceiver " + className);
				}
				return rcvr;
			}
		}

		/// <summary>Calls close() on all SpanReceivers created by this SpanReceiverHost.</summary>
		public virtual void CloseReceivers()
		{
			lock (this)
			{
				if (closed)
				{
					return;
				}
				closed = true;
				foreach (SpanReceiver rcvr in receivers.Values)
				{
					try
					{
						rcvr.Close();
					}
					catch (IOException e)
					{
						Log.Warn("Unable to close SpanReceiver correctly: " + e.Message, e);
					}
				}
				receivers.Clear();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override SpanReceiverInfo[] ListSpanReceivers()
		{
			lock (this)
			{
				SpanReceiverInfo[] info = new SpanReceiverInfo[receivers.Count];
				int i = 0;
				foreach (KeyValuePair<long, SpanReceiver> entry in receivers)
				{
					info[i] = new SpanReceiverInfo(entry.Key, entry.Value.GetType().FullName);
					i++;
				}
				return info;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override long AddSpanReceiver(SpanReceiverInfo info)
		{
			lock (this)
			{
				StringBuilder configStringBuilder = new StringBuilder();
				string prefix = string.Empty;
				foreach (SpanReceiverInfo.ConfigurationPair pair in info.configPairs)
				{
					configStringBuilder.Append(prefix).Append(pair.GetKey()).Append(" = ").Append(pair
						.GetValue());
					prefix = ", ";
				}
				SpanReceiver rcvr = null;
				try
				{
					rcvr = LoadInstance(info.GetClassName(), info.configPairs);
				}
				catch (IOException e)
				{
					Log.Info("Failed to add SpanReceiver " + info.GetClassName() + " with configuration "
						 + configStringBuilder.ToString(), e);
					throw;
				}
				catch (RuntimeException e)
				{
					Log.Info("Failed to add SpanReceiver " + info.GetClassName() + " with configuration "
						 + configStringBuilder.ToString(), e);
					throw;
				}
				Trace.AddReceiver(rcvr);
				long newId = highestId++;
				receivers[newId] = rcvr;
				Log.Info("Successfully added SpanReceiver " + info.GetClassName() + " with configuration "
					 + configStringBuilder.ToString());
				return newId;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveSpanReceiver(long spanReceiverId)
		{
			lock (this)
			{
				SpanReceiver rcvr = Collections.Remove(receivers, spanReceiverId);
				if (rcvr == null)
				{
					throw new IOException("There is no span receiver with id " + spanReceiverId);
				}
				Trace.RemoveReceiver(rcvr);
				rcvr.Close();
				Log.Info("Successfully removed SpanReceiver " + spanReceiverId + " with class " +
					 rcvr.GetType().FullName);
			}
		}
	}
}
