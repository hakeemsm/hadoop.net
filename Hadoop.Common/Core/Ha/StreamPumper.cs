using System;
using System.IO;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.HA
{
	/// <summary>
	/// Class responsible for pumping the streams of the subprocess
	/// out to log4j.
	/// </summary>
	/// <remarks>
	/// Class responsible for pumping the streams of the subprocess
	/// out to log4j. stderr is pumped to WARN level and stdout is
	/// pumped to INFO level
	/// </remarks>
	internal class StreamPumper
	{
		internal enum StreamType
		{
			Stdout,
			Stderr
		}

		private readonly Log log;

		internal readonly Sharpen.Thread thread;

		internal readonly string logPrefix;

		internal readonly StreamPumper.StreamType type;

		private readonly InputStream stream;

		private bool started = false;

		internal StreamPumper(Log log, string logPrefix, InputStream stream, StreamPumper.StreamType
			 type)
		{
			this.log = log;
			this.logPrefix = logPrefix;
			this.stream = stream;
			this.type = type;
			thread = new Sharpen.Thread(new _Runnable_53(this, logPrefix, type), logPrefix + 
				": StreamPumper for " + type);
			thread.SetDaemon(true);
		}

		private sealed class _Runnable_53 : Runnable
		{
			public _Runnable_53(StreamPumper _enclosing, string logPrefix, StreamPumper.StreamType
				 type)
			{
				this._enclosing = _enclosing;
				this.logPrefix = logPrefix;
				this.type = type;
			}

			public void Run()
			{
				try
				{
					this._enclosing.Pump();
				}
				catch (Exception t)
				{
					ShellCommandFencer.Log.Warn(logPrefix + ": Unable to pump output from " + type, t
						);
				}
			}

			private readonly StreamPumper _enclosing;

			private readonly string logPrefix;

			private readonly StreamPumper.StreamType type;
		}

		/// <exception cref="System.Exception"/>
		internal virtual void Join()
		{
			System.Diagnostics.Debug.Assert(started);
			thread.Join();
		}

		internal virtual void Start()
		{
			System.Diagnostics.Debug.Assert(!started);
			thread.Start();
			started = true;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void Pump()
		{
			InputStreamReader inputStreamReader = new InputStreamReader(stream, Charsets.Utf8
				);
			BufferedReader br = new BufferedReader(inputStreamReader);
			string line = null;
			while ((line = br.ReadLine()) != null)
			{
				if (type == StreamPumper.StreamType.Stdout)
				{
					log.Info(logPrefix + ": " + line);
				}
				else
				{
					log.Warn(logPrefix + ": " + line);
				}
			}
		}
	}
}
