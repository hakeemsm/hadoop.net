using Sharpen;

namespace org.apache.hadoop.ha
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
			STDOUT,
			STDERR
		}

		private readonly org.apache.commons.logging.Log log;

		internal readonly java.lang.Thread thread;

		internal readonly string logPrefix;

		internal readonly org.apache.hadoop.ha.StreamPumper.StreamType type;

		private readonly java.io.InputStream stream;

		private bool started = false;

		internal StreamPumper(org.apache.commons.logging.Log log, string logPrefix, java.io.InputStream
			 stream, org.apache.hadoop.ha.StreamPumper.StreamType type)
		{
			this.log = log;
			this.logPrefix = logPrefix;
			this.stream = stream;
			this.type = type;
			thread = new java.lang.Thread(new _Runnable_53(this, logPrefix, type), logPrefix 
				+ ": StreamPumper for " + type);
			thread.setDaemon(true);
		}

		private sealed class _Runnable_53 : java.lang.Runnable
		{
			public _Runnable_53(StreamPumper _enclosing, string logPrefix, org.apache.hadoop.ha.StreamPumper.StreamType
				 type)
			{
				this._enclosing = _enclosing;
				this.logPrefix = logPrefix;
				this.type = type;
			}

			public void run()
			{
				try
				{
					this._enclosing.pump();
				}
				catch (System.Exception t)
				{
					org.apache.hadoop.ha.ShellCommandFencer.LOG.warn(logPrefix + ": Unable to pump output from "
						 + type, t);
				}
			}

			private readonly StreamPumper _enclosing;

			private readonly string logPrefix;

			private readonly org.apache.hadoop.ha.StreamPumper.StreamType type;
		}

		/// <exception cref="System.Exception"/>
		internal virtual void join()
		{
			System.Diagnostics.Debug.Assert(started);
			thread.join();
		}

		internal virtual void start()
		{
			System.Diagnostics.Debug.Assert(!started);
			thread.start();
			started = true;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void pump()
		{
			java.io.InputStreamReader inputStreamReader = new java.io.InputStreamReader(stream
				, org.apache.commons.io.Charsets.UTF_8);
			java.io.BufferedReader br = new java.io.BufferedReader(inputStreamReader);
			string line = null;
			while ((line = br.readLine()) != null)
			{
				if (type == org.apache.hadoop.ha.StreamPumper.StreamType.STDOUT)
				{
					log.info(logPrefix + ": " + line);
				}
				else
				{
					log.warn(logPrefix + ": " + line);
				}
			}
		}
	}
}
