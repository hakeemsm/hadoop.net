using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>Filesystem disk space usage statistics.</summary>
	/// <remarks>Filesystem disk space usage statistics.  Uses the unix 'du' program</remarks>
	public class DU : org.apache.hadoop.util.Shell
	{
		private string dirPath;

		private java.util.concurrent.atomic.AtomicLong used = new java.util.concurrent.atomic.AtomicLong
			();

		private volatile bool shouldRun = true;

		private java.lang.Thread refreshUsed;

		private System.IO.IOException duException = null;

		private long refreshInterval;

		/// <summary>Keeps track of disk usage.</summary>
		/// <param name="path">the path to check disk usage in</param>
		/// <param name="interval">refresh the disk usage at this interval</param>
		/// <exception cref="System.IO.IOException">if we fail to refresh the disk usage</exception>
		public DU(java.io.File path, long interval)
			: this(path, interval, -1L)
		{
		}

		/// <summary>Keeps track of disk usage.</summary>
		/// <param name="path">the path to check disk usage in</param>
		/// <param name="interval">refresh the disk usage at this interval</param>
		/// <param name="initialUsed">use this value until next refresh</param>
		/// <exception cref="System.IO.IOException">if we fail to refresh the disk usage</exception>
		public DU(java.io.File path, long interval, long initialUsed)
			: base(0)
		{
			//we set the Shell interval to 0 so it will always run our command
			//and use this one to set the thread sleep interval
			this.refreshInterval = interval;
			this.dirPath = path.getCanonicalPath();
			//populate the used variable if the initial value is not specified.
			if (initialUsed < 0)
			{
				run();
			}
			else
			{
				this.used.set(initialUsed);
			}
		}

		/// <summary>Keeps track of disk usage.</summary>
		/// <param name="path">the path to check disk usage in</param>
		/// <param name="conf">configuration object</param>
		/// <exception cref="System.IO.IOException">if we fail to refresh the disk usage</exception>
		public DU(java.io.File path, org.apache.hadoop.conf.Configuration conf)
			: this(path, conf, -1L)
		{
		}

		/// <summary>Keeps track of disk usage.</summary>
		/// <param name="path">the path to check disk usage in</param>
		/// <param name="conf">configuration object</param>
		/// <param name="initialUsed">use it until the next refresh.</param>
		/// <exception cref="System.IO.IOException">if we fail to refresh the disk usage</exception>
		public DU(java.io.File path, org.apache.hadoop.conf.Configuration conf, long initialUsed
			)
			: this(path, conf.getLong(org.apache.hadoop.fs.CommonConfigurationKeys.FS_DU_INTERVAL_KEY
				, org.apache.hadoop.fs.CommonConfigurationKeys.FS_DU_INTERVAL_DEFAULT), initialUsed
				)
		{
		}

		/// <summary>This thread refreshes the "used" variable.</summary>
		/// <remarks>
		/// This thread refreshes the "used" variable.
		/// Future improvements could be to not permanently
		/// run this thread, instead run when getUsed is called.
		/// </remarks>
		internal class DURefreshThread : java.lang.Runnable
		{
			public virtual void run()
			{
				while (this._enclosing.shouldRun)
				{
					try
					{
						java.lang.Thread.sleep(this._enclosing.refreshInterval);
						try
						{
							//update the used variable
							this._enclosing.run();
						}
						catch (System.IO.IOException e)
						{
							lock (this._enclosing)
							{
								//save the latest exception so we can return it in getUsed()
								this._enclosing.duException = e;
							}
							org.apache.hadoop.util.Shell.LOG.warn("Could not get disk usage information", e);
						}
					}
					catch (System.Exception)
					{
					}
				}
			}

			internal DURefreshThread(DU _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly DU _enclosing;
		}

		/// <summary>Decrease how much disk space we use.</summary>
		/// <param name="value">decrease by this value</param>
		public virtual void decDfsUsed(long value)
		{
			used.addAndGet(-value);
		}

		/// <summary>Increase how much disk space we use.</summary>
		/// <param name="value">increase by this value</param>
		public virtual void incDfsUsed(long value)
		{
			used.addAndGet(value);
		}

		/// <returns>disk space used</returns>
		/// <exception cref="System.IO.IOException">if the shell command fails</exception>
		public virtual long getUsed()
		{
			//if the updating thread isn't started, update on demand
			if (refreshUsed == null)
			{
				run();
			}
			else
			{
				lock (this)
				{
					//if an exception was thrown in the last run, rethrow
					if (duException != null)
					{
						System.IO.IOException tmp = duException;
						duException = null;
						throw tmp;
					}
				}
			}
			return System.Math.max(used, 0L);
		}

		/// <returns>the path of which we're keeping track of disk usage</returns>
		public virtual string getDirPath()
		{
			return dirPath;
		}

		/// <summary>Override to hook in DUHelper class.</summary>
		/// <remarks>
		/// Override to hook in DUHelper class. Maybe this can be used more
		/// generally as well on Unix/Linux based systems
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		protected internal override void run()
		{
			if (WINDOWS)
			{
				used.set(org.apache.hadoop.fs.DUHelper.getFolderUsage(dirPath));
				return;
			}
			base.run();
		}

		/// <summary>Start the disk usage checking thread.</summary>
		public virtual void start()
		{
			//only start the thread if the interval is sane
			if (refreshInterval > 0)
			{
				refreshUsed = new java.lang.Thread(new org.apache.hadoop.fs.DU.DURefreshThread(this
					), "refreshUsed-" + dirPath);
				refreshUsed.setDaemon(true);
				refreshUsed.start();
			}
		}

		/// <summary>Shut down the refreshing thread.</summary>
		public virtual void shutdown()
		{
			this.shouldRun = false;
			if (this.refreshUsed != null)
			{
				this.refreshUsed.interrupt();
			}
		}

		public override string ToString()
		{
			return "du -sk " + dirPath + "\n" + used + "\t" + dirPath;
		}

		protected internal override string[] getExecString()
		{
			return new string[] { "du", "-sk", dirPath };
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void parseExecResult(java.io.BufferedReader lines)
		{
			string line = lines.readLine();
			if (line == null)
			{
				throw new System.IO.IOException("Expecting a line not the end of stream");
			}
			string[] tokens = line.split("\t");
			if (tokens.Length == 0)
			{
				throw new System.IO.IOException("Illegal du output");
			}
			this.used.set(long.Parse(tokens[0]) * 1024);
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			string path = ".";
			if (args.Length > 0)
			{
				path = args[0];
			}
			System.Console.Out.WriteLine(new org.apache.hadoop.fs.DU(new java.io.File(path), 
				new org.apache.hadoop.conf.Configuration()).ToString());
		}
	}
}
