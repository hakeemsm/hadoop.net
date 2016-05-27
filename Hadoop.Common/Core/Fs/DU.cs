using System;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Filesystem disk space usage statistics.</summary>
	/// <remarks>Filesystem disk space usage statistics.  Uses the unix 'du' program</remarks>
	public class DU : Shell
	{
		private string dirPath;

		private AtomicLong used = new AtomicLong();

		private volatile bool shouldRun = true;

		private Sharpen.Thread refreshUsed;

		private IOException duException = null;

		private long refreshInterval;

		/// <summary>Keeps track of disk usage.</summary>
		/// <param name="path">the path to check disk usage in</param>
		/// <param name="interval">refresh the disk usage at this interval</param>
		/// <exception cref="System.IO.IOException">if we fail to refresh the disk usage</exception>
		public DU(FilePath path, long interval)
			: this(path, interval, -1L)
		{
		}

		/// <summary>Keeps track of disk usage.</summary>
		/// <param name="path">the path to check disk usage in</param>
		/// <param name="interval">refresh the disk usage at this interval</param>
		/// <param name="initialUsed">use this value until next refresh</param>
		/// <exception cref="System.IO.IOException">if we fail to refresh the disk usage</exception>
		public DU(FilePath path, long interval, long initialUsed)
			: base(0)
		{
			//we set the Shell interval to 0 so it will always run our command
			//and use this one to set the thread sleep interval
			this.refreshInterval = interval;
			this.dirPath = path.GetCanonicalPath();
			//populate the used variable if the initial value is not specified.
			if (initialUsed < 0)
			{
				Run();
			}
			else
			{
				this.used.Set(initialUsed);
			}
		}

		/// <summary>Keeps track of disk usage.</summary>
		/// <param name="path">the path to check disk usage in</param>
		/// <param name="conf">configuration object</param>
		/// <exception cref="System.IO.IOException">if we fail to refresh the disk usage</exception>
		public DU(FilePath path, Configuration conf)
			: this(path, conf, -1L)
		{
		}

		/// <summary>Keeps track of disk usage.</summary>
		/// <param name="path">the path to check disk usage in</param>
		/// <param name="conf">configuration object</param>
		/// <param name="initialUsed">use it until the next refresh.</param>
		/// <exception cref="System.IO.IOException">if we fail to refresh the disk usage</exception>
		public DU(FilePath path, Configuration conf, long initialUsed)
			: this(path, conf.GetLong(CommonConfigurationKeys.FsDuIntervalKey, CommonConfigurationKeys
				.FsDuIntervalDefault), initialUsed)
		{
		}

		/// <summary>This thread refreshes the "used" variable.</summary>
		/// <remarks>
		/// This thread refreshes the "used" variable.
		/// Future improvements could be to not permanently
		/// run this thread, instead run when getUsed is called.
		/// </remarks>
		internal class DURefreshThread : Runnable
		{
			public virtual void Run()
			{
				while (this._enclosing.shouldRun)
				{
					try
					{
						Sharpen.Thread.Sleep(this._enclosing.refreshInterval);
						try
						{
							//update the used variable
							this._enclosing.Run();
						}
						catch (IOException e)
						{
							lock (this._enclosing)
							{
								//save the latest exception so we can return it in getUsed()
								this._enclosing.duException = e;
							}
							Shell.Log.Warn("Could not get disk usage information", e);
						}
					}
					catch (Exception)
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
		public virtual void DecDfsUsed(long value)
		{
			used.AddAndGet(-value);
		}

		/// <summary>Increase how much disk space we use.</summary>
		/// <param name="value">increase by this value</param>
		public virtual void IncDfsUsed(long value)
		{
			used.AddAndGet(value);
		}

		/// <returns>disk space used</returns>
		/// <exception cref="System.IO.IOException">if the shell command fails</exception>
		public virtual long GetUsed()
		{
			//if the updating thread isn't started, update on demand
			if (refreshUsed == null)
			{
				Run();
			}
			else
			{
				lock (this)
				{
					//if an exception was thrown in the last run, rethrow
					if (duException != null)
					{
						IOException tmp = duException;
						duException = null;
						throw tmp;
					}
				}
			}
			return Math.Max(used, 0L);
		}

		/// <returns>the path of which we're keeping track of disk usage</returns>
		public virtual string GetDirPath()
		{
			return dirPath;
		}

		/// <summary>Override to hook in DUHelper class.</summary>
		/// <remarks>
		/// Override to hook in DUHelper class. Maybe this can be used more
		/// generally as well on Unix/Linux based systems
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		protected internal override void Run()
		{
			if (Windows)
			{
				used.Set(DUHelper.GetFolderUsage(dirPath));
				return;
			}
			base.Run();
		}

		/// <summary>Start the disk usage checking thread.</summary>
		public virtual void Start()
		{
			//only start the thread if the interval is sane
			if (refreshInterval > 0)
			{
				refreshUsed = new Sharpen.Thread(new DU.DURefreshThread(this), "refreshUsed-" + dirPath
					);
				refreshUsed.SetDaemon(true);
				refreshUsed.Start();
			}
		}

		/// <summary>Shut down the refreshing thread.</summary>
		public virtual void Shutdown()
		{
			this.shouldRun = false;
			if (this.refreshUsed != null)
			{
				this.refreshUsed.Interrupt();
			}
		}

		public override string ToString()
		{
			return "du -sk " + dirPath + "\n" + used + "\t" + dirPath;
		}

		protected internal override string[] GetExecString()
		{
			return new string[] { "du", "-sk", dirPath };
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void ParseExecResult(BufferedReader lines)
		{
			string line = lines.ReadLine();
			if (line == null)
			{
				throw new IOException("Expecting a line not the end of stream");
			}
			string[] tokens = line.Split("\t");
			if (tokens.Length == 0)
			{
				throw new IOException("Illegal du output");
			}
			this.used.Set(long.Parse(tokens[0]) * 1024);
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			string path = ".";
			if (args.Length > 0)
			{
				path = args[0];
			}
			System.Console.Out.WriteLine(new DU(new FilePath(path), new Configuration()).ToString
				());
		}
	}
}
