using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>Provides a <i>trash</i> feature.</summary>
	/// <remarks>
	/// Provides a <i>trash</i> feature.  Files are moved to a user's trash
	/// directory, a subdirectory of their home directory named ".Trash".  Files are
	/// initially moved to a <i>current</i> sub-directory of the trash directory.
	/// Within that sub-directory their original path is preserved.  Periodically
	/// one may checkpoint the current trash and remove older checkpoints.  (This
	/// design permits trash management without enumeration of the full trash
	/// content, without date support in the filesystem, and without clock
	/// synchronization.)
	/// </remarks>
	public class TrashPolicyDefault : org.apache.hadoop.fs.TrashPolicy
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.TrashPolicyDefault
			)));

		private static readonly org.apache.hadoop.fs.Path CURRENT = new org.apache.hadoop.fs.Path
			("Current");

		private static readonly org.apache.hadoop.fs.Path TRASH = new org.apache.hadoop.fs.Path
			(".Trash/");

		private static readonly org.apache.hadoop.fs.permission.FsPermission PERMISSION = 
			new org.apache.hadoop.fs.permission.FsPermission(org.apache.hadoop.fs.permission.FsAction
			.ALL, org.apache.hadoop.fs.permission.FsAction.NONE, org.apache.hadoop.fs.permission.FsAction
			.NONE);

		private static readonly java.text.DateFormat CHECKPOINT = new java.text.SimpleDateFormat
			("yyMMddHHmmss");

		/// <summary>Format of checkpoint directories used prior to Hadoop 0.23.</summary>
		private static readonly java.text.DateFormat OLD_CHECKPOINT = new java.text.SimpleDateFormat
			("yyMMddHHmm");

		private const int MSECS_PER_MINUTE = 60 * 1000;

		private org.apache.hadoop.fs.Path current;

		private org.apache.hadoop.fs.Path homesParent;

		private long emptierInterval;

		public TrashPolicyDefault()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		private TrashPolicyDefault(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.fs.Path
			 home, org.apache.hadoop.conf.Configuration conf)
		{
			initialize(conf, fs, home);
		}

		public override void initialize(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.FileSystem
			 fs, org.apache.hadoop.fs.Path home)
		{
			this.fs = fs;
			this.trash = new org.apache.hadoop.fs.Path(home, TRASH);
			this.homesParent = home.getParent();
			this.current = new org.apache.hadoop.fs.Path(trash, CURRENT);
			this.deletionInterval = (long)(conf.getFloat(org.apache.hadoop.fs.CommonConfigurationKeysPublic
				.FS_TRASH_INTERVAL_KEY, org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_DEFAULT
				) * MSECS_PER_MINUTE);
			this.emptierInterval = (long)(conf.getFloat(org.apache.hadoop.fs.CommonConfigurationKeysPublic
				.FS_TRASH_CHECKPOINT_INTERVAL_KEY, org.apache.hadoop.fs.CommonConfigurationKeysPublic
				.FS_TRASH_CHECKPOINT_INTERVAL_DEFAULT) * MSECS_PER_MINUTE);
			LOG.info("Namenode trash configuration: Deletion interval = " + (this.deletionInterval
				 / MSECS_PER_MINUTE) + " minutes, Emptier interval = " + (this.emptierInterval /
				 MSECS_PER_MINUTE) + " minutes.");
		}

		private org.apache.hadoop.fs.Path makeTrashRelativePath(org.apache.hadoop.fs.Path
			 basePath, org.apache.hadoop.fs.Path rmFilePath)
		{
			return org.apache.hadoop.fs.Path.mergePaths(basePath, rmFilePath);
		}

		public override bool isEnabled()
		{
			return deletionInterval != 0;
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool moveToTrash(org.apache.hadoop.fs.Path path)
		{
			if (!isEnabled())
			{
				return false;
			}
			if (!path.isAbsolute())
			{
				// make path absolute
				path = new org.apache.hadoop.fs.Path(fs.getWorkingDirectory(), path);
			}
			if (!fs.exists(path))
			{
				// check that path exists
				throw new java.io.FileNotFoundException(path.ToString());
			}
			string qpath = fs.makeQualified(path).ToString();
			if (qpath.StartsWith(trash.ToString()))
			{
				return false;
			}
			// already in trash
			if (trash.getParent().ToString().StartsWith(qpath))
			{
				throw new System.IO.IOException("Cannot move \"" + path + "\" to the trash, as it contains the trash"
					);
			}
			org.apache.hadoop.fs.Path trashPath = makeTrashRelativePath(current, path);
			org.apache.hadoop.fs.Path baseTrashPath = makeTrashRelativePath(current, path.getParent
				());
			System.IO.IOException cause = null;
			// try twice, in case checkpoint between the mkdirs() & rename()
			for (int i = 0; i < 2; i++)
			{
				try
				{
					if (!fs.mkdirs(baseTrashPath, PERMISSION))
					{
						// create current
						LOG.warn("Can't create(mkdir) trash directory: " + baseTrashPath);
						return false;
					}
				}
				catch (System.IO.IOException e)
				{
					LOG.warn("Can't create trash directory: " + baseTrashPath, e);
					cause = e;
					break;
				}
				try
				{
					// if the target path in Trash already exists, then append with 
					// a current time in millisecs.
					string orig = trashPath.ToString();
					while (fs.exists(trashPath))
					{
						trashPath = new org.apache.hadoop.fs.Path(orig + org.apache.hadoop.util.Time.now(
							));
					}
					if (fs.rename(path, trashPath))
					{
						// move to current trash
						return true;
					}
				}
				catch (System.IO.IOException e)
				{
					cause = e;
				}
			}
			throw (System.IO.IOException)new System.IO.IOException("Failed to move to trash: "
				 + path).initCause(cause);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void createCheckpoint()
		{
			if (!fs.exists(current))
			{
				// no trash, no checkpoint
				return;
			}
			org.apache.hadoop.fs.Path checkpointBase;
			lock (CHECKPOINT)
			{
				checkpointBase = new org.apache.hadoop.fs.Path(trash, CHECKPOINT.format(new System.DateTime
					()));
			}
			org.apache.hadoop.fs.Path checkpoint = checkpointBase;
			int attempt = 0;
			while (true)
			{
				try
				{
					fs.rename(current, checkpoint, org.apache.hadoop.fs.Options.Rename.NONE);
					break;
				}
				catch (org.apache.hadoop.fs.FileAlreadyExistsException)
				{
					if (++attempt > 1000)
					{
						throw new System.IO.IOException("Failed to checkpoint trash: " + checkpoint);
					}
					checkpoint = checkpointBase.suffix("-" + attempt);
				}
			}
			LOG.info("Created trash checkpoint: " + checkpoint.toUri().getPath());
		}

		/// <exception cref="System.IO.IOException"/>
		public override void deleteCheckpoint()
		{
			org.apache.hadoop.fs.FileStatus[] dirs = null;
			try
			{
				dirs = fs.listStatus(trash);
			}
			catch (java.io.FileNotFoundException)
			{
				// scan trash sub-directories
				return;
			}
			long now = org.apache.hadoop.util.Time.now();
			for (int i = 0; i < dirs.Length; i++)
			{
				org.apache.hadoop.fs.Path path = dirs[i].getPath();
				string dir = path.toUri().getPath();
				string name = path.getName();
				if (name.Equals(CURRENT.getName()))
				{
					// skip current
					continue;
				}
				long time;
				try
				{
					time = getTimeFromCheckpoint(name);
				}
				catch (java.text.ParseException)
				{
					LOG.warn("Unexpected item in trash: " + dir + ". Ignoring.");
					continue;
				}
				if ((now - deletionInterval) > time)
				{
					if (fs.delete(path, true))
					{
						LOG.info("Deleted trash checkpoint: " + dir);
					}
					else
					{
						LOG.warn("Couldn't delete checkpoint: " + dir + " Ignoring.");
					}
				}
			}
		}

		public override org.apache.hadoop.fs.Path getCurrentTrashDir()
		{
			return current;
		}

		/// <exception cref="System.IO.IOException"/>
		public override java.lang.Runnable getEmptier()
		{
			return new org.apache.hadoop.fs.TrashPolicyDefault.Emptier(this, getConf(), emptierInterval
				);
		}

		private class Emptier : java.lang.Runnable
		{
			private org.apache.hadoop.conf.Configuration conf;

			private long emptierInterval;

			/// <exception cref="System.IO.IOException"/>
			internal Emptier(TrashPolicyDefault _enclosing, org.apache.hadoop.conf.Configuration
				 conf, long emptierInterval)
			{
				this._enclosing = _enclosing;
				this.conf = conf;
				this.emptierInterval = emptierInterval;
				if (emptierInterval > this._enclosing.deletionInterval || emptierInterval == 0)
				{
					org.apache.hadoop.fs.TrashPolicyDefault.LOG.info("The configured checkpoint interval is "
						 + (emptierInterval / org.apache.hadoop.fs.TrashPolicyDefault.MSECS_PER_MINUTE) 
						+ " minutes." + " Using an interval of " + (this._enclosing.deletionInterval / org.apache.hadoop.fs.TrashPolicyDefault
						.MSECS_PER_MINUTE) + " minutes that is used for deletion instead");
					this.emptierInterval = this._enclosing.deletionInterval;
				}
			}

			public virtual void run()
			{
				if (this.emptierInterval == 0)
				{
					return;
				}
				// trash disabled
				long now = org.apache.hadoop.util.Time.now();
				long end;
				while (true)
				{
					end = this.ceiling(now, this.emptierInterval);
					try
					{
						// sleep for interval
						java.lang.Thread.sleep(end - now);
					}
					catch (System.Exception)
					{
						break;
					}
					// exit on interrupt
					try
					{
						now = org.apache.hadoop.util.Time.now();
						if (now >= end)
						{
							org.apache.hadoop.fs.FileStatus[] homes = null;
							try
							{
								homes = this._enclosing.fs.listStatus(this._enclosing.homesParent);
							}
							catch (System.IO.IOException e)
							{
								// list all home dirs
								org.apache.hadoop.fs.TrashPolicyDefault.LOG.warn("Trash can't list homes: " + e +
									 " Sleeping.");
								continue;
							}
							foreach (org.apache.hadoop.fs.FileStatus home in homes)
							{
								// dump each trash
								if (!home.isDirectory())
								{
									continue;
								}
								try
								{
									org.apache.hadoop.fs.TrashPolicyDefault trash = new org.apache.hadoop.fs.TrashPolicyDefault
										(this._enclosing.fs, home.getPath(), this.conf);
									trash.deleteCheckpoint();
									trash.createCheckpoint();
								}
								catch (System.IO.IOException e)
								{
									org.apache.hadoop.fs.TrashPolicyDefault.LOG.warn("Trash caught: " + e + ". Skipping "
										 + home.getPath() + ".");
								}
							}
						}
					}
					catch (System.Exception e)
					{
						org.apache.hadoop.fs.TrashPolicyDefault.LOG.warn("RuntimeException during Trash.Emptier.run(): "
							, e);
					}
				}
				try
				{
					this._enclosing.fs.close();
				}
				catch (System.IO.IOException e)
				{
					org.apache.hadoop.fs.TrashPolicyDefault.LOG.warn("Trash cannot close FileSystem: "
						, e);
				}
			}

			private long ceiling(long time, long interval)
			{
				return this.floor(time, interval) + interval;
			}

			private long floor(long time, long interval)
			{
				return (time / interval) * interval;
			}

			private readonly TrashPolicyDefault _enclosing;
		}

		/// <exception cref="java.text.ParseException"/>
		private long getTimeFromCheckpoint(string name)
		{
			long time;
			try
			{
				lock (CHECKPOINT)
				{
					time = CHECKPOINT.parse(name).getTime();
				}
			}
			catch (java.text.ParseException)
			{
				// Check for old-style checkpoint directories left over
				// after an upgrade from Hadoop 1.x
				lock (OLD_CHECKPOINT)
				{
					time = OLD_CHECKPOINT.parse(name).getTime();
				}
			}
			return time;
		}
	}
}
