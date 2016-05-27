using System;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS
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
	public class TrashPolicyDefault : TrashPolicy
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.FS.TrashPolicyDefault
			));

		private static readonly Path Current = new Path("Current");

		private static readonly Path Trash = new Path(".Trash/");

		private static readonly FsPermission Permission = new FsPermission(FsAction.All, 
			FsAction.None, FsAction.None);

		private static readonly DateFormat Checkpoint = new SimpleDateFormat("yyMMddHHmmss"
			);

		/// <summary>Format of checkpoint directories used prior to Hadoop 0.23.</summary>
		private static readonly DateFormat OldCheckpoint = new SimpleDateFormat("yyMMddHHmm"
			);

		private const int MsecsPerMinute = 60 * 1000;

		private Path current;

		private Path homesParent;

		private long emptierInterval;

		public TrashPolicyDefault()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		private TrashPolicyDefault(FileSystem fs, Path home, Configuration conf)
		{
			Initialize(conf, fs, home);
		}

		public override void Initialize(Configuration conf, FileSystem fs, Path home)
		{
			this.fs = fs;
			this.trash = new Path(home, Trash);
			this.homesParent = home.GetParent();
			this.current = new Path(trash, Current);
			this.deletionInterval = (long)(conf.GetFloat(CommonConfigurationKeysPublic.FsTrashIntervalKey
				, CommonConfigurationKeysPublic.FsTrashIntervalDefault) * MsecsPerMinute);
			this.emptierInterval = (long)(conf.GetFloat(CommonConfigurationKeysPublic.FsTrashCheckpointIntervalKey
				, CommonConfigurationKeysPublic.FsTrashCheckpointIntervalDefault) * MsecsPerMinute
				);
			Log.Info("Namenode trash configuration: Deletion interval = " + (this.deletionInterval
				 / MsecsPerMinute) + " minutes, Emptier interval = " + (this.emptierInterval / MsecsPerMinute
				) + " minutes.");
		}

		private Path MakeTrashRelativePath(Path basePath, Path rmFilePath)
		{
			return Path.MergePaths(basePath, rmFilePath);
		}

		public override bool IsEnabled()
		{
			return deletionInterval != 0;
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool MoveToTrash(Path path)
		{
			if (!IsEnabled())
			{
				return false;
			}
			if (!path.IsAbsolute())
			{
				// make path absolute
				path = new Path(fs.GetWorkingDirectory(), path);
			}
			if (!fs.Exists(path))
			{
				// check that path exists
				throw new FileNotFoundException(path.ToString());
			}
			string qpath = fs.MakeQualified(path).ToString();
			if (qpath.StartsWith(trash.ToString()))
			{
				return false;
			}
			// already in trash
			if (trash.GetParent().ToString().StartsWith(qpath))
			{
				throw new IOException("Cannot move \"" + path + "\" to the trash, as it contains the trash"
					);
			}
			Path trashPath = MakeTrashRelativePath(current, path);
			Path baseTrashPath = MakeTrashRelativePath(current, path.GetParent());
			IOException cause = null;
			// try twice, in case checkpoint between the mkdirs() & rename()
			for (int i = 0; i < 2; i++)
			{
				try
				{
					if (!fs.Mkdirs(baseTrashPath, Permission))
					{
						// create current
						Log.Warn("Can't create(mkdir) trash directory: " + baseTrashPath);
						return false;
					}
				}
				catch (IOException e)
				{
					Log.Warn("Can't create trash directory: " + baseTrashPath, e);
					cause = e;
					break;
				}
				try
				{
					// if the target path in Trash already exists, then append with 
					// a current time in millisecs.
					string orig = trashPath.ToString();
					while (fs.Exists(trashPath))
					{
						trashPath = new Path(orig + Time.Now());
					}
					if (fs.Rename(path, trashPath))
					{
						// move to current trash
						return true;
					}
				}
				catch (IOException e)
				{
					cause = e;
				}
			}
			throw (IOException)Sharpen.Extensions.InitCause(new IOException("Failed to move to trash: "
				 + path), cause);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void CreateCheckpoint()
		{
			if (!fs.Exists(current))
			{
				// no trash, no checkpoint
				return;
			}
			Path checkpointBase;
			lock (Checkpoint)
			{
				checkpointBase = new Path(trash, Checkpoint.Format(new DateTime()));
			}
			Path checkpoint = checkpointBase;
			int attempt = 0;
			while (true)
			{
				try
				{
					fs.Rename(current, checkpoint, Options.Rename.None);
					break;
				}
				catch (FileAlreadyExistsException)
				{
					if (++attempt > 1000)
					{
						throw new IOException("Failed to checkpoint trash: " + checkpoint);
					}
					checkpoint = checkpointBase.Suffix("-" + attempt);
				}
			}
			Log.Info("Created trash checkpoint: " + checkpoint.ToUri().GetPath());
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DeleteCheckpoint()
		{
			FileStatus[] dirs = null;
			try
			{
				dirs = fs.ListStatus(trash);
			}
			catch (FileNotFoundException)
			{
				// scan trash sub-directories
				return;
			}
			long now = Time.Now();
			for (int i = 0; i < dirs.Length; i++)
			{
				Path path = dirs[i].GetPath();
				string dir = path.ToUri().GetPath();
				string name = path.GetName();
				if (name.Equals(Current.GetName()))
				{
					// skip current
					continue;
				}
				long time;
				try
				{
					time = GetTimeFromCheckpoint(name);
				}
				catch (ParseException)
				{
					Log.Warn("Unexpected item in trash: " + dir + ". Ignoring.");
					continue;
				}
				if ((now - deletionInterval) > time)
				{
					if (fs.Delete(path, true))
					{
						Log.Info("Deleted trash checkpoint: " + dir);
					}
					else
					{
						Log.Warn("Couldn't delete checkpoint: " + dir + " Ignoring.");
					}
				}
			}
		}

		public override Path GetCurrentTrashDir()
		{
			return current;
		}

		/// <exception cref="System.IO.IOException"/>
		public override Runnable GetEmptier()
		{
			return new TrashPolicyDefault.Emptier(this, GetConf(), emptierInterval);
		}

		private class Emptier : Runnable
		{
			private Configuration conf;

			private long emptierInterval;

			/// <exception cref="System.IO.IOException"/>
			internal Emptier(TrashPolicyDefault _enclosing, Configuration conf, long emptierInterval
				)
			{
				this._enclosing = _enclosing;
				this.conf = conf;
				this.emptierInterval = emptierInterval;
				if (emptierInterval > this._enclosing.deletionInterval || emptierInterval == 0)
				{
					TrashPolicyDefault.Log.Info("The configured checkpoint interval is " + (emptierInterval
						 / TrashPolicyDefault.MsecsPerMinute) + " minutes." + " Using an interval of " +
						 (this._enclosing.deletionInterval / TrashPolicyDefault.MsecsPerMinute) + " minutes that is used for deletion instead"
						);
					this.emptierInterval = this._enclosing.deletionInterval;
				}
			}

			public virtual void Run()
			{
				if (this.emptierInterval == 0)
				{
					return;
				}
				// trash disabled
				long now = Time.Now();
				long end;
				while (true)
				{
					end = this.Ceiling(now, this.emptierInterval);
					try
					{
						// sleep for interval
						Sharpen.Thread.Sleep(end - now);
					}
					catch (Exception)
					{
						break;
					}
					// exit on interrupt
					try
					{
						now = Time.Now();
						if (now >= end)
						{
							FileStatus[] homes = null;
							try
							{
								homes = this._enclosing.fs.ListStatus(this._enclosing.homesParent);
							}
							catch (IOException e)
							{
								// list all home dirs
								TrashPolicyDefault.Log.Warn("Trash can't list homes: " + e + " Sleeping.");
								continue;
							}
							foreach (FileStatus home in homes)
							{
								// dump each trash
								if (!home.IsDirectory())
								{
									continue;
								}
								try
								{
									TrashPolicyDefault trash = new TrashPolicyDefault(this._enclosing.fs, home.GetPath
										(), this.conf);
									trash.DeleteCheckpoint();
									trash.CreateCheckpoint();
								}
								catch (IOException e)
								{
									TrashPolicyDefault.Log.Warn("Trash caught: " + e + ". Skipping " + home.GetPath()
										 + ".");
								}
							}
						}
					}
					catch (Exception e)
					{
						TrashPolicyDefault.Log.Warn("RuntimeException during Trash.Emptier.run(): ", e);
					}
				}
				try
				{
					this._enclosing.fs.Close();
				}
				catch (IOException e)
				{
					TrashPolicyDefault.Log.Warn("Trash cannot close FileSystem: ", e);
				}
			}

			private long Ceiling(long time, long interval)
			{
				return this.Floor(time, interval) + interval;
			}

			private long Floor(long time, long interval)
			{
				return (time / interval) * interval;
			}

			private readonly TrashPolicyDefault _enclosing;
		}

		/// <exception cref="Sharpen.ParseException"/>
		private long GetTimeFromCheckpoint(string name)
		{
			long time;
			try
			{
				lock (Checkpoint)
				{
					time = Checkpoint.Parse(name).GetTime();
				}
			}
			catch (ParseException)
			{
				// Check for old-style checkpoint directories left over
				// after an upgrade from Hadoop 1.x
				lock (OldCheckpoint)
				{
					time = OldCheckpoint.Parse(name).GetTime();
				}
			}
			return time;
		}
	}
}
