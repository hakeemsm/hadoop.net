using System;
using System.IO;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.Fs;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Provides a trash facility which supports pluggable Trash policies.</summary>
	/// <remarks>
	/// Provides a trash facility which supports pluggable Trash policies.
	/// See the implementation of the configured TrashPolicy for more
	/// details.
	/// </remarks>
	public class Trash : Configured
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.FS.Trash
			));

		private TrashPolicy trashPolicy;

		/// <summary>Construct a trash can accessor.</summary>
		/// <param name="conf">a Configuration</param>
		/// <exception cref="System.IO.IOException"/>
		public Trash(Configuration conf)
			: this(FileSystem.Get(conf), conf)
		{
		}

		/// <summary>Construct a trash can accessor for the FileSystem provided.</summary>
		/// <param name="fs">the FileSystem</param>
		/// <param name="conf">a Configuration</param>
		/// <exception cref="System.IO.IOException"/>
		public Trash(FileSystem fs, Configuration conf)
			: base(conf)
		{
			// configured trash policy instance
			trashPolicy = TrashPolicy.GetInstance(conf, fs, fs.GetHomeDirectory());
		}

		/// <summary>
		/// In case of the symlinks or mount points, one has to move the appropriate
		/// trashbin in the actual volume of the path p being deleted.
		/// </summary>
		/// <remarks>
		/// In case of the symlinks or mount points, one has to move the appropriate
		/// trashbin in the actual volume of the path p being deleted.
		/// Hence we get the file system of the fully-qualified resolved-path and
		/// then move the path p to the trashbin in that volume,
		/// </remarks>
		/// <param name="fs">- the filesystem of path p</param>
		/// <param name="p">- the  path being deleted - to be moved to trasg</param>
		/// <param name="conf">- configuration</param>
		/// <returns>false if the item is already in the trash or trash is disabled</returns>
		/// <exception cref="System.IO.IOException">on error</exception>
		public static bool MoveToAppropriateTrash(FileSystem fs, Path p, Configuration conf
			)
		{
			Path fullyResolvedPath = fs.ResolvePath(p);
			FileSystem fullyResolvedFs = FileSystem.Get(fullyResolvedPath.ToUri(), conf);
			// If the trash interval is configured server side then clobber this
			// configuration so that we always respect the server configuration.
			try
			{
				long trashInterval = fullyResolvedFs.GetServerDefaults(fullyResolvedPath).GetTrashInterval
					();
				if (0 != trashInterval)
				{
					Configuration confCopy = new Configuration(conf);
					confCopy.SetLong(CommonConfigurationKeysPublic.FsTrashIntervalKey, trashInterval);
					conf = confCopy;
				}
			}
			catch (Exception e)
			{
				// If we can not determine that trash is enabled server side then
				// bail rather than potentially deleting a file when trash is enabled.
				Log.Warn("Failed to get server trash configuration", e);
				throw new IOException("Failed to get server trash configuration", e);
			}
			Org.Apache.Hadoop.FS.Trash trash = new Org.Apache.Hadoop.FS.Trash(fullyResolvedFs
				, conf);
			bool success = trash.MoveToTrash(fullyResolvedPath);
			if (success)
			{
				System.Console.Out.WriteLine("Moved: '" + p + "' to trash at: " + trash.GetCurrentTrashDir
					());
			}
			return success;
		}

		/// <summary>Returns whether the trash is enabled for this filesystem</summary>
		public virtual bool IsEnabled()
		{
			return trashPolicy.IsEnabled();
		}

		/// <summary>Move a file or directory to the current trash directory.</summary>
		/// <returns>false if the item is already in the trash or trash is disabled</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool MoveToTrash(Path path)
		{
			return trashPolicy.MoveToTrash(path);
		}

		/// <summary>Create a trash checkpoint.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Checkpoint()
		{
			trashPolicy.CreateCheckpoint();
		}

		/// <summary>Delete old checkpoint(s).</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Expunge()
		{
			trashPolicy.DeleteCheckpoint();
		}

		/// <summary>get the current working directory</summary>
		internal virtual Path GetCurrentTrashDir()
		{
			return trashPolicy.GetCurrentTrashDir();
		}

		/// <summary>get the configured trash policy</summary>
		internal virtual TrashPolicy GetTrashPolicy()
		{
			return trashPolicy;
		}

		/// <summary>
		/// Return a
		/// <see cref="Sharpen.Runnable"/>
		/// that periodically empties the trash of all
		/// users, intended to be run by the superuser.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual Runnable GetEmptier()
		{
			return trashPolicy.GetEmptier();
		}
	}
}
