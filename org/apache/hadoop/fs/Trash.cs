using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>Provides a trash facility which supports pluggable Trash policies.</summary>
	/// <remarks>
	/// Provides a trash facility which supports pluggable Trash policies.
	/// See the implementation of the configured TrashPolicy for more
	/// details.
	/// </remarks>
	public class Trash : org.apache.hadoop.conf.Configured
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.Trash)));

		private org.apache.hadoop.fs.TrashPolicy trashPolicy;

		/// <summary>Construct a trash can accessor.</summary>
		/// <param name="conf">a Configuration</param>
		/// <exception cref="System.IO.IOException"/>
		public Trash(org.apache.hadoop.conf.Configuration conf)
			: this(org.apache.hadoop.fs.FileSystem.get(conf), conf)
		{
		}

		/// <summary>Construct a trash can accessor for the FileSystem provided.</summary>
		/// <param name="fs">the FileSystem</param>
		/// <param name="conf">a Configuration</param>
		/// <exception cref="System.IO.IOException"/>
		public Trash(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.conf.Configuration
			 conf)
			: base(conf)
		{
			// configured trash policy instance
			trashPolicy = org.apache.hadoop.fs.TrashPolicy.getInstance(conf, fs, fs.getHomeDirectory
				());
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
		public static bool moveToAppropriateTrash(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.fs.Path
			 p, org.apache.hadoop.conf.Configuration conf)
		{
			org.apache.hadoop.fs.Path fullyResolvedPath = fs.resolvePath(p);
			org.apache.hadoop.fs.FileSystem fullyResolvedFs = org.apache.hadoop.fs.FileSystem
				.get(fullyResolvedPath.toUri(), conf);
			// If the trash interval is configured server side then clobber this
			// configuration so that we always respect the server configuration.
			try
			{
				long trashInterval = fullyResolvedFs.getServerDefaults(fullyResolvedPath).getTrashInterval
					();
				if (0 != trashInterval)
				{
					org.apache.hadoop.conf.Configuration confCopy = new org.apache.hadoop.conf.Configuration
						(conf);
					confCopy.setLong(org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY
						, trashInterval);
					conf = confCopy;
				}
			}
			catch (System.Exception e)
			{
				// If we can not determine that trash is enabled server side then
				// bail rather than potentially deleting a file when trash is enabled.
				LOG.warn("Failed to get server trash configuration", e);
				throw new System.IO.IOException("Failed to get server trash configuration", e);
			}
			org.apache.hadoop.fs.Trash trash = new org.apache.hadoop.fs.Trash(fullyResolvedFs
				, conf);
			bool success = trash.moveToTrash(fullyResolvedPath);
			if (success)
			{
				System.Console.Out.WriteLine("Moved: '" + p + "' to trash at: " + trash.getCurrentTrashDir
					());
			}
			return success;
		}

		/// <summary>Returns whether the trash is enabled for this filesystem</summary>
		public virtual bool isEnabled()
		{
			return trashPolicy.isEnabled();
		}

		/// <summary>Move a file or directory to the current trash directory.</summary>
		/// <returns>false if the item is already in the trash or trash is disabled</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool moveToTrash(org.apache.hadoop.fs.Path path)
		{
			return trashPolicy.moveToTrash(path);
		}

		/// <summary>Create a trash checkpoint.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void checkpoint()
		{
			trashPolicy.createCheckpoint();
		}

		/// <summary>Delete old checkpoint(s).</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void expunge()
		{
			trashPolicy.deleteCheckpoint();
		}

		/// <summary>get the current working directory</summary>
		internal virtual org.apache.hadoop.fs.Path getCurrentTrashDir()
		{
			return trashPolicy.getCurrentTrashDir();
		}

		/// <summary>get the configured trash policy</summary>
		internal virtual org.apache.hadoop.fs.TrashPolicy getTrashPolicy()
		{
			return trashPolicy;
		}

		/// <summary>
		/// Return a
		/// <see cref="java.lang.Runnable"/>
		/// that periodically empties the trash of all
		/// users, intended to be run by the superuser.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual java.lang.Runnable getEmptier()
		{
			return trashPolicy.getEmptier();
		}
	}
}
