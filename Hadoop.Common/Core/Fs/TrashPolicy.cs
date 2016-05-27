using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>This interface is used for implementing different Trash policies.</summary>
	/// <remarks>
	/// This interface is used for implementing different Trash policies.
	/// Provides factory method to create instances of the configured Trash policy.
	/// </remarks>
	public abstract class TrashPolicy : Configured
	{
		protected internal FileSystem fs;

		protected internal Path trash;

		protected internal long deletionInterval;

		// the FileSystem
		// path to trash directory
		// deletion interval for Emptier
		/// <summary>Used to setup the trash policy.</summary>
		/// <remarks>
		/// Used to setup the trash policy. Must be implemented by all TrashPolicy
		/// implementations
		/// </remarks>
		/// <param name="conf">the configuration to be used</param>
		/// <param name="fs">the filesystem to be used</param>
		/// <param name="home">the home directory</param>
		public abstract void Initialize(Configuration conf, FileSystem fs, Path home);

		/// <summary>Returns whether the Trash Policy is enabled for this filesystem</summary>
		public abstract bool IsEnabled();

		/// <summary>Move a file or directory to the current trash directory.</summary>
		/// <returns>false if the item is already in the trash or trash is disabled</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract bool MoveToTrash(Path path);

		/// <summary>Create a trash checkpoint.</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract void CreateCheckpoint();

		/// <summary>Delete old trash checkpoint(s).</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract void DeleteCheckpoint();

		/// <summary>Get the current working directory of the Trash Policy</summary>
		public abstract Path GetCurrentTrashDir();

		/// <summary>
		/// Return a
		/// <see cref="Sharpen.Runnable"/>
		/// that periodically empties the trash of all
		/// users, intended to be run by the superuser.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract Runnable GetEmptier();

		/// <summary>
		/// Get an instance of the configured TrashPolicy based on the value
		/// of the configuration parameter fs.trash.classname.
		/// </summary>
		/// <param name="conf">the configuration to be used</param>
		/// <param name="fs">the file system to be used</param>
		/// <param name="home">the home directory</param>
		/// <returns>an instance of TrashPolicy</returns>
		public static TrashPolicy GetInstance(Configuration conf, FileSystem fs, Path home
			)
		{
			Type trashClass = conf.GetClass<TrashPolicy>("fs.trash.classname", typeof(TrashPolicyDefault
				));
			TrashPolicy trash = ReflectionUtils.NewInstance(trashClass, conf);
			trash.Initialize(conf, fs, home);
			// initialize TrashPolicy
			return trash;
		}
	}
}
