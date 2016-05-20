using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>This interface is used for implementing different Trash policies.</summary>
	/// <remarks>
	/// This interface is used for implementing different Trash policies.
	/// Provides factory method to create instances of the configured Trash policy.
	/// </remarks>
	public abstract class TrashPolicy : org.apache.hadoop.conf.Configured
	{
		protected internal org.apache.hadoop.fs.FileSystem fs;

		protected internal org.apache.hadoop.fs.Path trash;

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
		public abstract void initialize(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.FileSystem
			 fs, org.apache.hadoop.fs.Path home);

		/// <summary>Returns whether the Trash Policy is enabled for this filesystem</summary>
		public abstract bool isEnabled();

		/// <summary>Move a file or directory to the current trash directory.</summary>
		/// <returns>false if the item is already in the trash or trash is disabled</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract bool moveToTrash(org.apache.hadoop.fs.Path path);

		/// <summary>Create a trash checkpoint.</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract void createCheckpoint();

		/// <summary>Delete old trash checkpoint(s).</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract void deleteCheckpoint();

		/// <summary>Get the current working directory of the Trash Policy</summary>
		public abstract org.apache.hadoop.fs.Path getCurrentTrashDir();

		/// <summary>
		/// Return a
		/// <see cref="java.lang.Runnable"/>
		/// that periodically empties the trash of all
		/// users, intended to be run by the superuser.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract java.lang.Runnable getEmptier();

		/// <summary>
		/// Get an instance of the configured TrashPolicy based on the value
		/// of the configuration parameter fs.trash.classname.
		/// </summary>
		/// <param name="conf">the configuration to be used</param>
		/// <param name="fs">the file system to be used</param>
		/// <param name="home">the home directory</param>
		/// <returns>an instance of TrashPolicy</returns>
		public static org.apache.hadoop.fs.TrashPolicy getInstance(org.apache.hadoop.conf.Configuration
			 conf, org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.fs.Path home)
		{
			java.lang.Class trashClass = conf.getClass<org.apache.hadoop.fs.TrashPolicy>("fs.trash.classname"
				, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.TrashPolicyDefault
				)));
			org.apache.hadoop.fs.TrashPolicy trash = org.apache.hadoop.util.ReflectionUtils.newInstance
				(trashClass, conf);
			trash.initialize(conf, fs, home);
			// initialize TrashPolicy
			return trash;
		}
	}
}
