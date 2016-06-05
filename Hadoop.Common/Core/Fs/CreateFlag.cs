using System.IO;
using Org.Apache.Hadoop;


namespace Org.Apache.Hadoop.FS
{
	/// <summary>CreateFlag specifies the file create semantic.</summary>
	/// <remarks>
	/// CreateFlag specifies the file create semantic. Users can combine flags like: <br />
	/// <code>
	/// EnumSet.of(CreateFlag.CREATE, CreateFlag.APPEND)
	/// <code>
	/// <p>
	/// Use the CreateFlag as follows:
	/// <ol>
	/// <li> CREATE - to create a file if it does not exist,
	/// else throw FileAlreadyExists.</li>
	/// <li> APPEND - to append to a file if it exists,
	/// else throw FileNotFoundException.</li>
	/// <li> OVERWRITE - to truncate a file if it exists,
	/// else throw FileNotFoundException.</li>
	/// <li> CREATE|APPEND - to create a file if it does not exist,
	/// else append to an existing file.</li>
	/// <li> CREATE|OVERWRITE - to create a file if it does not exist,
	/// else overwrite an existing file.</li>
	/// <li> SYNC_BLOCK - to force closed blocks to the disk device.
	/// In addition
	/// <see cref="Syncable.Hsync()"/>
	/// should be called after each write,
	/// if true synchronous behavior is required.</li>
	/// <li> LAZY_PERSIST - Create the block on transient storage (RAM) if
	/// available.</li>
	/// <li> APPEND_NEWBLOCK - Append data to a new block instead of end of the last
	/// partial block.</li>
	/// </ol>
	/// Following combination is not valid and will result in
	/// <see cref="Org.Apache.Hadoop.HadoopIllegalArgumentException"/>
	/// :
	/// <ol>
	/// <li> APPEND|OVERWRITE</li>
	/// <li> CREATE|APPEND|OVERWRITE</li>
	/// </ol>
	/// </remarks>
	[System.Serializable]
	public sealed class CreateFlag
	{
		/// <summary>Create a file.</summary>
		/// <remarks>
		/// Create a file. See javadoc for more description
		/// already exists
		/// </remarks>
		public static readonly Org.Apache.Hadoop.FS.CreateFlag Create = new Org.Apache.Hadoop.FS.CreateFlag
			((short)unchecked((int)(0x01)));

		/// <summary>Truncate/overwrite a file.</summary>
		/// <remarks>Truncate/overwrite a file. Same as POSIX O_TRUNC. See javadoc for description.
		/// 	</remarks>
		public static readonly Org.Apache.Hadoop.FS.CreateFlag Overwrite = new Org.Apache.Hadoop.FS.CreateFlag
			((short)unchecked((int)(0x02)));

		/// <summary>Append to a file.</summary>
		/// <remarks>Append to a file. See javadoc for more description.</remarks>
		public static readonly Org.Apache.Hadoop.FS.CreateFlag Append = new Org.Apache.Hadoop.FS.CreateFlag
			((short)unchecked((int)(0x04)));

		/// <summary>Force closed blocks to disk.</summary>
		/// <remarks>Force closed blocks to disk. Similar to POSIX O_SYNC. See javadoc for description.
		/// 	</remarks>
		public static readonly Org.Apache.Hadoop.FS.CreateFlag SyncBlock = new Org.Apache.Hadoop.FS.CreateFlag
			((short)unchecked((int)(0x08)));

		/// <summary>Create the block on transient storage (RAM) if available.</summary>
		/// <remarks>
		/// Create the block on transient storage (RAM) if available. If
		/// transient storage is unavailable then the block will be created
		/// on disk.
		/// HDFS will make a best effort to lazily write these files to persistent
		/// storage, however file contents may be lost at any time due to process/
		/// node restarts, hence there is no guarantee of data durability.
		/// This flag must only be used for intermediate data whose loss can be
		/// tolerated by the application.
		/// </remarks>
		public static readonly Org.Apache.Hadoop.FS.CreateFlag LazyPersist = new Org.Apache.Hadoop.FS.CreateFlag
			((short)unchecked((int)(0x10)));

		/// <summary>Append data to a new block instead of the end of the last partial block.
		/// 	</summary>
		/// <remarks>
		/// Append data to a new block instead of the end of the last partial block.
		/// This is only useful for APPEND.
		/// </remarks>
		public static readonly Org.Apache.Hadoop.FS.CreateFlag NewBlock = new Org.Apache.Hadoop.FS.CreateFlag
			((short)unchecked((int)(0x20)));

		private readonly short mode;

		private CreateFlag(short mode)
		{
			this.mode = mode;
		}

		internal short GetMode()
		{
			return Org.Apache.Hadoop.FS.CreateFlag.mode;
		}

		/// <summary>Validate the CreateFlag and throw exception if it is invalid</summary>
		/// <param name="flag">set of CreateFlag</param>
		/// <exception cref="Org.Apache.Hadoop.HadoopIllegalArgumentException">if the CreateFlag is invalid
		/// 	</exception>
		public static void Validate(EnumSet<Org.Apache.Hadoop.FS.CreateFlag> flag)
		{
			if (flag == null || flag.IsEmpty())
			{
				throw new HadoopIllegalArgumentException(flag + " does not specify any options");
			}
			bool append = flag.Contains(Org.Apache.Hadoop.FS.CreateFlag.Append);
			bool overwrite = flag.Contains(Org.Apache.Hadoop.FS.CreateFlag.Overwrite);
			// Both append and overwrite is an error
			if (append && overwrite)
			{
				throw new HadoopIllegalArgumentException(flag + "Both append and overwrite options cannot be enabled."
					);
			}
		}

		/// <summary>Validate the CreateFlag for create operation</summary>
		/// <param name="path">
		/// Object representing the path; usually String or
		/// <see cref="Path"/>
		/// </param>
		/// <param name="pathExists">pass true if the path exists in the file system</param>
		/// <param name="flag">set of CreateFlag</param>
		/// <exception cref="System.IO.IOException">on error</exception>
		/// <exception cref="Org.Apache.Hadoop.HadoopIllegalArgumentException">if the CreateFlag is invalid
		/// 	</exception>
		public static void Validate(object path, bool pathExists, EnumSet<Org.Apache.Hadoop.FS.CreateFlag
			> flag)
		{
			Validate(flag);
			bool append = flag.Contains(Org.Apache.Hadoop.FS.CreateFlag.Append);
			bool overwrite = flag.Contains(Org.Apache.Hadoop.FS.CreateFlag.Overwrite);
			if (pathExists)
			{
				if (!(append || overwrite))
				{
					throw new FileAlreadyExistsException("File already exists: " + path.ToString() + 
						". Append or overwrite option must be specified in " + flag);
				}
			}
			else
			{
				if (!flag.Contains(Org.Apache.Hadoop.FS.CreateFlag.Create))
				{
					throw new FileNotFoundException("Non existing file: " + path.ToString() + ". Create option is not specified in "
						 + flag);
				}
			}
		}

		/// <summary>Validate the CreateFlag for the append operation.</summary>
		/// <remarks>
		/// Validate the CreateFlag for the append operation. The flag must contain
		/// APPEND, and cannot contain OVERWRITE.
		/// </remarks>
		public static void ValidateForAppend(EnumSet<Org.Apache.Hadoop.FS.CreateFlag> flag
			)
		{
			Validate(flag);
			if (!flag.Contains(Org.Apache.Hadoop.FS.CreateFlag.Append))
			{
				throw new HadoopIllegalArgumentException(flag + " does not contain APPEND");
			}
		}
	}
}
