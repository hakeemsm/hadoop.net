using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>
	/// This class provides secure APIs for opening and creating files on the local
	/// disk.
	/// </summary>
	/// <remarks>
	/// This class provides secure APIs for opening and creating files on the local
	/// disk. The main issue this class tries to handle is that of symlink traversal.
	/// <br/>
	/// An example of such an attack is:
	/// <ol>
	/// <li> Malicious user removes his task's syslog file, and puts a link to the
	/// jobToken file of a target user.</li>
	/// <li> Malicious user tries to open the syslog file via the servlet on the
	/// tasktracker.</li>
	/// <li> The tasktracker is unaware of the symlink, and simply streams the contents
	/// of the jobToken file. The malicious user can now access potentially sensitive
	/// map outputs, etc. of the target user's job.</li>
	/// </ol>
	/// A similar attack is possible involving task log truncation, but in that case
	/// due to an insecure write to a file.
	/// <br/>
	/// </remarks>
	public class SecureIOUtils
	{
		static SecureIOUtils()
		{
			bool shouldBeSecure = org.apache.hadoop.security.UserGroupInformation.isSecurityEnabled
				();
			bool canBeSecure = org.apache.hadoop.io.nativeio.NativeIO.isAvailable();
			if (!canBeSecure && shouldBeSecure)
			{
				throw new System.Exception("Secure IO is not possible without native code extensions."
					);
			}
			// Pre-cache an instance of the raw FileSystem since we sometimes
			// do secure IO in a shutdown hook, where this call could fail.
			try
			{
				rawFilesystem = org.apache.hadoop.fs.FileSystem.getLocal(new org.apache.hadoop.conf.Configuration
					()).getRaw();
			}
			catch (System.IO.IOException)
			{
				throw new System.Exception("Couldn't obtain an instance of RawLocalFileSystem.");
			}
			// SecureIO just skips security checks in the case that security is
			// disabled
			skipSecurity = !canBeSecure;
		}

		private static readonly bool skipSecurity;

		private static readonly org.apache.hadoop.fs.FileSystem rawFilesystem;

		/// <summary>
		/// Open the given File for random read access, verifying the expected user/
		/// group constraints if security is enabled.
		/// </summary>
		/// <remarks>
		/// Open the given File for random read access, verifying the expected user/
		/// group constraints if security is enabled.
		/// Note that this function provides no additional security checks if hadoop
		/// security is disabled, since doing the checks would be too expensive when
		/// native libraries are not available.
		/// </remarks>
		/// <param name="f">file that we are trying to open</param>
		/// <param name="mode">mode in which we want to open the random access file</param>
		/// <param name="expectedOwner">the expected user owner for the file</param>
		/// <param name="expectedGroup">the expected group owner for the file</param>
		/// <exception cref="System.IO.IOException">
		/// if an IO error occurred or if the user/group does
		/// not match when security is enabled.
		/// </exception>
		public static java.io.RandomAccessFile openForRandomRead(java.io.File f, string mode
			, string expectedOwner, string expectedGroup)
		{
			if (!org.apache.hadoop.security.UserGroupInformation.isSecurityEnabled())
			{
				return new java.io.RandomAccessFile(f, mode);
			}
			return forceSecureOpenForRandomRead(f, mode, expectedOwner, expectedGroup);
		}

		/// <summary>Same as openForRandomRead except that it will run even if security is off.
		/// 	</summary>
		/// <remarks>
		/// Same as openForRandomRead except that it will run even if security is off.
		/// This is used by unit tests.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[com.google.common.annotations.VisibleForTesting]
		protected internal static java.io.RandomAccessFile forceSecureOpenForRandomRead(java.io.File
			 f, string mode, string expectedOwner, string expectedGroup)
		{
			java.io.RandomAccessFile raf = new java.io.RandomAccessFile(f, mode);
			bool success = false;
			try
			{
				org.apache.hadoop.io.nativeio.NativeIO.POSIX.Stat stat = org.apache.hadoop.io.nativeio.NativeIO.POSIX
					.getFstat(raf.getFD());
				checkStat(f, stat.getOwner(), stat.getGroup(), expectedOwner, expectedGroup);
				success = true;
				return raf;
			}
			finally
			{
				if (!success)
				{
					raf.close();
				}
			}
		}

		/// <summary>
		/// Opens the
		/// <see cref="org.apache.hadoop.fs.FSDataInputStream"/>
		/// on the requested file on local file
		/// system, verifying the expected user/group constraints if security is
		/// enabled.
		/// </summary>
		/// <param name="file">absolute path of the file</param>
		/// <param name="expectedOwner">the expected user owner for the file</param>
		/// <param name="expectedGroup">the expected group owner for the file</param>
		/// <exception cref="System.IO.IOException">
		/// if an IO Error occurred or the user/group does not
		/// match if security is enabled
		/// </exception>
		public static org.apache.hadoop.fs.FSDataInputStream openFSDataInputStream(java.io.File
			 file, string expectedOwner, string expectedGroup)
		{
			if (!org.apache.hadoop.security.UserGroupInformation.isSecurityEnabled())
			{
				return rawFilesystem.open(new org.apache.hadoop.fs.Path(file.getAbsolutePath()));
			}
			return forceSecureOpenFSDataInputStream(file, expectedOwner, expectedGroup);
		}

		/// <summary>
		/// Same as openFSDataInputStream except that it will run even if security is
		/// off.
		/// </summary>
		/// <remarks>
		/// Same as openFSDataInputStream except that it will run even if security is
		/// off. This is used by unit tests.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[com.google.common.annotations.VisibleForTesting]
		protected internal static org.apache.hadoop.fs.FSDataInputStream forceSecureOpenFSDataInputStream
			(java.io.File file, string expectedOwner, string expectedGroup)
		{
			org.apache.hadoop.fs.FSDataInputStream @in = rawFilesystem.open(new org.apache.hadoop.fs.Path
				(file.getAbsolutePath()));
			bool success = false;
			try
			{
				org.apache.hadoop.io.nativeio.NativeIO.POSIX.Stat stat = org.apache.hadoop.io.nativeio.NativeIO.POSIX
					.getFstat(@in.getFileDescriptor());
				checkStat(file, stat.getOwner(), stat.getGroup(), expectedOwner, expectedGroup);
				success = true;
				return @in;
			}
			finally
			{
				if (!success)
				{
					@in.close();
				}
			}
		}

		/// <summary>
		/// Open the given File for read access, verifying the expected user/group
		/// constraints if security is enabled.
		/// </summary>
		/// <remarks>
		/// Open the given File for read access, verifying the expected user/group
		/// constraints if security is enabled.
		/// Note that this function provides no additional checks if Hadoop
		/// security is disabled, since doing the checks would be too expensive
		/// when native libraries are not available.
		/// </remarks>
		/// <param name="f">the file that we are trying to open</param>
		/// <param name="expectedOwner">the expected user owner for the file</param>
		/// <param name="expectedGroup">the expected group owner for the file</param>
		/// <exception cref="System.IO.IOException">
		/// if an IO Error occurred, or security is enabled and
		/// the user/group does not match
		/// </exception>
		public static java.io.FileInputStream openForRead(java.io.File f, string expectedOwner
			, string expectedGroup)
		{
			if (!org.apache.hadoop.security.UserGroupInformation.isSecurityEnabled())
			{
				return new java.io.FileInputStream(f);
			}
			return forceSecureOpenForRead(f, expectedOwner, expectedGroup);
		}

		/// <summary>Same as openForRead() except that it will run even if security is off.</summary>
		/// <remarks>
		/// Same as openForRead() except that it will run even if security is off.
		/// This is used by unit tests.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[com.google.common.annotations.VisibleForTesting]
		protected internal static java.io.FileInputStream forceSecureOpenForRead(java.io.File
			 f, string expectedOwner, string expectedGroup)
		{
			java.io.FileInputStream fis = new java.io.FileInputStream(f);
			bool success = false;
			try
			{
				org.apache.hadoop.io.nativeio.NativeIO.POSIX.Stat stat = org.apache.hadoop.io.nativeio.NativeIO.POSIX
					.getFstat(fis.getFD());
				checkStat(f, stat.getOwner(), stat.getGroup(), expectedOwner, expectedGroup);
				success = true;
				return fis;
			}
			finally
			{
				if (!success)
				{
					fis.close();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static java.io.FileOutputStream insecureCreateForWrite(java.io.File f, int
			 permissions)
		{
			// If we can't do real security, do a racy exists check followed by an
			// open and chmod
			if (f.exists())
			{
				throw new org.apache.hadoop.io.SecureIOUtils.AlreadyExistsException("File " + f +
					 " already exists");
			}
			java.io.FileOutputStream fos = new java.io.FileOutputStream(f);
			bool success = false;
			try
			{
				rawFilesystem.setPermission(new org.apache.hadoop.fs.Path(f.getAbsolutePath()), new 
					org.apache.hadoop.fs.permission.FsPermission((short)permissions));
				success = true;
				return fos;
			}
			finally
			{
				if (!success)
				{
					fos.close();
				}
			}
		}

		/// <summary>Open the specified File for write access, ensuring that it does not exist.
		/// 	</summary>
		/// <param name="f">the file that we want to create</param>
		/// <param name="permissions">we want to have on the file (if security is enabled)</param>
		/// <exception cref="AlreadyExistsException">if the file already exists</exception>
		/// <exception cref="System.IO.IOException">if any other error occurred</exception>
		public static java.io.FileOutputStream createForWrite(java.io.File f, int permissions
			)
		{
			if (skipSecurity)
			{
				return insecureCreateForWrite(f, permissions);
			}
			else
			{
				return org.apache.hadoop.io.nativeio.NativeIO.getCreateForWriteFileOutputStream(f
					, permissions);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void checkStat(java.io.File f, string owner, string group, string 
			expectedOwner, string expectedGroup)
		{
			bool success = true;
			if (expectedOwner != null && !expectedOwner.Equals(owner))
			{
				if (org.apache.hadoop.fs.Path.WINDOWS)
				{
					org.apache.hadoop.security.UserGroupInformation ugi = org.apache.hadoop.security.UserGroupInformation
						.createRemoteUser(expectedOwner);
					string adminsGroupString = "Administrators";
					success = owner.Equals(adminsGroupString) && java.util.Arrays.asList(ugi.getGroupNames
						()).contains(adminsGroupString);
				}
				else
				{
					success = false;
				}
			}
			if (!success)
			{
				throw new System.IO.IOException("Owner '" + owner + "' for path " + f + " did not match "
					 + "expected owner '" + expectedOwner + "'");
			}
		}

		/// <summary>
		/// Signals that an attempt to create a file at a given pathname has failed
		/// because another file already existed at that path.
		/// </summary>
		[System.Serializable]
		public class AlreadyExistsException : System.IO.IOException
		{
			private const long serialVersionUID = 1L;

			public AlreadyExistsException(string msg)
				: base(msg)
			{
			}

			public AlreadyExistsException(System.Exception cause)
				: base(cause)
			{
			}
		}
	}
}
