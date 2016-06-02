using System;
using System.IO;
using Com.Google.Common.Annotations;
using Hadoop.Common.Core.Fs;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.IO.Nativeio;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.IO
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
			bool shouldBeSecure = UserGroupInformation.IsSecurityEnabled();
			bool canBeSecure = NativeIO.IsAvailable();
			if (!canBeSecure && shouldBeSecure)
			{
				throw new RuntimeException("Secure IO is not possible without native code extensions."
					);
			}
			// Pre-cache an instance of the raw FileSystem since we sometimes
			// do secure IO in a shutdown hook, where this call could fail.
			try
			{
				rawFilesystem = FileSystem.GetLocal(new Configuration()).GetRaw();
			}
			catch (IOException)
			{
				throw new RuntimeException("Couldn't obtain an instance of RawLocalFileSystem.");
			}
			// SecureIO just skips security checks in the case that security is
			// disabled
			skipSecurity = !canBeSecure;
		}

		private static readonly bool skipSecurity;

		private static readonly FileSystem rawFilesystem;

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
		public static RandomAccessFile OpenForRandomRead(FilePath f, string mode, string 
			expectedOwner, string expectedGroup)
		{
			if (!UserGroupInformation.IsSecurityEnabled())
			{
				return new RandomAccessFile(f, mode);
			}
			return ForceSecureOpenForRandomRead(f, mode, expectedOwner, expectedGroup);
		}

		/// <summary>Same as openForRandomRead except that it will run even if security is off.
		/// 	</summary>
		/// <remarks>
		/// Same as openForRandomRead except that it will run even if security is off.
		/// This is used by unit tests.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		protected internal static RandomAccessFile ForceSecureOpenForRandomRead(FilePath 
			f, string mode, string expectedOwner, string expectedGroup)
		{
			RandomAccessFile raf = new RandomAccessFile(f, mode);
			bool success = false;
			try
			{
				NativeIO.POSIX.Stat stat = NativeIO.POSIX.GetFstat(raf.GetFD());
				CheckStat(f, stat.GetOwner(), stat.GetGroup(), expectedOwner, expectedGroup);
				success = true;
				return raf;
			}
			finally
			{
				if (!success)
				{
					raf.Close();
				}
			}
		}

		/// <summary>
		/// Opens the
		/// <see cref="Org.Apache.Hadoop.FS.FSDataInputStream"/>
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
		public static FSDataInputStream OpenFSDataInputStream(FilePath file, string expectedOwner
			, string expectedGroup)
		{
			if (!UserGroupInformation.IsSecurityEnabled())
			{
				return rawFilesystem.Open(new Path(file.GetAbsolutePath()));
			}
			return ForceSecureOpenFSDataInputStream(file, expectedOwner, expectedGroup);
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
		[VisibleForTesting]
		protected internal static FSDataInputStream ForceSecureOpenFSDataInputStream(FilePath
			 file, string expectedOwner, string expectedGroup)
		{
			FSDataInputStream @in = rawFilesystem.Open(new Path(file.GetAbsolutePath()));
			bool success = false;
			try
			{
				NativeIO.POSIX.Stat stat = NativeIO.POSIX.GetFstat(@in.GetFileDescriptor());
				CheckStat(file, stat.GetOwner(), stat.GetGroup(), expectedOwner, expectedGroup);
				success = true;
				return @in;
			}
			finally
			{
				if (!success)
				{
					@in.Close();
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
		public static FileInputStream OpenForRead(FilePath f, string expectedOwner, string
			 expectedGroup)
		{
			if (!UserGroupInformation.IsSecurityEnabled())
			{
				return new FileInputStream(f);
			}
			return ForceSecureOpenForRead(f, expectedOwner, expectedGroup);
		}

		/// <summary>Same as openForRead() except that it will run even if security is off.</summary>
		/// <remarks>
		/// Same as openForRead() except that it will run even if security is off.
		/// This is used by unit tests.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		protected internal static FileInputStream ForceSecureOpenForRead(FilePath f, string
			 expectedOwner, string expectedGroup)
		{
			FileInputStream fis = new FileInputStream(f);
			bool success = false;
			try
			{
				NativeIO.POSIX.Stat stat = NativeIO.POSIX.GetFstat(fis.GetFD());
				CheckStat(f, stat.GetOwner(), stat.GetGroup(), expectedOwner, expectedGroup);
				success = true;
				return fis;
			}
			finally
			{
				if (!success)
				{
					fis.Close();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static FileOutputStream InsecureCreateForWrite(FilePath f, int permissions
			)
		{
			// If we can't do real security, do a racy exists check followed by an
			// open and chmod
			if (f.Exists())
			{
				throw new SecureIOUtils.AlreadyExistsException("File " + f + " already exists");
			}
			FileOutputStream fos = new FileOutputStream(f);
			bool success = false;
			try
			{
				rawFilesystem.SetPermission(new Path(f.GetAbsolutePath()), new FsPermission((short
					)permissions));
				success = true;
				return fos;
			}
			finally
			{
				if (!success)
				{
					fos.Close();
				}
			}
		}

		/// <summary>Open the specified File for write access, ensuring that it does not exist.
		/// 	</summary>
		/// <param name="f">the file that we want to create</param>
		/// <param name="permissions">we want to have on the file (if security is enabled)</param>
		/// <exception cref="AlreadyExistsException">if the file already exists</exception>
		/// <exception cref="System.IO.IOException">if any other error occurred</exception>
		public static FileOutputStream CreateForWrite(FilePath f, int permissions)
		{
			if (skipSecurity)
			{
				return InsecureCreateForWrite(f, permissions);
			}
			else
			{
				return NativeIO.GetCreateForWriteFileOutputStream(f, permissions);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void CheckStat(FilePath f, string owner, string group, string expectedOwner
			, string expectedGroup)
		{
			bool success = true;
			if (expectedOwner != null && !expectedOwner.Equals(owner))
			{
				if (Path.Windows)
				{
					UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser(expectedOwner);
					string adminsGroupString = "Administrators";
					success = owner.Equals(adminsGroupString) && Arrays.AsList(ugi.GetGroupNames()).Contains
						(adminsGroupString);
				}
				else
				{
					success = false;
				}
			}
			if (!success)
			{
				throw new IOException("Owner '" + owner + "' for path " + f + " did not match " +
					 "expected owner '" + expectedOwner + "'");
			}
		}

		/// <summary>
		/// Signals that an attempt to create a file at a given pathname has failed
		/// because another file already existed at that path.
		/// </summary>
		[System.Serializable]
		public class AlreadyExistsException : IOException
		{
			private const long serialVersionUID = 1L;

			public AlreadyExistsException(string msg)
				: base(msg)
			{
			}

			public AlreadyExistsException(Exception cause)
				: base(cause)
			{
			}
		}
	}
}
