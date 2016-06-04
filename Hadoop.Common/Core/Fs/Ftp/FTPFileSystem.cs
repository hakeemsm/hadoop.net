using System.IO;
using Com.Google.Common.Base;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.Fs;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Net.Ftp;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Ftp
{
	/// <summary>
	/// <p>
	/// A
	/// <see cref="FileSystem"/>
	/// backed by an FTP client provided by &lt;a
	/// href="http://commons.apache.org/net/"&gt;Apache Commons Net</a>.
	/// </p>
	/// </summary>
	public class FTPFileSystem : FileSystem
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(FTPFileSystem));

		public const int DefaultBufferSize = 1024 * 1024;

		public const int DefaultBlockSize = 4 * 1024;

		public const string FsFtpUserPrefix = "fs.ftp.user.";

		public const string FsFtpHost = "fs.ftp.host";

		public const string FsFtpHostPort = "fs.ftp.host.port";

		public const string FsFtpPasswordPrefix = "fs.ftp.password.";

		public const string ESameDirectoryOnly = "only same directory renames are supported";

		private URI uri;

		/// <summary>Return the protocol scheme for the FileSystem.</summary>
		/// <remarks>
		/// Return the protocol scheme for the FileSystem.
		/// <p/>
		/// </remarks>
		/// <returns><code>ftp</code></returns>
		public override string GetScheme()
		{
			return "ftp";
		}

		/// <summary>Get the default port for this FTPFileSystem.</summary>
		/// <returns>the default port</returns>
		protected internal override int GetDefaultPort()
		{
			return FTP.DefaultPort;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Initialize(URI uri, Configuration conf)
		{
			// get
			base.Initialize(uri, conf);
			// get host information from uri (overrides info in conf)
			string host = uri.GetHost();
			host = (host == null) ? conf.Get(FsFtpHost, null) : host;
			if (host == null)
			{
				throw new IOException("Invalid host specified");
			}
			conf.Set(FsFtpHost, host);
			// get port information from uri, (overrides info in conf)
			int port = uri.GetPort();
			port = (port == -1) ? FTP.DefaultPort : port;
			conf.SetInt("fs.ftp.host.port", port);
			// get user/password information from URI (overrides info in conf)
			string userAndPassword = uri.GetUserInfo();
			if (userAndPassword == null)
			{
				userAndPassword = (conf.Get("fs.ftp.user." + host, null) + ":" + conf.Get("fs.ftp.password."
					 + host, null));
			}
			string[] userPasswdInfo = userAndPassword.Split(":");
			Preconditions.CheckState(userPasswdInfo.Length > 1, "Invalid username / password"
				);
			conf.Set(FsFtpUserPrefix + host, userPasswdInfo[0]);
			conf.Set(FsFtpPasswordPrefix + host, userPasswdInfo[1]);
			SetConf(conf);
			this.uri = uri;
		}

		/// <summary>Connect to the FTP server using configuration parameters</summary>
		/// <returns>An FTPClient instance</returns>
		/// <exception cref="System.IO.IOException"/>
		private FTPClient Connect()
		{
			FTPClient client = null;
			Configuration conf = GetConf();
			string host = conf.Get(FsFtpHost);
			int port = conf.GetInt(FsFtpHostPort, FTP.DefaultPort);
			string user = conf.Get(FsFtpUserPrefix + host);
			string password = conf.Get(FsFtpPasswordPrefix + host);
			client = new FTPClient();
			client.Connect(host, port);
			int reply = client.GetReplyCode();
			if (!FTPReply.IsPositiveCompletion(reply))
			{
				throw NetUtils.WrapException(host, port, NetUtils.UnknownHost, 0, new ConnectException
					("Server response " + reply));
			}
			else
			{
				if (client.Login(user, password))
				{
					client.SetFileTransferMode(FTP.BlockTransferMode);
					client.SetFileType(FTP.BinaryFileType);
					client.SetBufferSize(DefaultBufferSize);
				}
				else
				{
					throw new IOException("Login failed on server - " + host + ", port - " + port + " as user '"
						 + user + "'");
				}
			}
			return client;
		}

		/// <summary>Logout and disconnect the given FTPClient.</summary>
		/// <param name="client"/>
		/// <exception cref="System.IO.IOException"/>
		private void Disconnect(FTPClient client)
		{
			if (client != null)
			{
				if (!client.IsConnected())
				{
					throw new FTPException("Client not connected");
				}
				bool logoutSuccess = client.Logout();
				client.Disconnect();
				if (!logoutSuccess)
				{
					Log.Warn("Logout failed while disconnecting, error code - " + client.GetReplyCode
						());
				}
			}
		}

		/// <summary>Resolve against given working directory.</summary>
		/// <param name="workDir"/>
		/// <param name="path"/>
		/// <returns/>
		private Path MakeAbsolute(Path workDir, Path path)
		{
			if (path.IsAbsolute())
			{
				return path;
			}
			return new Path(workDir, path);
		}

		/// <exception cref="System.IO.IOException"/>
		public override FSDataInputStream Open(Path file, int bufferSize)
		{
			FTPClient client = Connect();
			Path workDir = new Path(client.PrintWorkingDirectory());
			Path absolute = MakeAbsolute(workDir, file);
			FileStatus fileStat = GetFileStatus(client, absolute);
			if (fileStat.IsDirectory())
			{
				Disconnect(client);
				throw new FileNotFoundException("Path " + file + " is a directory.");
			}
			client.Allocate(bufferSize);
			Path parent = absolute.GetParent();
			// Change to parent directory on the
			// server. Only then can we read the
			// file
			// on the server by opening up an InputStream. As a side effect the working
			// directory on the server is changed to the parent directory of the file.
			// The FTP client connection is closed when close() is called on the
			// FSDataInputStream.
			client.ChangeWorkingDirectory(parent.ToUri().GetPath());
			InputStream @is = client.RetrieveFileStream(file.GetName());
			FSDataInputStream fis = new FSDataInputStream(new FTPInputStream(@is, client, statistics
				));
			if (!FTPReply.IsPositivePreliminary(client.GetReplyCode()))
			{
				// The ftpClient is an inconsistent state. Must close the stream
				// which in turn will logout and disconnect from FTP server
				fis.Close();
				throw new IOException("Unable to open file: " + file + ", Aborting");
			}
			return fis;
		}

		/// <summary>
		/// A stream obtained via this call must be closed before using other APIs of
		/// this class or else the invocation will block.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public override FSDataOutputStream Create(Path file, FsPermission permission, bool
			 overwrite, int bufferSize, short replication, long blockSize, Progressable progress
			)
		{
			FTPClient client = Connect();
			Path workDir = new Path(client.PrintWorkingDirectory());
			Path absolute = MakeAbsolute(workDir, file);
			FileStatus status;
			try
			{
				status = GetFileStatus(client, file);
			}
			catch (FileNotFoundException)
			{
				status = null;
			}
			if (status != null)
			{
				if (overwrite && !status.IsDirectory())
				{
					Delete(client, file, false);
				}
				else
				{
					Disconnect(client);
					throw new FileAlreadyExistsException("File already exists: " + file);
				}
			}
			Path parent = absolute.GetParent();
			if (parent == null || !Mkdirs(client, parent, FsPermission.GetDirDefault()))
			{
				parent = (parent == null) ? new Path("/") : parent;
				Disconnect(client);
				throw new IOException("create(): Mkdirs failed to create: " + parent);
			}
			client.Allocate(bufferSize);
			// Change to parent directory on the server. Only then can we write to the
			// file on the server by opening up an OutputStream. As a side effect the
			// working directory on the server is changed to the parent directory of the
			// file. The FTP client connection is closed when close() is called on the
			// FSDataOutputStream.
			client.ChangeWorkingDirectory(parent.ToUri().GetPath());
			FSDataOutputStream fos = new _FSDataOutputStream_263(this, client, client.StoreFileStream
				(file.GetName()), statistics);
			if (!FTPReply.IsPositivePreliminary(client.GetReplyCode()))
			{
				// The ftpClient is an inconsistent state. Must close the stream
				// which in turn will logout and disconnect from FTP server
				fos.Close();
				throw new IOException("Unable to create file: " + file + ", Aborting");
			}
			return fos;
		}

		private sealed class _FSDataOutputStream_263 : FSDataOutputStream
		{
			public _FSDataOutputStream_263(FTPFileSystem _enclosing, FTPClient client, OutputStream
				 baseArg1, FileSystem.Statistics baseArg2)
				: base(baseArg1, baseArg2)
			{
				this._enclosing = _enclosing;
				this.client = client;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				base.Close();
				if (!client.IsConnected())
				{
					throw new FTPException("Client not connected");
				}
				bool cmdCompleted = client.CompletePendingCommand();
				this._enclosing.Disconnect(client);
				if (!cmdCompleted)
				{
					throw new FTPException("Could not complete transfer, Reply Code - " + client.GetReplyCode
						());
				}
			}

			private readonly FTPFileSystem _enclosing;

			private readonly FTPClient client;
		}

		/// <summary>This optional operation is not yet supported.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override FSDataOutputStream Append(Path f, int bufferSize, Progressable progress
			)
		{
			throw new IOException("Not supported");
		}

		/// <summary>
		/// Convenience method, so that we don't open a new connection when using this
		/// method from within another method.
		/// </summary>
		/// <remarks>
		/// Convenience method, so that we don't open a new connection when using this
		/// method from within another method. Otherwise every API invocation incurs
		/// the overhead of opening/closing a TCP connection.
		/// </remarks>
		/// <exception cref="System.IO.IOException">on IO problems other than FileNotFoundException
		/// 	</exception>
		private bool Exists(FTPClient client, Path file)
		{
			try
			{
				GetFileStatus(client, file);
				return true;
			}
			catch (FileNotFoundException)
			{
				return false;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Delete(Path file, bool recursive)
		{
			FTPClient client = Connect();
			try
			{
				bool success = Delete(client, file, recursive);
				return success;
			}
			finally
			{
				Disconnect(client);
			}
		}

		/// <summary>
		/// Convenience method, so that we don't open a new connection when using this
		/// method from within another method.
		/// </summary>
		/// <remarks>
		/// Convenience method, so that we don't open a new connection when using this
		/// method from within another method. Otherwise every API invocation incurs
		/// the overhead of opening/closing a TCP connection.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private bool Delete(FTPClient client, Path file, bool recursive)
		{
			Path workDir = new Path(client.PrintWorkingDirectory());
			Path absolute = MakeAbsolute(workDir, file);
			string pathName = absolute.ToUri().GetPath();
			try
			{
				FileStatus fileStat = GetFileStatus(client, absolute);
				if (fileStat.IsFile())
				{
					return client.DeleteFile(pathName);
				}
			}
			catch (FileNotFoundException)
			{
				//the file is not there
				return false;
			}
			FileStatus[] dirEntries = ListStatus(client, absolute);
			if (dirEntries != null && dirEntries.Length > 0 && !(recursive))
			{
				throw new IOException("Directory: " + file + " is not empty.");
			}
			foreach (FileStatus dirEntry in dirEntries)
			{
				Delete(client, new Path(absolute, dirEntry.GetPath()), recursive);
			}
			return client.RemoveDirectory(pathName);
		}

		private FsAction GetFsAction(int accessGroup, FTPFile ftpFile)
		{
			FsAction action = FsAction.None;
			if (ftpFile.HasPermission(accessGroup, FTPFile.ReadPermission))
			{
				action.Or(FsAction.Read);
			}
			if (ftpFile.HasPermission(accessGroup, FTPFile.WritePermission))
			{
				action.Or(FsAction.Write);
			}
			if (ftpFile.HasPermission(accessGroup, FTPFile.ExecutePermission))
			{
				action.Or(FsAction.Execute);
			}
			return action;
		}

		private FsPermission GetPermissions(FTPFile ftpFile)
		{
			FsAction user;
			FsAction group;
			FsAction others;
			user = GetFsAction(FTPFile.UserAccess, ftpFile);
			group = GetFsAction(FTPFile.GroupAccess, ftpFile);
			others = GetFsAction(FTPFile.WorldAccess, ftpFile);
			return new FsPermission(user, group, others);
		}

		public override URI GetUri()
		{
			return uri;
		}

		/// <exception cref="System.IO.IOException"/>
		public override FileStatus[] ListStatus(Path file)
		{
			FTPClient client = Connect();
			try
			{
				FileStatus[] stats = ListStatus(client, file);
				return stats;
			}
			finally
			{
				Disconnect(client);
			}
		}

		/// <summary>
		/// Convenience method, so that we don't open a new connection when using this
		/// method from within another method.
		/// </summary>
		/// <remarks>
		/// Convenience method, so that we don't open a new connection when using this
		/// method from within another method. Otherwise every API invocation incurs
		/// the overhead of opening/closing a TCP connection.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private FileStatus[] ListStatus(FTPClient client, Path file)
		{
			Path workDir = new Path(client.PrintWorkingDirectory());
			Path absolute = MakeAbsolute(workDir, file);
			FileStatus fileStat = GetFileStatus(client, absolute);
			if (fileStat.IsFile())
			{
				return new FileStatus[] { fileStat };
			}
			FTPFile[] ftpFiles = client.ListFiles(absolute.ToUri().GetPath());
			FileStatus[] fileStats = new FileStatus[ftpFiles.Length];
			for (int i = 0; i < ftpFiles.Length; i++)
			{
				fileStats[i] = GetFileStatus(ftpFiles[i], absolute);
			}
			return fileStats;
		}

		/// <exception cref="System.IO.IOException"/>
		public override FileStatus GetFileStatus(Path file)
		{
			FTPClient client = Connect();
			try
			{
				FileStatus status = GetFileStatus(client, file);
				return status;
			}
			finally
			{
				Disconnect(client);
			}
		}

		/// <summary>
		/// Convenience method, so that we don't open a new connection when using this
		/// method from within another method.
		/// </summary>
		/// <remarks>
		/// Convenience method, so that we don't open a new connection when using this
		/// method from within another method. Otherwise every API invocation incurs
		/// the overhead of opening/closing a TCP connection.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private FileStatus GetFileStatus(FTPClient client, Path file)
		{
			FileStatus fileStat = null;
			Path workDir = new Path(client.PrintWorkingDirectory());
			Path absolute = MakeAbsolute(workDir, file);
			Path parentPath = absolute.GetParent();
			if (parentPath == null)
			{
				// root dir
				long length = -1;
				// Length of root dir on server not known
				bool isDir = true;
				int blockReplication = 1;
				long blockSize = DefaultBlockSize;
				// Block Size not known.
				long modTime = -1;
				// Modification time of root dir not known.
				Path root = new Path("/");
				return new FileStatus(length, isDir, blockReplication, blockSize, modTime, root.MakeQualified
					(this));
			}
			string pathName = parentPath.ToUri().GetPath();
			FTPFile[] ftpFiles = client.ListFiles(pathName);
			if (ftpFiles != null)
			{
				foreach (FTPFile ftpFile in ftpFiles)
				{
					if (ftpFile.GetName().Equals(file.GetName()))
					{
						// file found in dir
						fileStat = GetFileStatus(ftpFile, parentPath);
						break;
					}
				}
				if (fileStat == null)
				{
					throw new FileNotFoundException("File " + file + " does not exist.");
				}
			}
			else
			{
				throw new FileNotFoundException("File " + file + " does not exist.");
			}
			return fileStat;
		}

		/// <summary>
		/// Convert the file information in FTPFile to a
		/// <see cref="Org.Apache.Hadoop.FS.FileStatus"/>
		/// object.
		/// </summary>
		/// <param name="ftpFile"/>
		/// <param name="parentPath"/>
		/// <returns>FileStatus</returns>
		private FileStatus GetFileStatus(FTPFile ftpFile, Path parentPath)
		{
			long length = ftpFile.GetSize();
			bool isDir = ftpFile.IsDirectory();
			int blockReplication = 1;
			// Using default block size since there is no way in FTP client to know of
			// block sizes on server. The assumption could be less than ideal.
			long blockSize = DefaultBlockSize;
			long modTime = ftpFile.GetTimestamp().GetTimeInMillis();
			long accessTime = 0;
			FsPermission permission = GetPermissions(ftpFile);
			string user = ftpFile.GetUser();
			string group = ftpFile.GetGroup();
			Path filePath = new Path(parentPath, ftpFile.GetName());
			return new FileStatus(length, isDir, blockReplication, blockSize, modTime, accessTime
				, permission, user, group, filePath.MakeQualified(this));
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Mkdirs(Path file, FsPermission permission)
		{
			FTPClient client = Connect();
			try
			{
				bool success = Mkdirs(client, file, permission);
				return success;
			}
			finally
			{
				Disconnect(client);
			}
		}

		/// <summary>
		/// Convenience method, so that we don't open a new connection when using this
		/// method from within another method.
		/// </summary>
		/// <remarks>
		/// Convenience method, so that we don't open a new connection when using this
		/// method from within another method. Otherwise every API invocation incurs
		/// the overhead of opening/closing a TCP connection.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private bool Mkdirs(FTPClient client, Path file, FsPermission permission)
		{
			bool created = true;
			Path workDir = new Path(client.PrintWorkingDirectory());
			Path absolute = MakeAbsolute(workDir, file);
			string pathName = absolute.GetName();
			if (!Exists(client, absolute))
			{
				Path parent = absolute.GetParent();
				created = (parent == null || Mkdirs(client, parent, FsPermission.GetDirDefault())
					);
				if (created)
				{
					string parentDir = parent.ToUri().GetPath();
					client.ChangeWorkingDirectory(parentDir);
					created = created && client.MakeDirectory(pathName);
				}
			}
			else
			{
				if (IsFile(client, absolute))
				{
					throw new ParentNotDirectoryException(string.Format("Can't make directory for path %s since it is a file."
						, absolute));
				}
			}
			return created;
		}

		/// <summary>
		/// Convenience method, so that we don't open a new connection when using this
		/// method from within another method.
		/// </summary>
		/// <remarks>
		/// Convenience method, so that we don't open a new connection when using this
		/// method from within another method. Otherwise every API invocation incurs
		/// the overhead of opening/closing a TCP connection.
		/// </remarks>
		private bool IsFile(FTPClient client, Path file)
		{
			try
			{
				return GetFileStatus(client, file).IsFile();
			}
			catch (FileNotFoundException)
			{
				return false;
			}
			catch (IOException ioe)
			{
				// file does not exist
				throw new FTPException("File check failed", ioe);
			}
		}

		/*
		* Assuming that parent of both source and destination is the same. Is the
		* assumption correct or it is suppose to work like 'move' ?
		*/
		/// <exception cref="System.IO.IOException"/>
		public override bool Rename(Path src, Path dst)
		{
			FTPClient client = Connect();
			try
			{
				bool success = Rename(client, src, dst);
				return success;
			}
			finally
			{
				Disconnect(client);
			}
		}

		/// <summary>Probe for a path being a parent of another</summary>
		/// <param name="parent">parent path</param>
		/// <param name="child">possible child path</param>
		/// <returns>true if the parent's path matches the start of the child's</returns>
		private bool IsParentOf(Path parent, Path child)
		{
			URI parentURI = parent.ToUri();
			string parentPath = parentURI.GetPath();
			if (!parentPath.EndsWith("/"))
			{
				parentPath += "/";
			}
			URI childURI = child.ToUri();
			string childPath = childURI.GetPath();
			return childPath.StartsWith(parentPath);
		}

		/// <summary>
		/// Convenience method, so that we don't open a new connection when using this
		/// method from within another method.
		/// </summary>
		/// <remarks>
		/// Convenience method, so that we don't open a new connection when using this
		/// method from within another method. Otherwise every API invocation incurs
		/// the overhead of opening/closing a TCP connection.
		/// </remarks>
		/// <param name="client"/>
		/// <param name="src"/>
		/// <param name="dst"/>
		/// <returns/>
		/// <exception cref="System.IO.IOException"/>
		private bool Rename(FTPClient client, Path src, Path dst)
		{
			Path workDir = new Path(client.PrintWorkingDirectory());
			Path absoluteSrc = MakeAbsolute(workDir, src);
			Path absoluteDst = MakeAbsolute(workDir, dst);
			if (!Exists(client, absoluteSrc))
			{
				throw new FileNotFoundException("Source path " + src + " does not exist");
			}
			if (IsDirectory(absoluteDst))
			{
				// destination is a directory: rename goes underneath it with the
				// source name
				absoluteDst = new Path(absoluteDst, absoluteSrc.GetName());
			}
			if (Exists(client, absoluteDst))
			{
				throw new FileAlreadyExistsException("Destination path " + dst + " already exists"
					);
			}
			string parentSrc = absoluteSrc.GetParent().ToUri().ToString();
			string parentDst = absoluteDst.GetParent().ToUri().ToString();
			if (IsParentOf(absoluteSrc, absoluteDst))
			{
				throw new IOException("Cannot rename " + absoluteSrc + " under itself" + " : " + 
					absoluteDst);
			}
			if (!parentSrc.Equals(parentDst))
			{
				throw new IOException("Cannot rename source: " + absoluteSrc + " to " + absoluteDst
					 + " -" + ESameDirectoryOnly);
			}
			string from = absoluteSrc.GetName();
			string to = absoluteDst.GetName();
			client.ChangeWorkingDirectory(parentSrc);
			bool renamed = client.Rename(from, to);
			return renamed;
		}

		public override Path GetWorkingDirectory()
		{
			// Return home directory always since we do not maintain state.
			return GetHomeDirectory();
		}

		public override Path GetHomeDirectory()
		{
			FTPClient client = null;
			try
			{
				client = Connect();
				Path homeDir = new Path(client.PrintWorkingDirectory());
				return homeDir;
			}
			catch (IOException ioe)
			{
				throw new FTPException("Failed to get home directory", ioe);
			}
			finally
			{
				try
				{
					Disconnect(client);
				}
				catch (IOException ioe)
				{
					throw new FTPException("Failed to disconnect", ioe);
				}
			}
		}

		public override void SetWorkingDirectory(Path newDir)
		{
		}
		// we do not maintain the working directory state
	}
}
