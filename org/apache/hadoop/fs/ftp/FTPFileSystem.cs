using Sharpen;

namespace org.apache.hadoop.fs.ftp
{
	/// <summary>
	/// <p>
	/// A
	/// <see cref="org.apache.hadoop.fs.FileSystem"/>
	/// backed by an FTP client provided by &lt;a
	/// href="http://commons.apache.org/net/"&gt;Apache Commons Net</a>.
	/// </p>
	/// </summary>
	public class FTPFileSystem : org.apache.hadoop.fs.FileSystem
	{
		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.ftp.FTPFileSystem
			)));

		public const int DEFAULT_BUFFER_SIZE = 1024 * 1024;

		public const int DEFAULT_BLOCK_SIZE = 4 * 1024;

		public const string FS_FTP_USER_PREFIX = "fs.ftp.user.";

		public const string FS_FTP_HOST = "fs.ftp.host";

		public const string FS_FTP_HOST_PORT = "fs.ftp.host.port";

		public const string FS_FTP_PASSWORD_PREFIX = "fs.ftp.password.";

		public const string E_SAME_DIRECTORY_ONLY = "only same directory renames are supported";

		private java.net.URI uri;

		/// <summary>Return the protocol scheme for the FileSystem.</summary>
		/// <remarks>
		/// Return the protocol scheme for the FileSystem.
		/// <p/>
		/// </remarks>
		/// <returns><code>ftp</code></returns>
		public override string getScheme()
		{
			return "ftp";
		}

		/// <summary>Get the default port for this FTPFileSystem.</summary>
		/// <returns>the default port</returns>
		protected internal override int getDefaultPort()
		{
			return org.apache.commons.net.ftp.FTP.DEFAULT_PORT;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void initialize(java.net.URI uri, org.apache.hadoop.conf.Configuration
			 conf)
		{
			// get
			base.initialize(uri, conf);
			// get host information from uri (overrides info in conf)
			string host = uri.getHost();
			host = (host == null) ? conf.get(FS_FTP_HOST, null) : host;
			if (host == null)
			{
				throw new System.IO.IOException("Invalid host specified");
			}
			conf.set(FS_FTP_HOST, host);
			// get port information from uri, (overrides info in conf)
			int port = uri.getPort();
			port = (port == -1) ? org.apache.commons.net.ftp.FTP.DEFAULT_PORT : port;
			conf.setInt("fs.ftp.host.port", port);
			// get user/password information from URI (overrides info in conf)
			string userAndPassword = uri.getUserInfo();
			if (userAndPassword == null)
			{
				userAndPassword = (conf.get("fs.ftp.user." + host, null) + ":" + conf.get("fs.ftp.password."
					 + host, null));
			}
			string[] userPasswdInfo = userAndPassword.split(":");
			com.google.common.@base.Preconditions.checkState(userPasswdInfo.Length > 1, "Invalid username / password"
				);
			conf.set(FS_FTP_USER_PREFIX + host, userPasswdInfo[0]);
			conf.set(FS_FTP_PASSWORD_PREFIX + host, userPasswdInfo[1]);
			setConf(conf);
			this.uri = uri;
		}

		/// <summary>Connect to the FTP server using configuration parameters</summary>
		/// <returns>An FTPClient instance</returns>
		/// <exception cref="System.IO.IOException"/>
		private org.apache.commons.net.ftp.FTPClient connect()
		{
			org.apache.commons.net.ftp.FTPClient client = null;
			org.apache.hadoop.conf.Configuration conf = getConf();
			string host = conf.get(FS_FTP_HOST);
			int port = conf.getInt(FS_FTP_HOST_PORT, org.apache.commons.net.ftp.FTP.DEFAULT_PORT
				);
			string user = conf.get(FS_FTP_USER_PREFIX + host);
			string password = conf.get(FS_FTP_PASSWORD_PREFIX + host);
			client = new org.apache.commons.net.ftp.FTPClient();
			client.connect(host, port);
			int reply = client.getReplyCode();
			if (!org.apache.commons.net.ftp.FTPReply.isPositiveCompletion(reply))
			{
				throw org.apache.hadoop.net.NetUtils.wrapException(host, port, org.apache.hadoop.net.NetUtils
					.UNKNOWN_HOST, 0, new java.net.ConnectException("Server response " + reply));
			}
			else
			{
				if (client.login(user, password))
				{
					client.setFileTransferMode(org.apache.commons.net.ftp.FTP.BLOCK_TRANSFER_MODE);
					client.setFileType(org.apache.commons.net.ftp.FTP.BINARY_FILE_TYPE);
					client.setBufferSize(DEFAULT_BUFFER_SIZE);
				}
				else
				{
					throw new System.IO.IOException("Login failed on server - " + host + ", port - " 
						+ port + " as user '" + user + "'");
				}
			}
			return client;
		}

		/// <summary>Logout and disconnect the given FTPClient.</summary>
		/// <param name="client"/>
		/// <exception cref="System.IO.IOException"/>
		private void disconnect(org.apache.commons.net.ftp.FTPClient client)
		{
			if (client != null)
			{
				if (!client.isConnected())
				{
					throw new org.apache.hadoop.fs.ftp.FTPException("Client not connected");
				}
				bool logoutSuccess = client.logout();
				client.disconnect();
				if (!logoutSuccess)
				{
					LOG.warn("Logout failed while disconnecting, error code - " + client.getReplyCode
						());
				}
			}
		}

		/// <summary>Resolve against given working directory.</summary>
		/// <param name="workDir"/>
		/// <param name="path"/>
		/// <returns/>
		private org.apache.hadoop.fs.Path makeAbsolute(org.apache.hadoop.fs.Path workDir, 
			org.apache.hadoop.fs.Path path)
		{
			if (path.isAbsolute())
			{
				return path;
			}
			return new org.apache.hadoop.fs.Path(workDir, path);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSDataInputStream open(org.apache.hadoop.fs.Path
			 file, int bufferSize)
		{
			org.apache.commons.net.ftp.FTPClient client = connect();
			org.apache.hadoop.fs.Path workDir = new org.apache.hadoop.fs.Path(client.printWorkingDirectory
				());
			org.apache.hadoop.fs.Path absolute = makeAbsolute(workDir, file);
			org.apache.hadoop.fs.FileStatus fileStat = getFileStatus(client, absolute);
			if (fileStat.isDirectory())
			{
				disconnect(client);
				throw new java.io.FileNotFoundException("Path " + file + " is a directory.");
			}
			client.allocate(bufferSize);
			org.apache.hadoop.fs.Path parent = absolute.getParent();
			// Change to parent directory on the
			// server. Only then can we read the
			// file
			// on the server by opening up an InputStream. As a side effect the working
			// directory on the server is changed to the parent directory of the file.
			// The FTP client connection is closed when close() is called on the
			// FSDataInputStream.
			client.changeWorkingDirectory(parent.toUri().getPath());
			java.io.InputStream @is = client.retrieveFileStream(file.getName());
			org.apache.hadoop.fs.FSDataInputStream fis = new org.apache.hadoop.fs.FSDataInputStream
				(new org.apache.hadoop.fs.ftp.FTPInputStream(@is, client, statistics));
			if (!org.apache.commons.net.ftp.FTPReply.isPositivePreliminary(client.getReplyCode
				()))
			{
				// The ftpClient is an inconsistent state. Must close the stream
				// which in turn will logout and disconnect from FTP server
				fis.close();
				throw new System.IO.IOException("Unable to open file: " + file + ", Aborting");
			}
			return fis;
		}

		/// <summary>
		/// A stream obtained via this call must be closed before using other APIs of
		/// this class or else the invocation will block.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
			 file, org.apache.hadoop.fs.permission.FsPermission permission, bool overwrite, 
			int bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
			 progress)
		{
			org.apache.commons.net.ftp.FTPClient client = connect();
			org.apache.hadoop.fs.Path workDir = new org.apache.hadoop.fs.Path(client.printWorkingDirectory
				());
			org.apache.hadoop.fs.Path absolute = makeAbsolute(workDir, file);
			org.apache.hadoop.fs.FileStatus status;
			try
			{
				status = getFileStatus(client, file);
			}
			catch (java.io.FileNotFoundException)
			{
				status = null;
			}
			if (status != null)
			{
				if (overwrite && !status.isDirectory())
				{
					delete(client, file, false);
				}
				else
				{
					disconnect(client);
					throw new org.apache.hadoop.fs.FileAlreadyExistsException("File already exists: "
						 + file);
				}
			}
			org.apache.hadoop.fs.Path parent = absolute.getParent();
			if (parent == null || !mkdirs(client, parent, org.apache.hadoop.fs.permission.FsPermission
				.getDirDefault()))
			{
				parent = (parent == null) ? new org.apache.hadoop.fs.Path("/") : parent;
				disconnect(client);
				throw new System.IO.IOException("create(): Mkdirs failed to create: " + parent);
			}
			client.allocate(bufferSize);
			// Change to parent directory on the server. Only then can we write to the
			// file on the server by opening up an OutputStream. As a side effect the
			// working directory on the server is changed to the parent directory of the
			// file. The FTP client connection is closed when close() is called on the
			// FSDataOutputStream.
			client.changeWorkingDirectory(parent.toUri().getPath());
			org.apache.hadoop.fs.FSDataOutputStream fos = new _FSDataOutputStream_263(this, client
				, client.storeFileStream(file.getName()), statistics);
			if (!org.apache.commons.net.ftp.FTPReply.isPositivePreliminary(client.getReplyCode
				()))
			{
				// The ftpClient is an inconsistent state. Must close the stream
				// which in turn will logout and disconnect from FTP server
				fos.close();
				throw new System.IO.IOException("Unable to create file: " + file + ", Aborting");
			}
			return fos;
		}

		private sealed class _FSDataOutputStream_263 : org.apache.hadoop.fs.FSDataOutputStream
		{
			public _FSDataOutputStream_263(FTPFileSystem _enclosing, org.apache.commons.net.ftp.FTPClient
				 client, java.io.OutputStream baseArg1, org.apache.hadoop.fs.FileSystem.Statistics
				 baseArg2)
				: base(baseArg1, baseArg2)
			{
				this._enclosing = _enclosing;
				this.client = client;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void close()
			{
				base.close();
				if (!client.isConnected())
				{
					throw new org.apache.hadoop.fs.ftp.FTPException("Client not connected");
				}
				bool cmdCompleted = client.completePendingCommand();
				this._enclosing.disconnect(client);
				if (!cmdCompleted)
				{
					throw new org.apache.hadoop.fs.ftp.FTPException("Could not complete transfer, Reply Code - "
						 + client.getReplyCode());
				}
			}

			private readonly FTPFileSystem _enclosing;

			private readonly org.apache.commons.net.ftp.FTPClient client;
		}

		/// <summary>This optional operation is not yet supported.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSDataOutputStream append(org.apache.hadoop.fs.Path
			 f, int bufferSize, org.apache.hadoop.util.Progressable progress)
		{
			throw new System.IO.IOException("Not supported");
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
		private bool exists(org.apache.commons.net.ftp.FTPClient client, org.apache.hadoop.fs.Path
			 file)
		{
			try
			{
				getFileStatus(client, file);
				return true;
			}
			catch (java.io.FileNotFoundException)
			{
				return false;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool delete(org.apache.hadoop.fs.Path file, bool recursive)
		{
			org.apache.commons.net.ftp.FTPClient client = connect();
			try
			{
				bool success = delete(client, file, recursive);
				return success;
			}
			finally
			{
				disconnect(client);
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
		private bool delete(org.apache.commons.net.ftp.FTPClient client, org.apache.hadoop.fs.Path
			 file, bool recursive)
		{
			org.apache.hadoop.fs.Path workDir = new org.apache.hadoop.fs.Path(client.printWorkingDirectory
				());
			org.apache.hadoop.fs.Path absolute = makeAbsolute(workDir, file);
			string pathName = absolute.toUri().getPath();
			try
			{
				org.apache.hadoop.fs.FileStatus fileStat = getFileStatus(client, absolute);
				if (fileStat.isFile())
				{
					return client.deleteFile(pathName);
				}
			}
			catch (java.io.FileNotFoundException)
			{
				//the file is not there
				return false;
			}
			org.apache.hadoop.fs.FileStatus[] dirEntries = listStatus(client, absolute);
			if (dirEntries != null && dirEntries.Length > 0 && !(recursive))
			{
				throw new System.IO.IOException("Directory: " + file + " is not empty.");
			}
			foreach (org.apache.hadoop.fs.FileStatus dirEntry in dirEntries)
			{
				delete(client, new org.apache.hadoop.fs.Path(absolute, dirEntry.getPath()), recursive
					);
			}
			return client.removeDirectory(pathName);
		}

		private org.apache.hadoop.fs.permission.FsAction getFsAction(int accessGroup, org.apache.commons.net.ftp.FTPFile
			 ftpFile)
		{
			org.apache.hadoop.fs.permission.FsAction action = org.apache.hadoop.fs.permission.FsAction
				.NONE;
			if (ftpFile.hasPermission(accessGroup, org.apache.commons.net.ftp.FTPFile.READ_PERMISSION
				))
			{
				action.or(org.apache.hadoop.fs.permission.FsAction.READ);
			}
			if (ftpFile.hasPermission(accessGroup, org.apache.commons.net.ftp.FTPFile.WRITE_PERMISSION
				))
			{
				action.or(org.apache.hadoop.fs.permission.FsAction.WRITE);
			}
			if (ftpFile.hasPermission(accessGroup, org.apache.commons.net.ftp.FTPFile.EXECUTE_PERMISSION
				))
			{
				action.or(org.apache.hadoop.fs.permission.FsAction.EXECUTE);
			}
			return action;
		}

		private org.apache.hadoop.fs.permission.FsPermission getPermissions(org.apache.commons.net.ftp.FTPFile
			 ftpFile)
		{
			org.apache.hadoop.fs.permission.FsAction user;
			org.apache.hadoop.fs.permission.FsAction group;
			org.apache.hadoop.fs.permission.FsAction others;
			user = getFsAction(org.apache.commons.net.ftp.FTPFile.USER_ACCESS, ftpFile);
			group = getFsAction(org.apache.commons.net.ftp.FTPFile.GROUP_ACCESS, ftpFile);
			others = getFsAction(org.apache.commons.net.ftp.FTPFile.WORLD_ACCESS, ftpFile);
			return new org.apache.hadoop.fs.permission.FsPermission(user, group, others);
		}

		public override java.net.URI getUri()
		{
			return uri;
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path
			 file)
		{
			org.apache.commons.net.ftp.FTPClient client = connect();
			try
			{
				org.apache.hadoop.fs.FileStatus[] stats = listStatus(client, file);
				return stats;
			}
			finally
			{
				disconnect(client);
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
		private org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.commons.net.ftp.FTPClient
			 client, org.apache.hadoop.fs.Path file)
		{
			org.apache.hadoop.fs.Path workDir = new org.apache.hadoop.fs.Path(client.printWorkingDirectory
				());
			org.apache.hadoop.fs.Path absolute = makeAbsolute(workDir, file);
			org.apache.hadoop.fs.FileStatus fileStat = getFileStatus(client, absolute);
			if (fileStat.isFile())
			{
				return new org.apache.hadoop.fs.FileStatus[] { fileStat };
			}
			org.apache.commons.net.ftp.FTPFile[] ftpFiles = client.listFiles(absolute.toUri()
				.getPath());
			org.apache.hadoop.fs.FileStatus[] fileStats = new org.apache.hadoop.fs.FileStatus
				[ftpFiles.Length];
			for (int i = 0; i < ftpFiles.Length; i++)
			{
				fileStats[i] = getFileStatus(ftpFiles[i], absolute);
			}
			return fileStats;
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileStatus getFileStatus(org.apache.hadoop.fs.Path
			 file)
		{
			org.apache.commons.net.ftp.FTPClient client = connect();
			try
			{
				org.apache.hadoop.fs.FileStatus status = getFileStatus(client, file);
				return status;
			}
			finally
			{
				disconnect(client);
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
		private org.apache.hadoop.fs.FileStatus getFileStatus(org.apache.commons.net.ftp.FTPClient
			 client, org.apache.hadoop.fs.Path file)
		{
			org.apache.hadoop.fs.FileStatus fileStat = null;
			org.apache.hadoop.fs.Path workDir = new org.apache.hadoop.fs.Path(client.printWorkingDirectory
				());
			org.apache.hadoop.fs.Path absolute = makeAbsolute(workDir, file);
			org.apache.hadoop.fs.Path parentPath = absolute.getParent();
			if (parentPath == null)
			{
				// root dir
				long length = -1;
				// Length of root dir on server not known
				bool isDir = true;
				int blockReplication = 1;
				long blockSize = DEFAULT_BLOCK_SIZE;
				// Block Size not known.
				long modTime = -1;
				// Modification time of root dir not known.
				org.apache.hadoop.fs.Path root = new org.apache.hadoop.fs.Path("/");
				return new org.apache.hadoop.fs.FileStatus(length, isDir, blockReplication, blockSize
					, modTime, root.makeQualified(this));
			}
			string pathName = parentPath.toUri().getPath();
			org.apache.commons.net.ftp.FTPFile[] ftpFiles = client.listFiles(pathName);
			if (ftpFiles != null)
			{
				foreach (org.apache.commons.net.ftp.FTPFile ftpFile in ftpFiles)
				{
					if (ftpFile.getName().Equals(file.getName()))
					{
						// file found in dir
						fileStat = getFileStatus(ftpFile, parentPath);
						break;
					}
				}
				if (fileStat == null)
				{
					throw new java.io.FileNotFoundException("File " + file + " does not exist.");
				}
			}
			else
			{
				throw new java.io.FileNotFoundException("File " + file + " does not exist.");
			}
			return fileStat;
		}

		/// <summary>
		/// Convert the file information in FTPFile to a
		/// <see cref="org.apache.hadoop.fs.FileStatus"/>
		/// object.
		/// </summary>
		/// <param name="ftpFile"/>
		/// <param name="parentPath"/>
		/// <returns>FileStatus</returns>
		private org.apache.hadoop.fs.FileStatus getFileStatus(org.apache.commons.net.ftp.FTPFile
			 ftpFile, org.apache.hadoop.fs.Path parentPath)
		{
			long length = ftpFile.getSize();
			bool isDir = ftpFile.isDirectory();
			int blockReplication = 1;
			// Using default block size since there is no way in FTP client to know of
			// block sizes on server. The assumption could be less than ideal.
			long blockSize = DEFAULT_BLOCK_SIZE;
			long modTime = ftpFile.getTimestamp().getTimeInMillis();
			long accessTime = 0;
			org.apache.hadoop.fs.permission.FsPermission permission = getPermissions(ftpFile);
			string user = ftpFile.getUser();
			string group = ftpFile.getGroup();
			org.apache.hadoop.fs.Path filePath = new org.apache.hadoop.fs.Path(parentPath, ftpFile
				.getName());
			return new org.apache.hadoop.fs.FileStatus(length, isDir, blockReplication, blockSize
				, modTime, accessTime, permission, user, group, filePath.makeQualified(this));
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool mkdirs(org.apache.hadoop.fs.Path file, org.apache.hadoop.fs.permission.FsPermission
			 permission)
		{
			org.apache.commons.net.ftp.FTPClient client = connect();
			try
			{
				bool success = mkdirs(client, file, permission);
				return success;
			}
			finally
			{
				disconnect(client);
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
		private bool mkdirs(org.apache.commons.net.ftp.FTPClient client, org.apache.hadoop.fs.Path
			 file, org.apache.hadoop.fs.permission.FsPermission permission)
		{
			bool created = true;
			org.apache.hadoop.fs.Path workDir = new org.apache.hadoop.fs.Path(client.printWorkingDirectory
				());
			org.apache.hadoop.fs.Path absolute = makeAbsolute(workDir, file);
			string pathName = absolute.getName();
			if (!exists(client, absolute))
			{
				org.apache.hadoop.fs.Path parent = absolute.getParent();
				created = (parent == null || mkdirs(client, parent, org.apache.hadoop.fs.permission.FsPermission
					.getDirDefault()));
				if (created)
				{
					string parentDir = parent.toUri().getPath();
					client.changeWorkingDirectory(parentDir);
					created = created && client.makeDirectory(pathName);
				}
			}
			else
			{
				if (isFile(client, absolute))
				{
					throw new org.apache.hadoop.fs.ParentNotDirectoryException(string.format("Can't make directory for path %s since it is a file."
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
		private bool isFile(org.apache.commons.net.ftp.FTPClient client, org.apache.hadoop.fs.Path
			 file)
		{
			try
			{
				return getFileStatus(client, file).isFile();
			}
			catch (java.io.FileNotFoundException)
			{
				return false;
			}
			catch (System.IO.IOException ioe)
			{
				// file does not exist
				throw new org.apache.hadoop.fs.ftp.FTPException("File check failed", ioe);
			}
		}

		/*
		* Assuming that parent of both source and destination is the same. Is the
		* assumption correct or it is suppose to work like 'move' ?
		*/
		/// <exception cref="System.IO.IOException"/>
		public override bool rename(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
			 dst)
		{
			org.apache.commons.net.ftp.FTPClient client = connect();
			try
			{
				bool success = rename(client, src, dst);
				return success;
			}
			finally
			{
				disconnect(client);
			}
		}

		/// <summary>Probe for a path being a parent of another</summary>
		/// <param name="parent">parent path</param>
		/// <param name="child">possible child path</param>
		/// <returns>true if the parent's path matches the start of the child's</returns>
		private bool isParentOf(org.apache.hadoop.fs.Path parent, org.apache.hadoop.fs.Path
			 child)
		{
			java.net.URI parentURI = parent.toUri();
			string parentPath = parentURI.getPath();
			if (!parentPath.EndsWith("/"))
			{
				parentPath += "/";
			}
			java.net.URI childURI = child.toUri();
			string childPath = childURI.getPath();
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
		private bool rename(org.apache.commons.net.ftp.FTPClient client, org.apache.hadoop.fs.Path
			 src, org.apache.hadoop.fs.Path dst)
		{
			org.apache.hadoop.fs.Path workDir = new org.apache.hadoop.fs.Path(client.printWorkingDirectory
				());
			org.apache.hadoop.fs.Path absoluteSrc = makeAbsolute(workDir, src);
			org.apache.hadoop.fs.Path absoluteDst = makeAbsolute(workDir, dst);
			if (!exists(client, absoluteSrc))
			{
				throw new java.io.FileNotFoundException("Source path " + src + " does not exist");
			}
			if (isDirectory(absoluteDst))
			{
				// destination is a directory: rename goes underneath it with the
				// source name
				absoluteDst = new org.apache.hadoop.fs.Path(absoluteDst, absoluteSrc.getName());
			}
			if (exists(client, absoluteDst))
			{
				throw new org.apache.hadoop.fs.FileAlreadyExistsException("Destination path " + dst
					 + " already exists");
			}
			string parentSrc = absoluteSrc.getParent().toUri().ToString();
			string parentDst = absoluteDst.getParent().toUri().ToString();
			if (isParentOf(absoluteSrc, absoluteDst))
			{
				throw new System.IO.IOException("Cannot rename " + absoluteSrc + " under itself" 
					+ " : " + absoluteDst);
			}
			if (!parentSrc.Equals(parentDst))
			{
				throw new System.IO.IOException("Cannot rename source: " + absoluteSrc + " to " +
					 absoluteDst + " -" + E_SAME_DIRECTORY_ONLY);
			}
			string from = absoluteSrc.getName();
			string to = absoluteDst.getName();
			client.changeWorkingDirectory(parentSrc);
			bool renamed = client.rename(from, to);
			return renamed;
		}

		public override org.apache.hadoop.fs.Path getWorkingDirectory()
		{
			// Return home directory always since we do not maintain state.
			return getHomeDirectory();
		}

		public override org.apache.hadoop.fs.Path getHomeDirectory()
		{
			org.apache.commons.net.ftp.FTPClient client = null;
			try
			{
				client = connect();
				org.apache.hadoop.fs.Path homeDir = new org.apache.hadoop.fs.Path(client.printWorkingDirectory
					());
				return homeDir;
			}
			catch (System.IO.IOException ioe)
			{
				throw new org.apache.hadoop.fs.ftp.FTPException("Failed to get home directory", ioe
					);
			}
			finally
			{
				try
				{
					disconnect(client);
				}
				catch (System.IO.IOException ioe)
				{
					throw new org.apache.hadoop.fs.ftp.FTPException("Failed to disconnect", ioe);
				}
			}
		}

		public override void setWorkingDirectory(org.apache.hadoop.fs.Path newDir)
		{
		}
		// we do not maintain the working directory state
	}
}
