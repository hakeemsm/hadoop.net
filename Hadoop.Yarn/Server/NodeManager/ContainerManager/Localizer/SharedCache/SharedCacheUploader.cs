using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Sharedcache;
using Org.Apache.Hadoop.Yarn.Sharedcache;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Sharedcache
{
	/// <summary>The callable class that handles the actual upload to the shared cache.</summary>
	internal class SharedCacheUploader : Callable<bool>
	{
		internal static readonly FsPermission DirectoryPermission = new FsPermission((short
			)0x1ed);

		internal static readonly FsPermission FilePermission = new FsPermission((short)0x16d
			);

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Sharedcache.SharedCacheUploader
			));

		private sealed class _ThreadLocal_65 : ThreadLocal<Random>
		{
			public _ThreadLocal_65()
			{
			}

			// rwxr-xr-x
			// r-xr-xr-x
			protected override Random InitialValue()
			{
				return new Random(Runtime.NanoTime());
			}
		}

		private static readonly ThreadLocal<Random> randomTl = new _ThreadLocal_65();

		private readonly LocalResource resource;

		private readonly Path localPath;

		private readonly string user;

		private readonly Configuration conf;

		private readonly SCMUploaderProtocol scmClient;

		private readonly FileSystem fs;

		private readonly FileSystem localFs;

		private readonly string sharedCacheRootDir;

		private readonly int nestedLevel;

		private readonly SharedCacheChecksum checksum;

		private readonly RecordFactory recordFactory;

		/// <exception cref="System.IO.IOException"/>
		public SharedCacheUploader(LocalResource resource, Path localPath, string user, Configuration
			 conf, SCMUploaderProtocol scmClient)
			: this(resource, localPath, user, conf, scmClient, FileSystem.Get(conf), localPath
				.GetFileSystem(conf))
		{
		}

		/// <param name="resource">the local resource that contains the original remote path</param>
		/// <param name="localPath">
		/// the path in the local filesystem where the resource is
		/// localized
		/// </param>
		/// <param name="fs">the filesystem of the shared cache</param>
		/// <param name="localFs">the local filesystem</param>
		public SharedCacheUploader(LocalResource resource, Path localPath, string user, Configuration
			 conf, SCMUploaderProtocol scmClient, FileSystem fs, FileSystem localFs)
		{
			this.resource = resource;
			this.localPath = localPath;
			this.user = user;
			this.conf = conf;
			this.scmClient = scmClient;
			this.fs = fs;
			this.sharedCacheRootDir = conf.Get(YarnConfiguration.SharedCacheRoot, YarnConfiguration
				.DefaultSharedCacheRoot);
			this.nestedLevel = SharedCacheUtil.GetCacheDepth(conf);
			this.checksum = SharedCacheChecksumFactory.GetChecksum(conf);
			this.localFs = localFs;
			this.recordFactory = RecordFactoryProvider.GetRecordFactory(null);
		}

		/// <summary>
		/// Uploads the file under the shared cache, and notifies the shared cache
		/// manager.
		/// </summary>
		/// <remarks>
		/// Uploads the file under the shared cache, and notifies the shared cache
		/// manager. If it is unable to upload the file because it already exists, it
		/// returns false.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual bool Call()
		{
			Path tempPath = null;
			try
			{
				if (!VerifyAccess())
				{
					Log.Warn("User " + user + " is not authorized to upload file " + localPath.GetName
						());
					return false;
				}
				// first determine the actual local path that will be used for upload
				Path actualPath = GetActualPath();
				// compute the checksum
				string checksumVal = ComputeChecksum(actualPath);
				// create the directory (if it doesn't exist)
				Path directoryPath = new Path(SharedCacheUtil.GetCacheEntryPath(nestedLevel, sharedCacheRootDir
					, checksumVal));
				// let's not check if the directory already exists: in the vast majority
				// of the cases, the directory does not exist; as long as mkdirs does not
				// error out if it exists, we should be fine
				fs.Mkdirs(directoryPath, DirectoryPermission);
				// create the temporary file
				tempPath = new Path(directoryPath, GetTemporaryFileName(actualPath));
				if (!UploadFile(actualPath, tempPath))
				{
					Log.Warn("Could not copy the file to the shared cache at " + tempPath);
					return false;
				}
				// set the permission so that it is readable but not writable
				fs.SetPermission(tempPath, FilePermission);
				// rename it to the final filename
				Path finalPath = new Path(directoryPath, actualPath.GetName());
				if (!fs.Rename(tempPath, finalPath))
				{
					Log.Warn("The file already exists under " + finalPath + ". Ignoring this attempt."
						);
					DeleteTempFile(tempPath);
					return false;
				}
				// notify the SCM
				if (!NotifySharedCacheManager(checksumVal, actualPath.GetName()))
				{
					// the shared cache manager rejected the upload (as it is likely
					// uploaded under a different name
					// clean up this file and exit
					fs.Delete(finalPath, false);
					return false;
				}
				// set the replication factor
				short replication = (short)conf.GetInt(YarnConfiguration.SharedCacheNmUploaderReplicationFactor
					, YarnConfiguration.DefaultSharedCacheNmUploaderReplicationFactor);
				fs.SetReplication(finalPath, replication);
				Log.Info("File " + actualPath.GetName() + " was uploaded to the shared cache at "
					 + finalPath);
				return true;
			}
			catch (IOException e)
			{
				Log.Warn("Exception while uploading the file " + localPath.GetName(), e);
				// in case an exception is thrown, delete the temp file
				DeleteTempFile(tempPath);
				throw;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual Path GetActualPath()
		{
			Path path = localPath;
			FileStatus status = localFs.GetFileStatus(path);
			if (status != null && status.IsDirectory())
			{
				// for certain types of resources that get unpacked, the original file may
				// be found under the directory with the same name (see
				// FSDownload.unpack); check if the path is a directory and if so look
				// under it
				path = new Path(path, path.GetName());
			}
			return path;
		}

		private void DeleteTempFile(Path tempPath)
		{
			try
			{
				if (tempPath != null && fs.Exists(tempPath))
				{
					fs.Delete(tempPath, false);
				}
			}
			catch (IOException)
			{
			}
		}

		/// <summary>
		/// Checks that the (original) remote file is either owned by the user who
		/// started the app or public.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual bool VerifyAccess()
		{
			// if it is in the public cache, it's trivially OK
			if (resource.GetVisibility() == LocalResourceVisibility.Public)
			{
				return true;
			}
			Path remotePath;
			try
			{
				remotePath = ConverterUtils.GetPathFromYarnURL(resource.GetResource());
			}
			catch (URISyntaxException e)
			{
				throw new IOException("Invalid resource", e);
			}
			// get the file status of the HDFS file
			FileSystem remoteFs = remotePath.GetFileSystem(conf);
			FileStatus status = remoteFs.GetFileStatus(remotePath);
			// check to see if the file has been modified in any way
			if (status.GetModificationTime() != resource.GetTimestamp())
			{
				Log.Warn("The remote file " + remotePath + " has changed since it's localized; will not consider it for upload"
					);
				return false;
			}
			// check for the user ownership
			if (status.GetOwner().Equals(user))
			{
				return true;
			}
			// the user owns the file
			// check if the file is publicly readable otherwise
			return FileIsPublic(remotePath, remoteFs, status);
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual bool FileIsPublic(Path remotePath, FileSystem remoteFs, FileStatus
			 status)
		{
			return FSDownload.IsPublic(remoteFs, remotePath, status, null);
		}

		/// <summary>
		/// Uploads the file to the shared cache under a temporary name, and returns
		/// the result.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual bool UploadFile(Path sourcePath, Path tempPath)
		{
			return FileUtil.Copy(localFs, sourcePath, fs, tempPath, false, conf);
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual string ComputeChecksum(Path path)
		{
			InputStream @is = localFs.Open(path);
			try
			{
				return checksum.ComputeChecksum(@is);
			}
			finally
			{
				try
				{
					@is.Close();
				}
				catch (IOException)
				{
				}
			}
		}

		private string GetTemporaryFileName(Path path)
		{
			return path.GetName() + "-" + randomTl.Get().NextLong();
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual bool NotifySharedCacheManager(string checksumVal, string fileName
			)
		{
			try
			{
				SCMUploaderNotifyRequest request = recordFactory.NewRecordInstance<SCMUploaderNotifyRequest
					>();
				request.SetResourceKey(checksumVal);
				request.SetFilename(fileName);
				return scmClient.Notify(request).GetAccepted();
			}
			catch (YarnException e)
			{
				throw new IOException(e);
			}
			catch (UndeclaredThrowableException e)
			{
				// retrieve the cause of the exception and throw it as an IOException
				throw new IOException(e.InnerException == null ? e : e.InnerException);
			}
		}
	}
}
