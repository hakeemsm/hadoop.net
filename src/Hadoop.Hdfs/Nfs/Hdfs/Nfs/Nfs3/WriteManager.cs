using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Nfs.Conf;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Nfs;
using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Nfs.Nfs3.Request;
using Org.Apache.Hadoop.Nfs.Nfs3.Response;
using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Oncrpc.Security;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Nfs.Nfs3
{
	/// <summary>Manage the writes and responds asynchronously.</summary>
	public class WriteManager
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.WriteManager
			));

		private readonly NfsConfiguration config;

		private readonly IdMappingServiceProvider iug;

		private AsyncDataService asyncDataService;

		private bool asyncDataServiceStarted = false;

		private readonly int maxStreams;

		private readonly bool aixCompatMode;

		/// <summary>
		/// The time limit to wait for accumulate reordered sequential writes to the
		/// same file before the write is considered done.
		/// </summary>
		private long streamTimeout;

		private readonly OpenFileCtxCache fileContextCache;

		[System.Serializable]
		public class MultipleCachedStreamException : IOException
		{
			private const long serialVersionUID = 1L;

			public MultipleCachedStreamException(string msg)
				: base(msg)
			{
			}
		}

		internal virtual bool AddOpenFileStream(FileHandle h, OpenFileCtx ctx)
		{
			return fileContextCache.Put(h, ctx);
		}

		internal WriteManager(IdMappingServiceProvider iug, NfsConfiguration config, bool
			 aixCompatMode)
		{
			this.iug = iug;
			this.config = config;
			this.aixCompatMode = aixCompatMode;
			streamTimeout = config.GetLong(NfsConfigKeys.DfsNfsStreamTimeoutKey, NfsConfigKeys
				.DfsNfsStreamTimeoutDefault);
			Log.Info("Stream timeout is " + streamTimeout + "ms.");
			if (streamTimeout < NfsConfigKeys.DfsNfsStreamTimeoutMinDefault)
			{
				Log.Info("Reset stream timeout to minimum value " + NfsConfigKeys.DfsNfsStreamTimeoutMinDefault
					 + "ms.");
				streamTimeout = NfsConfigKeys.DfsNfsStreamTimeoutMinDefault;
			}
			maxStreams = config.GetInt(NfsConfigKeys.DfsNfsMaxOpenFilesKey, NfsConfigKeys.DfsNfsMaxOpenFilesDefault
				);
			Log.Info("Maximum open streams is " + maxStreams);
			this.fileContextCache = new OpenFileCtxCache(config, streamTimeout);
		}

		internal virtual void StartAsyncDataService()
		{
			if (asyncDataServiceStarted)
			{
				return;
			}
			fileContextCache.Start();
			this.asyncDataService = new AsyncDataService();
			asyncDataServiceStarted = true;
		}

		internal virtual void ShutdownAsyncDataService()
		{
			if (!asyncDataServiceStarted)
			{
				return;
			}
			asyncDataServiceStarted = false;
			asyncDataService.Shutdown();
			fileContextCache.Shutdown();
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void HandleWrite(DFSClient dfsClient, WRITE3Request request, Org.Jboss.Netty.Channel.Channel
			 channel, int xid, Nfs3FileAttributes preOpAttr)
		{
			int count = request.GetCount();
			byte[] data = ((byte[])request.GetData().Array());
			if (data.Length < count)
			{
				WRITE3Response response = new WRITE3Response(Nfs3Status.Nfs3errInval);
				Nfs3Utils.WriteChannel(channel, response.Serialize(new XDR(), xid, new VerifierNone
					()), xid);
				return;
			}
			FileHandle handle = request.GetHandle();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("handleWrite " + request);
			}
			// Check if there is a stream to write
			FileHandle fileHandle = request.GetHandle();
			OpenFileCtx openFileCtx = fileContextCache.Get(fileHandle);
			if (openFileCtx == null)
			{
				Log.Info("No opened stream for fileId: " + fileHandle.GetFileId());
				string fileIdPath = Nfs3Utils.GetFileIdPath(fileHandle.GetFileId());
				HdfsDataOutputStream fos = null;
				Nfs3FileAttributes latestAttr = null;
				try
				{
					int bufferSize = config.GetInt(CommonConfigurationKeysPublic.IoFileBufferSizeKey, 
						CommonConfigurationKeysPublic.IoFileBufferSizeDefault);
					fos = dfsClient.Append(fileIdPath, bufferSize, EnumSet.Of(CreateFlag.Append), null
						, null);
					latestAttr = Nfs3Utils.GetFileAttr(dfsClient, fileIdPath, iug);
				}
				catch (RemoteException e)
				{
					IOException io = e.UnwrapRemoteException();
					if (io is AlreadyBeingCreatedException)
					{
						Log.Warn("Can't append file: " + fileIdPath + ". Possibly the file is being closed. Drop the request: "
							 + request + ", wait for the client to retry...");
						return;
					}
					throw;
				}
				catch (IOException e)
				{
					Log.Error("Can't append to file: " + fileIdPath, e);
					if (fos != null)
					{
						fos.Close();
					}
					WccData fileWcc = new WccData(Nfs3Utils.GetWccAttr(preOpAttr), preOpAttr);
					WRITE3Response response = new WRITE3Response(Nfs3Status.Nfs3errIo, fileWcc, count
						, request.GetStableHow(), Nfs3Constant.WriteCommitVerf);
					Nfs3Utils.WriteChannel(channel, response.Serialize(new XDR(), xid, new VerifierNone
						()), xid);
					return;
				}
				// Add open stream
				string writeDumpDir = config.Get(NfsConfigKeys.DfsNfsFileDumpDirKey, NfsConfigKeys
					.DfsNfsFileDumpDirDefault);
				openFileCtx = new OpenFileCtx(fos, latestAttr, writeDumpDir + "/" + fileHandle.GetFileId
					(), dfsClient, iug, aixCompatMode, config);
				if (!AddOpenFileStream(fileHandle, openFileCtx))
				{
					Log.Info("Can't add new stream. Close it. Tell client to retry.");
					try
					{
						fos.Close();
					}
					catch (IOException e)
					{
						Log.Error("Can't close stream for fileId: " + handle.GetFileId(), e);
					}
					// Notify client to retry
					WccData fileWcc = new WccData(latestAttr.GetWccAttr(), latestAttr);
					WRITE3Response response = new WRITE3Response(Nfs3Status.Nfs3errJukebox, fileWcc, 
						0, request.GetStableHow(), Nfs3Constant.WriteCommitVerf);
					Nfs3Utils.WriteChannel(channel, response.Serialize(new XDR(), xid, new VerifierNone
						()), xid);
					return;
				}
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Opened stream for appending file: " + fileHandle.GetFileId());
				}
			}
			// Add write into the async job queue
			openFileCtx.ReceivedNewWrite(dfsClient, request, channel, xid, asyncDataService, 
				iug);
			return;
		}

		// Do a possible commit before read request in case there is buffered data
		// inside DFSClient which has been flushed but not synced.
		internal virtual int CommitBeforeRead(DFSClient dfsClient, FileHandle fileHandle, 
			long commitOffset)
		{
			int status;
			OpenFileCtx openFileCtx = fileContextCache.Get(fileHandle);
			if (openFileCtx == null)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("No opened stream for fileId: " + fileHandle.GetFileId() + " commitOffset="
						 + commitOffset + ". Return success in this case.");
				}
				status = Nfs3Status.Nfs3Ok;
			}
			else
			{
				// commit request triggered by read won't create pending comment obj
				OpenFileCtx.COMMIT_STATUS ret = openFileCtx.CheckCommit(dfsClient, commitOffset, 
					null, 0, null, true);
				switch (ret)
				{
					case OpenFileCtx.COMMIT_STATUS.CommitFinished:
					case OpenFileCtx.COMMIT_STATUS.CommitInactiveCtx:
					{
						status = Nfs3Status.Nfs3Ok;
						break;
					}

					case OpenFileCtx.COMMIT_STATUS.CommitInactiveWithPendingWrite:
					case OpenFileCtx.COMMIT_STATUS.CommitError:
					{
						status = Nfs3Status.Nfs3errIo;
						break;
					}

					case OpenFileCtx.COMMIT_STATUS.CommitWait:
					case OpenFileCtx.COMMIT_STATUS.CommitSpecialWait:
					{
						status = Nfs3Status.Nfs3errJukebox;
						break;
					}

					case OpenFileCtx.COMMIT_STATUS.CommitSpecialSuccess:
					{
						// Read beyond eof could result in partial read
						status = Nfs3Status.Nfs3Ok;
						break;
					}

					default:
					{
						Log.Error("Should not get commit return code: " + ret.ToString());
						throw new RuntimeException("Should not get commit return code: " + ret.ToString()
							);
					}
				}
			}
			return status;
		}

		internal virtual void HandleCommit(DFSClient dfsClient, FileHandle fileHandle, long
			 commitOffset, Org.Jboss.Netty.Channel.Channel channel, int xid, Nfs3FileAttributes
			 preOpAttr)
		{
			long startTime = Runtime.NanoTime();
			int status;
			OpenFileCtx openFileCtx = fileContextCache.Get(fileHandle);
			if (openFileCtx == null)
			{
				Log.Info("No opened stream for fileId: " + fileHandle.GetFileId() + " commitOffset="
					 + commitOffset + ". Return success in this case.");
				status = Nfs3Status.Nfs3Ok;
			}
			else
			{
				OpenFileCtx.COMMIT_STATUS ret = openFileCtx.CheckCommit(dfsClient, commitOffset, 
					channel, xid, preOpAttr, false);
				switch (ret)
				{
					case OpenFileCtx.COMMIT_STATUS.CommitFinished:
					case OpenFileCtx.COMMIT_STATUS.CommitInactiveCtx:
					{
						status = Nfs3Status.Nfs3Ok;
						break;
					}

					case OpenFileCtx.COMMIT_STATUS.CommitInactiveWithPendingWrite:
					case OpenFileCtx.COMMIT_STATUS.CommitError:
					{
						status = Nfs3Status.Nfs3errIo;
						break;
					}

					case OpenFileCtx.COMMIT_STATUS.CommitWait:
					{
						// Do nothing. Commit is async now.
						return;
					}

					case OpenFileCtx.COMMIT_STATUS.CommitSpecialWait:
					{
						status = Nfs3Status.Nfs3errJukebox;
						break;
					}

					case OpenFileCtx.COMMIT_STATUS.CommitSpecialSuccess:
					{
						status = Nfs3Status.Nfs3Ok;
						break;
					}

					default:
					{
						Log.Error("Should not get commit return code: " + ret.ToString());
						throw new RuntimeException("Should not get commit return code: " + ret.ToString()
							);
					}
				}
			}
			// Send out the response
			Nfs3FileAttributes postOpAttr = null;
			try
			{
				postOpAttr = GetFileAttr(dfsClient, new FileHandle(preOpAttr.GetFileId()), iug);
			}
			catch (IOException e1)
			{
				Log.Info("Can't get postOpAttr for fileId: " + preOpAttr.GetFileId(), e1);
			}
			WccData fileWcc = new WccData(Nfs3Utils.GetWccAttr(preOpAttr), postOpAttr);
			COMMIT3Response response = new COMMIT3Response(status, fileWcc, Nfs3Constant.WriteCommitVerf
				);
			RpcProgramNfs3.metrics.AddCommit(Nfs3Utils.GetElapsedTime(startTime));
			Nfs3Utils.WriteChannelCommit(channel, response.Serialize(new XDR(), xid, new VerifierNone
				()), xid);
		}

		/// <summary>If the file is in cache, update the size based on the cached data size</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual Nfs3FileAttributes GetFileAttr(DFSClient client, FileHandle fileHandle
			, IdMappingServiceProvider iug)
		{
			string fileIdPath = Nfs3Utils.GetFileIdPath(fileHandle);
			Nfs3FileAttributes attr = Nfs3Utils.GetFileAttr(client, fileIdPath, iug);
			if (attr != null)
			{
				OpenFileCtx openFileCtx = fileContextCache.Get(fileHandle);
				if (openFileCtx != null)
				{
					attr.SetSize(openFileCtx.GetNextOffset());
					attr.SetUsed(openFileCtx.GetNextOffset());
				}
			}
			return attr;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual Nfs3FileAttributes GetFileAttr(DFSClient client, FileHandle dirHandle
			, string fileName)
		{
			string fileIdPath = Nfs3Utils.GetFileIdPath(dirHandle) + "/" + fileName;
			Nfs3FileAttributes attr = Nfs3Utils.GetFileAttr(client, fileIdPath, iug);
			if ((attr != null) && (attr.GetType() == NfsFileType.Nfsreg.ToValue()))
			{
				OpenFileCtx openFileCtx = fileContextCache.Get(new FileHandle(attr.GetFileId()));
				if (openFileCtx != null)
				{
					attr.SetSize(openFileCtx.GetNextOffset());
					attr.SetUsed(openFileCtx.GetNextOffset());
				}
			}
			return attr;
		}

		[VisibleForTesting]
		internal virtual OpenFileCtxCache GetOpenFileCtxCache()
		{
			return this.fileContextCache;
		}
	}
}
