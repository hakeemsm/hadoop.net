using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Collect;
using Javax.Servlet.Http;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication.Client;
using Org.Apache.Hadoop.Util;
using Org.Apache.Http.Client.Utils;
using Org.Mortbay.Jetty;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>This class provides fetching a specified file from the NameNode.</summary>
	public class TransferFsImage
	{
		public const string ContentLength = "Content-Length";

		public const string FileLength = "File-Length";

		public const string Md5Header = "X-MD5-Digest";

		private const string ContentType = "Content-Type";

		private const string ContentTransferEncoding = "Content-Transfer-Encoding";

		[VisibleForTesting]
		internal static int timeout = 0;

		private static readonly URLConnectionFactory connectionFactory;

		private static readonly bool isSpnegoEnabled;

		static TransferFsImage()
		{
			Configuration conf = new Configuration();
			connectionFactory = URLConnectionFactory.NewDefaultURLConnectionFactory(conf);
			isSpnegoEnabled = UserGroupInformation.IsSecurityEnabled();
		}

		private static readonly Log Log = LogFactory.GetLog(typeof(TransferFsImage));

		/// <exception cref="System.IO.IOException"/>
		public static void DownloadMostRecentImageToDirectory(Uri infoServer, FilePath dir
			)
		{
			string fileId = ImageServlet.GetParamStringForMostRecentImage();
			GetFileClient(infoServer, fileId, Lists.NewArrayList(dir), null, false);
		}

		/// <exception cref="System.IO.IOException"/>
		public static MD5Hash DownloadImageToStorage(Uri fsName, long imageTxId, Storage 
			dstStorage, bool needDigest)
		{
			string fileid = ImageServlet.GetParamStringForImage(null, imageTxId, dstStorage);
			string fileName = NNStorage.GetCheckpointImageFileName(imageTxId);
			IList<FilePath> dstFiles = dstStorage.GetFiles(NNStorage.NameNodeDirType.Image, fileName
				);
			if (dstFiles.IsEmpty())
			{
				throw new IOException("No targets in destination storage!");
			}
			MD5Hash hash = GetFileClient(fsName, fileid, dstFiles, dstStorage, needDigest);
			Log.Info("Downloaded file " + dstFiles[0].GetName() + " size " + dstFiles[0].Length
				() + " bytes.");
			return hash;
		}

		/// <exception cref="System.IO.IOException"/>
		internal static MD5Hash HandleUploadImageRequest(HttpServletRequest request, long
			 imageTxId, Storage dstStorage, InputStream stream, long advertisedSize, DataTransferThrottler
			 throttler)
		{
			string fileName = NNStorage.GetCheckpointImageFileName(imageTxId);
			IList<FilePath> dstFiles = dstStorage.GetFiles(NNStorage.NameNodeDirType.Image, fileName
				);
			if (dstFiles.IsEmpty())
			{
				throw new IOException("No targets in destination storage!");
			}
			MD5Hash advertisedDigest = ParseMD5Header(request);
			MD5Hash hash = ReceiveFile(fileName, dstFiles, dstStorage, true, advertisedSize, 
				advertisedDigest, fileName, stream, throttler);
			Log.Info("Downloaded file " + dstFiles[0].GetName() + " size " + dstFiles[0].Length
				() + " bytes.");
			return hash;
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void DownloadEditsToStorage(Uri fsName, RemoteEditLog log, NNStorage
			 dstStorage)
		{
			System.Diagnostics.Debug.Assert(log.GetStartTxId() > 0 && log.GetEndTxId() > 0, "bad log: "
				 + log);
			string fileid = ImageServlet.GetParamStringForLog(log, dstStorage);
			string finalFileName = NNStorage.GetFinalizedEditsFileName(log.GetStartTxId(), log
				.GetEndTxId());
			IList<FilePath> finalFiles = dstStorage.GetFiles(NNStorage.NameNodeDirType.Edits, 
				finalFileName);
			System.Diagnostics.Debug.Assert(!finalFiles.IsEmpty(), "No checkpoint targets.");
			foreach (FilePath f in finalFiles)
			{
				if (f.Exists() && FileUtil.CanRead(f))
				{
					Log.Info("Skipping download of remote edit log " + log + " since it already is stored locally at "
						 + f);
					return;
				}
				else
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Dest file: " + f);
					}
				}
			}
			long milliTime = Time.MonotonicNow();
			string tmpFileName = NNStorage.GetTemporaryEditsFileName(log.GetStartTxId(), log.
				GetEndTxId(), milliTime);
			IList<FilePath> tmpFiles = dstStorage.GetFiles(NNStorage.NameNodeDirType.Edits, tmpFileName
				);
			GetFileClient(fsName, fileid, tmpFiles, dstStorage, false);
			Log.Info("Downloaded file " + tmpFiles[0].GetName() + " size " + finalFiles[0].Length
				() + " bytes.");
			CheckpointFaultInjector.GetInstance().BeforeEditsRename();
			foreach (Storage.StorageDirectory sd in dstStorage.DirIterable(NNStorage.NameNodeDirType
				.Edits))
			{
				FilePath tmpFile = NNStorage.GetTemporaryEditsFile(sd, log.GetStartTxId(), log.GetEndTxId
					(), milliTime);
				FilePath finalizedFile = NNStorage.GetFinalizedEditsFile(sd, log.GetStartTxId(), 
					log.GetEndTxId());
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Renaming " + tmpFile + " to " + finalizedFile);
				}
				bool success = tmpFile.RenameTo(finalizedFile);
				if (!success)
				{
					Log.Warn("Unable to rename edits file from " + tmpFile + " to " + finalizedFile);
				}
			}
		}

		/// <summary>Requests that the NameNode download an image from this node.</summary>
		/// <param name="fsName">the http address for the remote NN</param>
		/// <param name="conf">Configuration</param>
		/// <param name="storage">the storage directory to transfer the image from</param>
		/// <param name="nnf">the NameNodeFile type of the image</param>
		/// <param name="txid">the transaction ID of the image to be uploaded</param>
		/// <exception cref="System.IO.IOException">if there is an I/O error</exception>
		public static void UploadImageFromStorage(Uri fsName, Configuration conf, NNStorage
			 storage, NNStorage.NameNodeFile nnf, long txid)
		{
			UploadImageFromStorage(fsName, conf, storage, nnf, txid, null);
		}

		/// <summary>Requests that the NameNode download an image from this node.</summary>
		/// <remarks>
		/// Requests that the NameNode download an image from this node.  Allows for
		/// optional external cancelation.
		/// </remarks>
		/// <param name="fsName">the http address for the remote NN</param>
		/// <param name="conf">Configuration</param>
		/// <param name="storage">the storage directory to transfer the image from</param>
		/// <param name="nnf">the NameNodeFile type of the image</param>
		/// <param name="txid">the transaction ID of the image to be uploaded</param>
		/// <param name="canceler">optional canceler to check for abort of upload</param>
		/// <exception cref="System.IO.IOException">if there is an I/O error or cancellation</exception>
		public static void UploadImageFromStorage(Uri fsName, Configuration conf, NNStorage
			 storage, NNStorage.NameNodeFile nnf, long txid, Canceler canceler)
		{
			Uri url = new Uri(fsName, ImageServlet.PathSpec);
			long startTime = Time.MonotonicNow();
			try
			{
				UploadImage(url, conf, storage, nnf, txid, canceler);
			}
			catch (TransferFsImage.HttpPutFailedException e)
			{
				if (e.GetResponseCode() == HttpServletResponse.ScConflict)
				{
					// this is OK - this means that a previous attempt to upload
					// this checkpoint succeeded even though we thought it failed.
					Log.Info("Image upload with txid " + txid + " conflicted with a previous image upload to the "
						 + "same NameNode. Continuing...", e);
					return;
				}
				else
				{
					throw;
				}
			}
			double xferSec = Math.Max(((float)(Time.MonotonicNow() - startTime)) / 1000.0, 0.001
				);
			Log.Info("Uploaded image with txid " + txid + " to namenode at " + fsName + " in "
				 + xferSec + " seconds");
		}

		/*
		* Uploads the imagefile using HTTP PUT method
		*/
		/// <exception cref="System.IO.IOException"/>
		private static void UploadImage(Uri url, Configuration conf, NNStorage storage, NNStorage.NameNodeFile
			 nnf, long txId, Canceler canceler)
		{
			FilePath imageFile = storage.FindImageFile(nnf, txId);
			if (imageFile == null)
			{
				throw new IOException("Could not find image with txid " + txId);
			}
			HttpURLConnection connection = null;
			try
			{
				URIBuilder uriBuilder = new URIBuilder(url.ToURI());
				// write all params for image upload request as query itself.
				// Request body contains the image to be uploaded.
				IDictionary<string, string> @params = ImageServlet.GetParamsForPutImage(storage, 
					txId, imageFile.Length(), nnf);
				foreach (KeyValuePair<string, string> entry in @params)
				{
					uriBuilder.AddParameter(entry.Key, entry.Value);
				}
				Uri urlWithParams = uriBuilder.Build().ToURL();
				connection = (HttpURLConnection)connectionFactory.OpenConnection(urlWithParams, UserGroupInformation
					.IsSecurityEnabled());
				// Set the request to PUT
				connection.SetRequestMethod("PUT");
				connection.SetDoOutput(true);
				int chunkSize = conf.GetInt(DFSConfigKeys.DfsImageTransferChunksizeKey, DFSConfigKeys
					.DfsImageTransferChunksizeDefault);
				if (imageFile.Length() > chunkSize)
				{
					// using chunked streaming mode to support upload of 2GB+ files and to
					// avoid internal buffering.
					// this mode should be used only if more than chunkSize data is present
					// to upload. otherwise upload may not happen sometimes.
					connection.SetChunkedStreamingMode(chunkSize);
				}
				SetTimeout(connection);
				// set headers for verification
				ImageServlet.SetVerificationHeadersForPut(connection, imageFile);
				// Write the file to output stream.
				WriteFileToPutRequest(conf, connection, imageFile, canceler);
				int responseCode = connection.GetResponseCode();
				if (responseCode != HttpURLConnection.HttpOk)
				{
					throw new TransferFsImage.HttpPutFailedException(string.Format("Image uploading failed, status: %d, url: %s, message: %s"
						, responseCode, urlWithParams, connection.GetResponseMessage()), responseCode);
				}
			}
			catch (AuthenticationException e)
			{
				throw new IOException(e);
			}
			catch (URISyntaxException e)
			{
				throw new IOException(e);
			}
			finally
			{
				if (connection != null)
				{
					connection.Disconnect();
				}
			}
		}

		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		private static void WriteFileToPutRequest(Configuration conf, HttpURLConnection connection
			, FilePath imageFile, Canceler canceler)
		{
			connection.SetRequestProperty(ContentType, "application/octet-stream");
			connection.SetRequestProperty(ContentTransferEncoding, "binary");
			OutputStream output = connection.GetOutputStream();
			FileInputStream input = new FileInputStream(imageFile);
			try
			{
				CopyFileToStream(output, imageFile, input, ImageServlet.GetThrottler(conf), canceler
					);
			}
			finally
			{
				IOUtils.CloseStream(input);
				IOUtils.CloseStream(output);
			}
		}

		/// <summary>
		/// A server-side method to respond to a getfile http request
		/// Copies the contents of the local file into the output stream.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public static void CopyFileToStream(OutputStream @out, FilePath localfile, FileInputStream
			 infile, DataTransferThrottler throttler)
		{
			CopyFileToStream(@out, localfile, infile, throttler, null);
		}

		/// <exception cref="System.IO.IOException"/>
		private static void CopyFileToStream(OutputStream @out, FilePath localfile, FileInputStream
			 infile, DataTransferThrottler throttler, Canceler canceler)
		{
			byte[] buf = new byte[HdfsConstants.IoFileBufferSize];
			try
			{
				CheckpointFaultInjector.GetInstance().AboutToSendFile(localfile);
				if (CheckpointFaultInjector.GetInstance().ShouldSendShortFile(localfile))
				{
					// Test sending image shorter than localfile
					long len = localfile.Length();
					buf = new byte[(int)Math.Min(len / 2, HdfsConstants.IoFileBufferSize)];
					// This will read at most half of the image
					// and the rest of the image will be sent over the wire
					infile.Read(buf);
				}
				int num = 1;
				while (num > 0)
				{
					if (canceler != null && canceler.IsCancelled())
					{
						throw new SaveNamespaceCancelledException(canceler.GetCancellationReason());
					}
					num = infile.Read(buf);
					if (num <= 0)
					{
						break;
					}
					if (CheckpointFaultInjector.GetInstance().ShouldCorruptAByte(localfile))
					{
						// Simulate a corrupted byte on the wire
						Log.Warn("SIMULATING A CORRUPT BYTE IN IMAGE TRANSFER!");
						buf[0]++;
					}
					@out.Write(buf, 0, num);
					if (throttler != null)
					{
						throttler.Throttle(num, canceler);
					}
				}
			}
			catch (EofException)
			{
				Log.Info("Connection closed by client");
				@out = null;
			}
			finally
			{
				// so we don't close in the finally
				if (@out != null)
				{
					@out.Close();
				}
			}
		}

		/// <summary>
		/// Client-side Method to fetch file from a server
		/// Copies the response from the URL to a list of local files.
		/// </summary>
		/// <param name="dstStorage">
		/// if an error occurs writing to one of the files,
		/// this storage object will be notified.
		/// </param>
		/// <Return>a digest of the received file if getChecksum is true</Return>
		/// <exception cref="System.IO.IOException"/>
		internal static MD5Hash GetFileClient(Uri infoServer, string queryString, IList<FilePath
			> localPaths, Storage dstStorage, bool getChecksum)
		{
			Uri url = new Uri(infoServer, ImageServlet.PathSpec + "?" + queryString);
			Log.Info("Opening connection to " + url);
			return DoGetUrl(url, localPaths, dstStorage, getChecksum);
		}

		/// <exception cref="System.IO.IOException"/>
		public static MD5Hash DoGetUrl(Uri url, IList<FilePath> localPaths, Storage dstStorage
			, bool getChecksum)
		{
			HttpURLConnection connection;
			try
			{
				connection = (HttpURLConnection)connectionFactory.OpenConnection(url, isSpnegoEnabled
					);
			}
			catch (AuthenticationException e)
			{
				throw new IOException(e);
			}
			SetTimeout(connection);
			if (connection.GetResponseCode() != HttpURLConnection.HttpOk)
			{
				throw new TransferFsImage.HttpGetFailedException("Image transfer servlet at " + url
					 + " failed with status code " + connection.GetResponseCode() + "\nResponse message:\n"
					 + connection.GetResponseMessage(), connection);
			}
			long advertisedSize;
			string contentLength = connection.GetHeaderField(ContentLength);
			if (contentLength != null)
			{
				advertisedSize = long.Parse(contentLength);
			}
			else
			{
				throw new IOException(ContentLength + " header is not provided " + "by the namenode when trying to fetch "
					 + url);
			}
			MD5Hash advertisedDigest = ParseMD5Header(connection);
			string fsImageName = connection.GetHeaderField(ImageServlet.HadoopImageEditsHeader
				);
			InputStream stream = connection.GetInputStream();
			return ReceiveFile(url.ToExternalForm(), localPaths, dstStorage, getChecksum, advertisedSize
				, advertisedDigest, fsImageName, stream, null);
		}

		private static void SetTimeout(HttpURLConnection connection)
		{
			if (timeout <= 0)
			{
				Configuration conf = new HdfsConfiguration();
				timeout = conf.GetInt(DFSConfigKeys.DfsImageTransferTimeoutKey, DFSConfigKeys.DfsImageTransferTimeoutDefault
					);
				Log.Info("Image Transfer timeout configured to " + timeout + " milliseconds");
			}
			if (timeout > 0)
			{
				connection.SetConnectTimeout(timeout);
				connection.SetReadTimeout(timeout);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static MD5Hash ReceiveFile(string url, IList<FilePath> localPaths, Storage
			 dstStorage, bool getChecksum, long advertisedSize, MD5Hash advertisedDigest, string
			 fsImageName, InputStream stream, DataTransferThrottler throttler)
		{
			long startTime = Time.MonotonicNow();
			if (localPaths != null)
			{
				// If the local paths refer to directories, use the server-provided header
				// as the filename within that directory
				IList<FilePath> newLocalPaths = new AList<FilePath>();
				foreach (FilePath localPath in localPaths)
				{
					if (localPath.IsDirectory())
					{
						if (fsImageName == null)
						{
							throw new IOException("No filename header provided by server");
						}
						newLocalPaths.AddItem(new FilePath(localPath, fsImageName));
					}
					else
					{
						newLocalPaths.AddItem(localPath);
					}
				}
				localPaths = newLocalPaths;
			}
			long received = 0;
			MessageDigest digester = null;
			if (getChecksum)
			{
				digester = MD5Hash.GetDigester();
				stream = new DigestInputStream(stream, digester);
			}
			bool finishedReceiving = false;
			IList<FileOutputStream> outputStreams = Lists.NewArrayList();
			try
			{
				if (localPaths != null)
				{
					foreach (FilePath f in localPaths)
					{
						try
						{
							if (f.Exists())
							{
								Log.Warn("Overwriting existing file " + f + " with file downloaded from " + url);
							}
							outputStreams.AddItem(new FileOutputStream(f));
						}
						catch (IOException ioe)
						{
							Log.Warn("Unable to download file " + f, ioe);
							// This will be null if we're downloading the fsimage to a file
							// outside of an NNStorage directory.
							if (dstStorage != null && (dstStorage is StorageErrorReporter))
							{
								((StorageErrorReporter)dstStorage).ReportErrorOnFile(f);
							}
						}
					}
					if (outputStreams.IsEmpty())
					{
						throw new IOException("Unable to download to any storage directory");
					}
				}
				int num = 1;
				byte[] buf = new byte[HdfsConstants.IoFileBufferSize];
				while (num > 0)
				{
					num = stream.Read(buf);
					if (num > 0)
					{
						received += num;
						foreach (FileOutputStream fos in outputStreams)
						{
							fos.Write(buf, 0, num);
						}
						if (throttler != null)
						{
							throttler.Throttle(num);
						}
					}
				}
				finishedReceiving = true;
			}
			finally
			{
				stream.Close();
				foreach (FileOutputStream fos in outputStreams)
				{
					fos.GetChannel().Force(true);
					fos.Close();
				}
				// Something went wrong and did not finish reading.
				// Remove the temporary files.
				if (!finishedReceiving)
				{
					DeleteTmpFiles(localPaths);
				}
				if (finishedReceiving && received != advertisedSize)
				{
					// only throw this exception if we think we read all of it on our end
					// -- otherwise a client-side IOException would be masked by this
					// exception that makes it look like a server-side problem!
					DeleteTmpFiles(localPaths);
					throw new IOException("File " + url + " received length " + received + " is not of the advertised size "
						 + advertisedSize);
				}
			}
			double xferSec = Math.Max(((float)(Time.MonotonicNow() - startTime)) / 1000.0, 0.001
				);
			long xferKb = received / 1024;
			Log.Info(string.Format("Transfer took %.2fs at %.2f KB/s", xferSec, xferKb / xferSec
				));
			if (digester != null)
			{
				MD5Hash computedDigest = new MD5Hash(digester.Digest());
				if (advertisedDigest != null && !computedDigest.Equals(advertisedDigest))
				{
					DeleteTmpFiles(localPaths);
					throw new IOException("File " + url + " computed digest " + computedDigest + " does not match advertised digest "
						 + advertisedDigest);
				}
				return computedDigest;
			}
			else
			{
				return null;
			}
		}

		private static void DeleteTmpFiles(IList<FilePath> files)
		{
			if (files == null)
			{
				return;
			}
			Log.Info("Deleting temporary files: " + files);
			foreach (FilePath file in files)
			{
				if (!file.Delete())
				{
					Log.Warn("Deleting " + file + " has failed");
				}
			}
		}

		private static MD5Hash ParseMD5Header(HttpURLConnection connection)
		{
			string header = connection.GetHeaderField(Md5Header);
			return (header != null) ? new MD5Hash(header) : null;
		}

		private static MD5Hash ParseMD5Header(HttpServletRequest request)
		{
			string header = request.GetHeader(Md5Header);
			return (header != null) ? new MD5Hash(header) : null;
		}

		[System.Serializable]
		public class HttpGetFailedException : IOException
		{
			private const long serialVersionUID = 1L;

			private readonly int responseCode;

			/// <exception cref="System.IO.IOException"/>
			internal HttpGetFailedException(string msg, HttpURLConnection connection)
				: base(msg)
			{
				this.responseCode = connection.GetResponseCode();
			}

			public virtual int GetResponseCode()
			{
				return responseCode;
			}
		}

		[System.Serializable]
		public class HttpPutFailedException : IOException
		{
			private const long serialVersionUID = 1L;

			private readonly int responseCode;

			/// <exception cref="System.IO.IOException"/>
			internal HttpPutFailedException(string msg, int responseCode)
				: base(msg)
			{
				this.responseCode = responseCode;
			}

			public virtual int GetResponseCode()
			{
				return responseCode;
			}
		}
	}
}
