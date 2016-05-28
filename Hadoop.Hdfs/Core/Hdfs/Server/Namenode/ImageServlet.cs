using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Javax.Servlet;
using Javax.Servlet.Http;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Metrics;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// This class is used in Namesystem's jetty to retrieve/upload a file
	/// Typically used by the Secondary NameNode to retrieve image and
	/// edit file for periodic checkpointing in Non-HA deployments.
	/// </summary>
	/// <remarks>
	/// This class is used in Namesystem's jetty to retrieve/upload a file
	/// Typically used by the Secondary NameNode to retrieve image and
	/// edit file for periodic checkpointing in Non-HA deployments.
	/// Standby NameNode uses to upload checkpoints in HA deployments.
	/// </remarks>
	[System.Serializable]
	public class ImageServlet : HttpServlet
	{
		public const string PathSpec = "/imagetransfer";

		private const long serialVersionUID = -7669068179452648952L;

		private static readonly Log Log = LogFactory.GetLog(typeof(ImageServlet));

		public const string ContentDisposition = "Content-Disposition";

		public const string HadoopImageEditsHeader = "X-Image-Edits-Name";

		private const string TxidParam = "txid";

		private const string StartTxidParam = "startTxId";

		private const string EndTxidParam = "endTxId";

		private const string StorageinfoParam = "storageInfo";

		private const string LatestFsimageValue = "latest";

		private const string ImageFileType = "imageFile";

		/// <exception cref="Javax.Servlet.ServletException"/>
		/// <exception cref="System.IO.IOException"/>
		protected override void DoGet(HttpServletRequest request, HttpServletResponse response
			)
		{
			try
			{
				ServletContext context = GetServletContext();
				FSImage nnImage = NameNodeHttpServer.GetFsImageFromContext(context);
				ImageServlet.GetImageParams parsedParams = new ImageServlet.GetImageParams(request
					, response);
				Configuration conf = (Configuration)context.GetAttribute(JspHelper.CurrentConf);
				NameNodeMetrics metrics = NameNode.GetNameNodeMetrics();
				ValidateRequest(context, conf, request, response, nnImage, parsedParams.GetStorageInfoString
					());
				UserGroupInformation.GetCurrentUser().DoAs(new _PrivilegedExceptionAction_98(parsedParams
					, nnImage, metrics, response, conf));
			}
			catch (Exception t)
			{
				// Metrics non-null only when used inside name node
				// Metrics non-null only when used inside name node
				// Potential race where the file was deleted while we were in the
				// process of setting headers!
				// It's possible the file could be deleted after this point, but
				// we've already opened the 'fis' stream.
				// It's also possible length could change, but this would be
				// detected by the client side as an inaccurate length header.
				// send file
				string errMsg = "GetImage failed. " + StringUtils.StringifyException(t);
				response.SendError(HttpServletResponse.ScGone, errMsg);
				throw new IOException(errMsg);
			}
			finally
			{
				response.GetOutputStream().Close();
			}
		}

		private sealed class _PrivilegedExceptionAction_98 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_98(ImageServlet.GetImageParams parsedParams, FSImage
				 nnImage, NameNodeMetrics metrics, HttpServletResponse response, Configuration conf
				)
			{
				this.parsedParams = parsedParams;
				this.nnImage = nnImage;
				this.metrics = metrics;
				this.response = response;
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				if (parsedParams.IsGetImage())
				{
					long txid = parsedParams.GetTxId();
					FilePath imageFile = null;
					string errorMessage = "Could not find image";
					if (parsedParams.ShouldFetchLatest())
					{
						imageFile = nnImage.GetStorage().GetHighestFsImageName();
					}
					else
					{
						errorMessage += " with txid " + txid;
						imageFile = nnImage.GetStorage().GetFsImage(txid, EnumSet.Of(NNStorage.NameNodeFile
							.Image, NNStorage.NameNodeFile.ImageRollback));
					}
					if (imageFile == null)
					{
						throw new IOException(errorMessage);
					}
					CheckpointFaultInjector.GetInstance().BeforeGetImageSetsHeaders();
					long start = Time.MonotonicNow();
					this.ServeFile(imageFile);
					if (metrics != null)
					{
						long elapsed = Time.MonotonicNow() - start;
						metrics.AddGetImage(elapsed);
					}
				}
				else
				{
					if (parsedParams.IsGetEdit())
					{
						long startTxId = parsedParams.GetStartTxId();
						long endTxId = parsedParams.GetEndTxId();
						FilePath editFile = nnImage.GetStorage().FindFinalizedEditsFile(startTxId, endTxId
							);
						long start = Time.MonotonicNow();
						this.ServeFile(editFile);
						if (metrics != null)
						{
							long elapsed = Time.MonotonicNow() - start;
							metrics.AddGetEdit(elapsed);
						}
					}
				}
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			private void ServeFile(FilePath file)
			{
				FileInputStream fis = new FileInputStream(file);
				try
				{
					ImageServlet.SetVerificationHeadersForGet(response, file);
					ImageServlet.SetFileNameHeaders(response, file);
					if (!file.Exists())
					{
						throw new FileNotFoundException(file.ToString());
					}
					TransferFsImage.CopyFileToStream(response.GetOutputStream(), file, fis, ImageServlet
						.GetThrottler(conf));
				}
				finally
				{
					IOUtils.CloseStream(fis);
				}
			}

			private readonly ImageServlet.GetImageParams parsedParams;

			private readonly FSImage nnImage;

			private readonly NameNodeMetrics metrics;

			private readonly HttpServletResponse response;

			private readonly Configuration conf;
		}

		/// <exception cref="System.IO.IOException"/>
		private void ValidateRequest(ServletContext context, Configuration conf, HttpServletRequest
			 request, HttpServletResponse response, FSImage nnImage, string theirStorageInfoString
			)
		{
			if (UserGroupInformation.IsSecurityEnabled() && !IsValidRequestor(context, request
				.GetUserPrincipal().GetName(), conf))
			{
				string errorMsg = "Only Namenode, Secondary Namenode, and administrators may access "
					 + "this servlet";
				response.SendError(HttpServletResponse.ScForbidden, errorMsg);
				Log.Warn("Received non-NN/SNN/administrator request for image or edits from " + request
					.GetUserPrincipal().GetName() + " at " + request.GetRemoteHost());
				throw new IOException(errorMsg);
			}
			string myStorageInfoString = nnImage.GetStorage().ToColonSeparatedString();
			if (theirStorageInfoString != null && !myStorageInfoString.Equals(theirStorageInfoString
				))
			{
				string errorMsg = "This namenode has storage info " + myStorageInfoString + " but the secondary expected "
					 + theirStorageInfoString;
				response.SendError(HttpServletResponse.ScForbidden, errorMsg);
				Log.Warn("Received an invalid request file transfer request " + "from a secondary with storage info "
					 + theirStorageInfoString);
				throw new IOException(errorMsg);
			}
		}

		public static void SetFileNameHeaders(HttpServletResponse response, FilePath file
			)
		{
			response.SetHeader(ContentDisposition, "attachment; filename=" + file.GetName());
			response.SetHeader(HadoopImageEditsHeader, file.GetName());
		}

		/// <summary>Construct a throttler from conf</summary>
		/// <param name="conf">configuration</param>
		/// <returns>a data transfer throttler</returns>
		public static DataTransferThrottler GetThrottler(Configuration conf)
		{
			long transferBandwidth = conf.GetLong(DFSConfigKeys.DfsImageTransferRateKey, DFSConfigKeys
				.DfsImageTransferRateDefault);
			DataTransferThrottler throttler = null;
			if (transferBandwidth > 0)
			{
				throttler = new DataTransferThrottler(transferBandwidth);
			}
			return throttler;
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal static bool IsValidRequestor(ServletContext context, string remoteUser, 
			Configuration conf)
		{
			if (remoteUser == null)
			{
				// This really shouldn't happen...
				Log.Warn("Received null remoteUser while authorizing access to getImage servlet");
				return false;
			}
			ICollection<string> validRequestors = new HashSet<string>();
			validRequestors.AddItem(SecurityUtil.GetServerPrincipal(conf.Get(DFSConfigKeys.DfsNamenodeKerberosPrincipalKey
				), NameNode.GetAddress(conf).GetHostName()));
			try
			{
				validRequestors.AddItem(SecurityUtil.GetServerPrincipal(conf.Get(DFSConfigKeys.DfsSecondaryNamenodeKerberosPrincipalKey
					), SecondaryNameNode.GetHttpAddress(conf).GetHostName()));
			}
			catch (Exception e)
			{
				// Don't halt if SecondaryNameNode principal could not be added.
				Log.Debug("SecondaryNameNode principal could not be added", e);
				string msg = string.Format("SecondaryNameNode principal not considered, %s = %s, %s = %s"
					, DFSConfigKeys.DfsSecondaryNamenodeKerberosPrincipalKey, conf.Get(DFSConfigKeys
					.DfsSecondaryNamenodeKerberosPrincipalKey), DFSConfigKeys.DfsNamenodeSecondaryHttpAddressKey
					, conf.GetTrimmed(DFSConfigKeys.DfsNamenodeSecondaryHttpAddressKey, DFSConfigKeys
					.DfsNamenodeSecondaryHttpAddressDefault));
				Log.Warn(msg);
			}
			if (HAUtil.IsHAEnabled(conf, DFSUtil.GetNamenodeNameServiceId(conf)))
			{
				Configuration otherNnConf = HAUtil.GetConfForOtherNode(conf);
				validRequestors.AddItem(SecurityUtil.GetServerPrincipal(otherNnConf.Get(DFSConfigKeys
					.DfsNamenodeKerberosPrincipalKey), NameNode.GetAddress(otherNnConf).GetHostName(
					)));
			}
			foreach (string v in validRequestors)
			{
				if (v != null && v.Equals(remoteUser))
				{
					Log.Info("ImageServlet allowing checkpointer: " + remoteUser);
					return true;
				}
			}
			if (HttpServer2.UserHasAdministratorAccess(context, remoteUser))
			{
				Log.Info("ImageServlet allowing administrator: " + remoteUser);
				return true;
			}
			Log.Info("ImageServlet rejecting: " + remoteUser);
			return false;
		}

		/// <summary>Set headers for content length, and, if available, md5.</summary>
		/// <exception cref="System.IO.IOException"></exception>
		public static void SetVerificationHeadersForGet(HttpServletResponse response, FilePath
			 file)
		{
			response.SetHeader(TransferFsImage.ContentLength, file.Length().ToString());
			MD5Hash hash = MD5FileUtils.ReadStoredMd5ForFile(file);
			if (hash != null)
			{
				response.SetHeader(TransferFsImage.Md5Header, hash.ToString());
			}
		}

		internal static string GetParamStringForMostRecentImage()
		{
			return "getimage=1&" + TxidParam + "=" + LatestFsimageValue;
		}

		internal static string GetParamStringForImage(NNStorage.NameNodeFile nnf, long txid
			, StorageInfo remoteStorageInfo)
		{
			string imageType = nnf == null ? string.Empty : "&" + ImageFileType + "=" + nnf.ToString
				();
			return "getimage=1&" + TxidParam + "=" + txid + imageType + "&" + StorageinfoParam
				 + "=" + remoteStorageInfo.ToColonSeparatedString();
		}

		internal static string GetParamStringForLog(RemoteEditLog log, StorageInfo remoteStorageInfo
			)
		{
			return "getedit=1&" + StartTxidParam + "=" + log.GetStartTxId() + "&" + EndTxidParam
				 + "=" + log.GetEndTxId() + "&" + StorageinfoParam + "=" + remoteStorageInfo.ToColonSeparatedString
				();
		}

		internal class GetImageParams
		{
			private bool isGetImage;

			private bool isGetEdit;

			private NNStorage.NameNodeFile nnf;

			private long startTxId;

			private long endTxId;

			private long txId;

			private string storageInfoString;

			private bool fetchLatest;

			/// <param name="request">the object from which this servlet reads the url contents</param>
			/// <param name="response">the object into which this servlet writes the url contents
			/// 	</param>
			/// <exception cref="System.IO.IOException">if the request is bad</exception>
			public GetImageParams(HttpServletRequest request, HttpServletResponse response)
			{
				IDictionary<string, string[]> pmap = request.GetParameterMap();
				isGetImage = isGetEdit = fetchLatest = false;
				foreach (KeyValuePair<string, string[]> entry in pmap)
				{
					string key = entry.Key;
					string[] val = entry.Value;
					if (key.Equals("getimage"))
					{
						isGetImage = true;
						try
						{
							txId = ServletUtil.ParseLongParam(request, TxidParam);
							string imageType = ServletUtil.GetParameter(request, ImageFileType);
							nnf = imageType == null ? NNStorage.NameNodeFile.Image : NNStorage.NameNodeFile.ValueOf
								(imageType);
						}
						catch (FormatException nfe)
						{
							if (request.GetParameter(TxidParam).Equals(LatestFsimageValue))
							{
								fetchLatest = true;
							}
							else
							{
								throw;
							}
						}
					}
					else
					{
						if (key.Equals("getedit"))
						{
							isGetEdit = true;
							startTxId = ServletUtil.ParseLongParam(request, StartTxidParam);
							endTxId = ServletUtil.ParseLongParam(request, EndTxidParam);
						}
						else
						{
							if (key.Equals(StorageinfoParam))
							{
								storageInfoString = val[0];
							}
						}
					}
				}
				int numGets = (isGetImage ? 1 : 0) + (isGetEdit ? 1 : 0);
				if ((numGets > 1) || (numGets == 0))
				{
					throw new IOException("Illegal parameters to TransferFsImage");
				}
			}

			public virtual string GetStorageInfoString()
			{
				return storageInfoString;
			}

			public virtual long GetTxId()
			{
				Preconditions.CheckState(isGetImage);
				return txId;
			}

			public virtual NNStorage.NameNodeFile GetNameNodeFile()
			{
				Preconditions.CheckState(isGetImage);
				return nnf;
			}

			public virtual long GetStartTxId()
			{
				Preconditions.CheckState(isGetEdit);
				return startTxId;
			}

			public virtual long GetEndTxId()
			{
				Preconditions.CheckState(isGetEdit);
				return endTxId;
			}

			internal virtual bool IsGetEdit()
			{
				return isGetEdit;
			}

			internal virtual bool IsGetImage()
			{
				return isGetImage;
			}

			internal virtual bool ShouldFetchLatest()
			{
				return fetchLatest;
			}
		}

		/// <summary>Set headers for image length and if available, md5.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal static void SetVerificationHeadersForPut(HttpURLConnection connection, FilePath
			 file)
		{
			connection.SetRequestProperty(TransferFsImage.ContentLength, file.Length().ToString
				());
			MD5Hash hash = MD5FileUtils.ReadStoredMd5ForFile(file);
			if (hash != null)
			{
				connection.SetRequestProperty(TransferFsImage.Md5Header, hash.ToString());
			}
		}

		/// <summary>Set the required parameters for uploading image</summary>
		/// <param name="httpMethod">instance of method to set the parameters</param>
		/// <param name="storage">colon separated storageInfo string</param>
		/// <param name="txid">txid of the image</param>
		/// <param name="imageFileSize">size of the imagefile to be uploaded</param>
		/// <param name="nnf">NameNodeFile Type</param>
		/// <returns>Returns map of parameters to be used with PUT request.</returns>
		internal static IDictionary<string, string> GetParamsForPutImage(Storage storage, 
			long txid, long imageFileSize, NNStorage.NameNodeFile nnf)
		{
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[TxidParam] = System.Convert.ToString(txid);
			@params[StorageinfoParam] = storage.ToColonSeparatedString();
			// setting the length of the file to be uploaded in separate property as
			// Content-Length only supports up to 2GB
			@params[TransferFsImage.FileLength] = System.Convert.ToString(imageFileSize);
			@params[ImageFileType] = nnf.ToString();
			return @params;
		}

		/// <exception cref="Javax.Servlet.ServletException"/>
		/// <exception cref="System.IO.IOException"/>
		protected override void DoPut(HttpServletRequest request, HttpServletResponse response
			)
		{
			try
			{
				ServletContext context = GetServletContext();
				FSImage nnImage = NameNodeHttpServer.GetFsImageFromContext(context);
				Configuration conf = (Configuration)GetServletContext().GetAttribute(JspHelper.CurrentConf
					);
				ImageServlet.PutImageParams parsedParams = new ImageServlet.PutImageParams(request
					, response, conf);
				NameNodeMetrics metrics = NameNode.GetNameNodeMetrics();
				ValidateRequest(context, conf, request, response, nnImage, parsedParams.GetStorageInfoString
					());
				UserGroupInformation.GetCurrentUser().DoAs(new _PrivilegedExceptionAction_458(parsedParams
					, nnImage, response, request, conf, metrics));
			}
			catch (Exception t)
			{
				// Metrics non-null only when used inside name node
				// Now that we have a new checkpoint, we might be able to
				// remove some old ones.
				string errMsg = "PutImage failed. " + StringUtils.StringifyException(t);
				response.SendError(HttpServletResponse.ScGone, errMsg);
				throw new IOException(errMsg);
			}
		}

		private sealed class _PrivilegedExceptionAction_458 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_458(ImageServlet.PutImageParams parsedParams, FSImage
				 nnImage, HttpServletResponse response, HttpServletRequest request, Configuration
				 conf, NameNodeMetrics metrics)
			{
				this.parsedParams = parsedParams;
				this.nnImage = nnImage;
				this.response = response;
				this.request = request;
				this.conf = conf;
				this.metrics = metrics;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				long txid = parsedParams.GetTxId();
				NNStorage.NameNodeFile nnf = parsedParams.GetNameNodeFile();
				if (!nnImage.AddToCheckpointing(txid))
				{
					response.SendError(HttpServletResponse.ScConflict, "Either current namenode is checkpointing or another"
						 + " checkpointer is already in the process of " + "uploading a checkpoint made at transaction ID "
						 + txid);
					return null;
				}
				try
				{
					if (nnImage.GetStorage().FindImageFile(nnf, txid) != null)
					{
						response.SendError(HttpServletResponse.ScConflict, "Either current namenode has checkpointed or "
							 + "another checkpointer already uploaded an " + "checkpoint for txid " + txid);
						return null;
					}
					InputStream stream = request.GetInputStream();
					try
					{
						long start = Time.MonotonicNow();
						MD5Hash downloadImageDigest = TransferFsImage.HandleUploadImageRequest(request, txid
							, nnImage.GetStorage(), stream, parsedParams.GetFileSize(), ImageServlet.GetThrottler
							(conf));
						nnImage.SaveDigestAndRenameCheckpointImage(nnf, txid, downloadImageDigest);
						if (metrics != null)
						{
							long elapsed = Time.MonotonicNow() - start;
							metrics.AddPutImage(elapsed);
						}
						nnImage.PurgeOldStorage(nnf);
					}
					finally
					{
						stream.Close();
					}
				}
				finally
				{
					nnImage.RemoveFromCheckpointing(txid);
				}
				return null;
			}

			private readonly ImageServlet.PutImageParams parsedParams;

			private readonly FSImage nnImage;

			private readonly HttpServletResponse response;

			private readonly HttpServletRequest request;

			private readonly Configuration conf;

			private readonly NameNodeMetrics metrics;
		}

		internal class PutImageParams
		{
			private long txId = -1;

			private string storageInfoString = null;

			private long fileSize = 0L;

			private NNStorage.NameNodeFile nnf;

			/// <exception cref="System.IO.IOException"/>
			public PutImageParams(HttpServletRequest request, HttpServletResponse response, Configuration
				 conf)
			{
				/*
				* Params required to handle put image request
				*/
				txId = ServletUtil.ParseLongParam(request, TxidParam);
				storageInfoString = ServletUtil.GetParameter(request, StorageinfoParam);
				fileSize = ServletUtil.ParseLongParam(request, TransferFsImage.FileLength);
				string imageType = ServletUtil.GetParameter(request, ImageFileType);
				nnf = imageType == null ? NNStorage.NameNodeFile.Image : NNStorage.NameNodeFile.ValueOf
					(imageType);
				if (fileSize == 0 || txId == -1 || storageInfoString == null || storageInfoString
					.IsEmpty())
				{
					throw new IOException("Illegal parameters to TransferFsImage");
				}
			}

			public virtual long GetTxId()
			{
				return txId;
			}

			public virtual string GetStorageInfoString()
			{
				return storageInfoString;
			}

			public virtual long GetFileSize()
			{
				return fileSize;
			}

			public virtual NNStorage.NameNodeFile GetNameNodeFile()
			{
				return nnf;
			}
		}
	}
}
