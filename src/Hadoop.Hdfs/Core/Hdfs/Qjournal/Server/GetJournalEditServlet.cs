using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Javax.Servlet;
using Javax.Servlet.Http;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Qjournal.Client;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal.Server
{
	/// <summary>
	/// This servlet is used in two cases:
	/// <ul>
	/// <li>The QuorumJournalManager, when reading edits, fetches the edit streams
	/// from the journal nodes.</li>
	/// <li>During edits synchronization, one journal node will fetch edits from
	/// another journal node.</li>
	/// </ul>
	/// </summary>
	[System.Serializable]
	public class GetJournalEditServlet : HttpServlet
	{
		private const long serialVersionUID = -4635891628211723009L;

		private static readonly Log Log = LogFactory.GetLog(typeof(GetJournalEditServlet)
			);

		internal const string StorageinfoParam = "storageInfo";

		internal const string JournalIdParam = "jid";

		internal const string SegmentTxidParam = "segmentTxId";

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual bool IsValidRequestor(HttpServletRequest request, Configuration
			 conf)
		{
			string remotePrincipal = request.GetUserPrincipal().GetName();
			string remoteShortName = request.GetRemoteUser();
			if (remotePrincipal == null)
			{
				// This really shouldn't happen...
				Log.Warn("Received null remoteUser while authorizing access to " + "GetJournalEditServlet"
					);
				return false;
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Validating request made by " + remotePrincipal + " / " + remoteShortName
					 + ". This user is: " + UserGroupInformation.GetLoginUser());
			}
			ICollection<string> validRequestors = new HashSet<string>();
			Sharpen.Collections.AddAll(validRequestors, DFSUtil.GetAllNnPrincipals(conf));
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
					, conf.Get(DFSConfigKeys.DfsNamenodeSecondaryHttpAddressKey, DFSConfigKeys.DfsNamenodeSecondaryHttpAddressDefault
					));
				Log.Warn(msg);
			}
			// Check the full principal name of all the configured valid requestors.
			foreach (string v in validRequestors)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("isValidRequestor is comparing to valid requestor: " + v);
				}
				if (v != null && v.Equals(remotePrincipal))
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("isValidRequestor is allowing: " + remotePrincipal);
					}
					return true;
				}
			}
			// Additionally, we compare the short name of the requestor to this JN's
			// username, because we want to allow requests from other JNs during
			// recovery, but we can't enumerate the full list of JNs.
			if (remoteShortName.Equals(UserGroupInformation.GetLoginUser().GetShortUserName()
				))
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("isValidRequestor is allowing other JN principal: " + remotePrincipal);
				}
				return true;
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug("isValidRequestor is rejecting: " + remotePrincipal);
			}
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		private bool CheckRequestorOrSendError(Configuration conf, HttpServletRequest request
			, HttpServletResponse response)
		{
			if (UserGroupInformation.IsSecurityEnabled() && !IsValidRequestor(request, conf))
			{
				response.SendError(HttpServletResponse.ScForbidden, "Only Namenode and another JournalNode may access this servlet"
					);
				Log.Warn("Received non-NN/JN request for edits from " + request.GetRemoteHost());
				return false;
			}
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		private bool CheckStorageInfoOrSendError(JNStorage storage, HttpServletRequest request
			, HttpServletResponse response)
		{
			int myNsId = storage.GetNamespaceID();
			string myClusterId = storage.GetClusterID();
			string theirStorageInfoString = StringEscapeUtils.EscapeHtml(request.GetParameter
				(StorageinfoParam));
			if (theirStorageInfoString != null)
			{
				int theirNsId = StorageInfo.GetNsIdFromColonSeparatedString(theirStorageInfoString
					);
				string theirClusterId = StorageInfo.GetClusterIdFromColonSeparatedString(theirStorageInfoString
					);
				if (myNsId != theirNsId || !myClusterId.Equals(theirClusterId))
				{
					string msg = "This node has namespaceId '" + myNsId + " and clusterId '" + myClusterId
						 + "' but the requesting node expected '" + theirNsId + "' and '" + theirClusterId
						 + "'";
					response.SendError(HttpServletResponse.ScForbidden, msg);
					Log.Warn("Received an invalid request file transfer request from " + request.GetRemoteAddr
						() + ": " + msg);
					return false;
				}
			}
			return true;
		}

		/// <exception cref="Javax.Servlet.ServletException"/>
		/// <exception cref="System.IO.IOException"/>
		protected override void DoGet(HttpServletRequest request, HttpServletResponse response
			)
		{
			FileInputStream editFileIn = null;
			try
			{
				ServletContext context = GetServletContext();
				Configuration conf = (Configuration)GetServletContext().GetAttribute(JspHelper.CurrentConf
					);
				string journalId = request.GetParameter(JournalIdParam);
				QuorumJournalManager.CheckJournalId(journalId);
				JNStorage storage = JournalNodeHttpServer.GetJournalFromContext(context, journalId
					).GetStorage();
				// Check security
				if (!CheckRequestorOrSendError(conf, request, response))
				{
					return;
				}
				// Check that the namespace info is correct
				if (!CheckStorageInfoOrSendError(storage, request, response))
				{
					return;
				}
				long segmentTxId = ServletUtil.ParseLongParam(request, SegmentTxidParam);
				FileJournalManager fjm = storage.GetJournalManager();
				FilePath editFile;
				lock (fjm)
				{
					// Synchronize on the FJM so that the file doesn't get finalized
					// out from underneath us while we're in the process of opening
					// it up.
					FileJournalManager.EditLogFile elf = fjm.GetLogFile(segmentTxId);
					if (elf == null)
					{
						response.SendError(HttpServletResponse.ScNotFound, "No edit log found starting at txid "
							 + segmentTxId);
						return;
					}
					editFile = elf.GetFile();
					ImageServlet.SetVerificationHeadersForGet(response, editFile);
					ImageServlet.SetFileNameHeaders(response, editFile);
					editFileIn = new FileInputStream(editFile);
				}
				DataTransferThrottler throttler = ImageServlet.GetThrottler(conf);
				// send edits
				TransferFsImage.CopyFileToStream(response.GetOutputStream(), editFile, editFileIn
					, throttler);
			}
			catch (Exception t)
			{
				string errMsg = "getedit failed. " + StringUtils.StringifyException(t);
				response.SendError(HttpServletResponse.ScInternalServerError, errMsg);
				throw new IOException(errMsg);
			}
			finally
			{
				IOUtils.CloseStream(editFileIn);
			}
		}

		public static string BuildPath(string journalId, long segmentTxId, NamespaceInfo 
			nsInfo)
		{
			StringBuilder path = new StringBuilder("/getJournal?");
			try
			{
				path.Append(JournalIdParam).Append("=").Append(URLEncoder.Encode(journalId, "UTF-8"
					));
				path.Append("&" + SegmentTxidParam).Append("=").Append(segmentTxId);
				path.Append("&" + StorageinfoParam).Append("=").Append(URLEncoder.Encode(nsInfo.ToColonSeparatedString
					(), "UTF-8"));
			}
			catch (UnsupportedEncodingException e)
			{
				// Never get here -- everyone supports UTF-8
				throw new RuntimeException(e);
			}
			return path.ToString();
		}
	}
}
