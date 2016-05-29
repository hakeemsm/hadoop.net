using System.Net;
using System.Text;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	/// <summary>Manages ResourceManager audit logs.</summary>
	/// <remarks>
	/// Manages ResourceManager audit logs.
	/// Audit log format is written as key=value pairs. Tab separated.
	/// </remarks>
	public class RMAuditLogger
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(RMAuditLogger));

		internal enum Keys
		{
			User,
			Operation,
			Target,
			Result,
			Ip,
			Permissions,
			Description,
			Appid,
			Appattemptid,
			Containerid
		}

		public class AuditConstants
		{
			internal const string Success = "SUCCESS";

			internal const string Failure = "FAILURE";

			internal const string KeyValSeparator = "=";

			internal const char PairSeparator = '\t';

			public const string KillAppRequest = "Kill Application Request";

			public const string SubmitAppRequest = "Submit Application Request";

			public const string MoveAppRequest = "Move Application Request";

			public const string FinishSuccessApp = "Application Finished - Succeeded";

			public const string FinishFailedApp = "Application Finished - Failed";

			public const string FinishKilledApp = "Application Finished - Killed";

			public const string RegisterAm = "Register App Master";

			public const string AmAllocate = "App Master Heartbeats";

			public const string UnregisterAm = "Unregister App Master";

			public const string AllocContainer = "AM Allocated Container";

			public const string ReleaseContainer = "AM Released Container";

			public const string UnauthorizedUser = "Unauthorized user";

			public const string SubmitReservationRequest = "Submit Reservation Request";

			public const string UpdateReservationRequest = "Update Reservation Request";

			public const string DeleteReservationRequest = "Delete Reservation Request";
			// Some commonly used descriptions
			// For Reservation system
		}

		/// <summary>A helper api for creating an audit log for a successful event.</summary>
		internal static string CreateSuccessLog(string user, string operation, string target
			, ApplicationId appId, ApplicationAttemptId attemptId, ContainerId containerId)
		{
			StringBuilder b = new StringBuilder();
			Start(RMAuditLogger.Keys.User, user, b);
			AddRemoteIP(b);
			Add(RMAuditLogger.Keys.Operation, operation, b);
			Add(RMAuditLogger.Keys.Target, target, b);
			Add(RMAuditLogger.Keys.Result, RMAuditLogger.AuditConstants.Success, b);
			if (appId != null)
			{
				Add(RMAuditLogger.Keys.Appid, appId.ToString(), b);
			}
			if (attemptId != null)
			{
				Add(RMAuditLogger.Keys.Appattemptid, attemptId.ToString(), b);
			}
			if (containerId != null)
			{
				Add(RMAuditLogger.Keys.Containerid, containerId.ToString(), b);
			}
			return b.ToString();
		}

		/// <summary>Create a readable and parseable audit log string for a successful event.
		/// 	</summary>
		/// <param name="user">User who made the service request to the ResourceManager</param>
		/// <param name="operation">Operation requested by the user.</param>
		/// <param name="target">The target on which the operation is being performed.</param>
		/// <param name="appId">Application Id in which operation was performed.</param>
		/// <param name="containerId">
		/// Container Id in which operation was performed.
		/// <br /><br />
		/// Note that the
		/// <see cref="RMAuditLogger"/>
		/// uses tabs ('\t') as a key-val delimiter
		/// and hence the value fields should not contains tabs ('\t').
		/// </param>
		public static void LogSuccess(string user, string operation, string target, ApplicationId
			 appId, ContainerId containerId)
		{
			if (Log.IsInfoEnabled())
			{
				Log.Info(CreateSuccessLog(user, operation, target, appId, null, containerId));
			}
		}

		/// <summary>Create a readable and parseable audit log string for a successful event.
		/// 	</summary>
		/// <param name="user">User who made the service request to the ResourceManager.</param>
		/// <param name="operation">Operation requested by the user.</param>
		/// <param name="target">The target on which the operation is being performed.</param>
		/// <param name="appId">Application Id in which operation was performed.</param>
		/// <param name="attemptId">
		/// Application Attempt Id in which operation was performed.
		/// <br /><br />
		/// Note that the
		/// <see cref="RMAuditLogger"/>
		/// uses tabs ('\t') as a key-val delimiter
		/// and hence the value fields should not contains tabs ('\t').
		/// </param>
		public static void LogSuccess(string user, string operation, string target, ApplicationId
			 appId, ApplicationAttemptId attemptId)
		{
			if (Log.IsInfoEnabled())
			{
				Log.Info(CreateSuccessLog(user, operation, target, appId, attemptId, null));
			}
		}

		/// <summary>Create a readable and parseable audit log string for a successful event.
		/// 	</summary>
		/// <param name="user">User who made the service request to the ResourceManager.</param>
		/// <param name="operation">Operation requested by the user.</param>
		/// <param name="target">The target on which the operation is being performed.</param>
		/// <param name="appId">
		/// Application Id in which operation was performed.
		/// <br /><br />
		/// Note that the
		/// <see cref="RMAuditLogger"/>
		/// uses tabs ('\t') as a key-val delimiter
		/// and hence the value fields should not contains tabs ('\t').
		/// </param>
		public static void LogSuccess(string user, string operation, string target, ApplicationId
			 appId)
		{
			if (Log.IsInfoEnabled())
			{
				Log.Info(CreateSuccessLog(user, operation, target, appId, null, null));
			}
		}

		/// <summary>Create a readable and parseable audit log string for a successful event.
		/// 	</summary>
		/// <param name="user">User who made the service request.</param>
		/// <param name="operation">Operation requested by the user.</param>
		/// <param name="target">
		/// The target on which the operation is being performed.
		/// <br /><br />
		/// Note that the
		/// <see cref="RMAuditLogger"/>
		/// uses tabs ('\t') as a key-val delimiter
		/// and hence the value fields should not contains tabs ('\t').
		/// </param>
		public static void LogSuccess(string user, string operation, string target)
		{
			if (Log.IsInfoEnabled())
			{
				Log.Info(CreateSuccessLog(user, operation, target, null, null, null));
			}
		}

		/// <summary>A helper api for creating an audit log for a failure event.</summary>
		internal static string CreateFailureLog(string user, string operation, string perm
			, string target, string description, ApplicationId appId, ApplicationAttemptId attemptId
			, ContainerId containerId)
		{
			StringBuilder b = new StringBuilder();
			Start(RMAuditLogger.Keys.User, user, b);
			AddRemoteIP(b);
			Add(RMAuditLogger.Keys.Operation, operation, b);
			Add(RMAuditLogger.Keys.Target, target, b);
			Add(RMAuditLogger.Keys.Result, RMAuditLogger.AuditConstants.Failure, b);
			Add(RMAuditLogger.Keys.Description, description, b);
			Add(RMAuditLogger.Keys.Permissions, perm, b);
			if (appId != null)
			{
				Add(RMAuditLogger.Keys.Appid, appId.ToString(), b);
			}
			if (attemptId != null)
			{
				Add(RMAuditLogger.Keys.Appattemptid, attemptId.ToString(), b);
			}
			if (containerId != null)
			{
				Add(RMAuditLogger.Keys.Containerid, containerId.ToString(), b);
			}
			return b.ToString();
		}

		/// <summary>Create a readable and parseable audit log string for a failed event.</summary>
		/// <param name="user">User who made the service request.</param>
		/// <param name="operation">Operation requested by the user.</param>
		/// <param name="perm">Target permissions.</param>
		/// <param name="target">The target on which the operation is being performed.</param>
		/// <param name="description">
		/// Some additional information as to why the operation
		/// failed.
		/// </param>
		/// <param name="appId">Application Id in which operation was performed.</param>
		/// <param name="containerId">
		/// Container Id in which operation was performed.
		/// <br /><br />
		/// Note that the
		/// <see cref="RMAuditLogger"/>
		/// uses tabs ('\t') as a key-val delimiter
		/// and hence the value fields should not contains tabs ('\t').
		/// </param>
		public static void LogFailure(string user, string operation, string perm, string 
			target, string description, ApplicationId appId, ContainerId containerId)
		{
			if (Log.IsWarnEnabled())
			{
				Log.Warn(CreateFailureLog(user, operation, perm, target, description, appId, null
					, containerId));
			}
		}

		/// <summary>Create a readable and parseable audit log string for a failed event.</summary>
		/// <param name="user">User who made the service request.</param>
		/// <param name="operation">Operation requested by the user.</param>
		/// <param name="perm">Target permissions.</param>
		/// <param name="target">The target on which the operation is being performed.</param>
		/// <param name="description">
		/// Some additional information as to why the operation
		/// failed.
		/// </param>
		/// <param name="appId">
		/// ApplicationId in which operation was performed.
		/// <br /><br />
		/// Note that the
		/// <see cref="RMAuditLogger"/>
		/// uses tabs ('\t') as a key-val delimiter
		/// and hence the value fields should not contains tabs ('\t').
		/// </param>
		public static void LogFailure(string user, string operation, string perm, string 
			target, string description, ApplicationId appId, ApplicationAttemptId attemptId)
		{
			if (Log.IsWarnEnabled())
			{
				Log.Warn(CreateFailureLog(user, operation, perm, target, description, appId, attemptId
					, null));
			}
		}

		/// <summary>Create a readable and parseable audit log string for a failed event.</summary>
		/// <param name="user">User who made the service request.</param>
		/// <param name="operation">Operation requested by the user.</param>
		/// <param name="perm">Target permissions.</param>
		/// <param name="target">The target on which the operation is being performed.</param>
		/// <param name="description">
		/// Some additional information as to why the operation
		/// failed.
		/// </param>
		/// <param name="appId">
		/// ApplicationId in which operation was performed.
		/// <br /><br />
		/// Note that the
		/// <see cref="RMAuditLogger"/>
		/// uses tabs ('\t') as a key-val delimiter
		/// and hence the value fields should not contains tabs ('\t').
		/// </param>
		public static void LogFailure(string user, string operation, string perm, string 
			target, string description, ApplicationId appId)
		{
			if (Log.IsWarnEnabled())
			{
				Log.Warn(CreateFailureLog(user, operation, perm, target, description, appId, null
					, null));
			}
		}

		/// <summary>Create a readable and parseable audit log string for a failed event.</summary>
		/// <param name="user">User who made the service request.</param>
		/// <param name="operation">Operation requested by the user.</param>
		/// <param name="perm">Target permissions.</param>
		/// <param name="target">The target on which the operation is being performed.</param>
		/// <param name="description">
		/// Some additional information as to why the operation
		/// failed.
		/// <br /><br />
		/// Note that the
		/// <see cref="RMAuditLogger"/>
		/// uses tabs ('\t') as a key-val delimiter
		/// and hence the value fields should not contains tabs ('\t').
		/// </param>
		public static void LogFailure(string user, string operation, string perm, string 
			target, string description)
		{
			if (Log.IsWarnEnabled())
			{
				Log.Warn(CreateFailureLog(user, operation, perm, target, description, null, null, 
					null));
			}
		}

		/// <summary>A helper api to add remote IP address</summary>
		internal static void AddRemoteIP(StringBuilder b)
		{
			IPAddress ip = Org.Apache.Hadoop.Ipc.Server.GetRemoteIp();
			// ip address can be null for testcases
			if (ip != null)
			{
				Add(RMAuditLogger.Keys.Ip, ip.GetHostAddress(), b);
			}
		}

		/// <summary>
		/// Adds the first key-val pair to the passed builder in the following format
		/// key=value
		/// </summary>
		internal static void Start(RMAuditLogger.Keys key, string value, StringBuilder b)
		{
			b.Append(key.ToString()).Append(RMAuditLogger.AuditConstants.KeyValSeparator).Append
				(value);
		}

		/// <summary>
		/// Appends the key-val pair to the passed builder in the following format
		/// <pair-delim>key=value
		/// </summary>
		internal static void Add(RMAuditLogger.Keys key, string value, StringBuilder b)
		{
			b.Append(RMAuditLogger.AuditConstants.PairSeparator).Append(key.ToString()).Append
				(RMAuditLogger.AuditConstants.KeyValSeparator).Append(value);
		}
	}
}
