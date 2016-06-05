using System.Net;
using System.Text;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	/// <summary>Manages NodeManager audit logs.</summary>
	/// <remarks>
	/// Manages NodeManager audit logs.
	/// Audit log format is written as key=value pairs. Tab separated.
	/// </remarks>
	public class NMAuditLogger
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(NMAuditLogger));

		internal enum Keys
		{
			User,
			Operation,
			Target,
			Result,
			Ip,
			Description,
			Appid,
			Containerid
		}

		public class AuditConstants
		{
			internal const string Success = "SUCCESS";

			internal const string Failure = "FAILURE";

			internal const string KeyValSeparator = "=";

			internal const char PairSeparator = '\t';

			public const string StartContainer = "Start Container Request";

			public const string StopContainer = "Stop Container Request";

			public const string FinishSuccessContainer = "Container Finished - Succeeded";

			public const string FinishFailedContainer = "Container Finished - Failed";

			public const string FinishKilledContainer = "Container Finished - Killed";
			// Some commonly used descriptions
		}

		/// <summary>A helper api for creating an audit log for a successful event.</summary>
		internal static string CreateSuccessLog(string user, string operation, string target
			, ApplicationId appId, ContainerId containerId)
		{
			StringBuilder b = new StringBuilder();
			Start(NMAuditLogger.Keys.User, user, b);
			AddRemoteIP(b);
			Add(NMAuditLogger.Keys.Operation, operation, b);
			Add(NMAuditLogger.Keys.Target, target, b);
			Add(NMAuditLogger.Keys.Result, NMAuditLogger.AuditConstants.Success, b);
			if (appId != null)
			{
				Add(NMAuditLogger.Keys.Appid, appId.ToString(), b);
			}
			if (containerId != null)
			{
				Add(NMAuditLogger.Keys.Containerid, containerId.ToString(), b);
			}
			return b.ToString();
		}

		/// <summary>Create a readable and parseable audit log string for a successful event.
		/// 	</summary>
		/// <param name="user">User who made the service request.</param>
		/// <param name="operation">Operation requested by the user</param>
		/// <param name="target">The target on which the operation is being performed.</param>
		/// <param name="appId">Application Id in which operation was performed.</param>
		/// <param name="containerId">
		/// Container Id in which operation was performed.
		/// <br /><br />
		/// Note that the
		/// <see cref="NMAuditLogger"/>
		/// uses tabs ('\t') as a key-val delimiter
		/// and hence the value fields should not contains tabs ('\t').
		/// </param>
		public static void LogSuccess(string user, string operation, string target, ApplicationId
			 appId, ContainerId containerId)
		{
			if (Log.IsInfoEnabled())
			{
				Log.Info(CreateSuccessLog(user, operation, target, appId, containerId));
			}
		}

		/// <summary>Create a readable and parseable audit log string for a successful event.
		/// 	</summary>
		/// <param name="user">User who made the service request.</param>
		/// <param name="operation">Operation requested by the user</param>
		/// <param name="target">
		/// The target on which the operation is being performed.
		/// <br /><br />
		/// Note that the
		/// <see cref="NMAuditLogger"/>
		/// uses tabs ('\t') as a key-val delimiter
		/// and hence the value fields should not contains tabs ('\t').
		/// </param>
		public static void LogSuccess(string user, string operation, string target)
		{
			if (Log.IsInfoEnabled())
			{
				Log.Info(CreateSuccessLog(user, operation, target, null, null));
			}
		}

		/// <summary>A helper api for creating an audit log for a failure event.</summary>
		/// <remarks>
		/// A helper api for creating an audit log for a failure event.
		/// This is factored out for testing purpose.
		/// </remarks>
		internal static string CreateFailureLog(string user, string operation, string target
			, string description, ApplicationId appId, ContainerId containerId)
		{
			StringBuilder b = new StringBuilder();
			Start(NMAuditLogger.Keys.User, user, b);
			AddRemoteIP(b);
			Add(NMAuditLogger.Keys.Operation, operation, b);
			Add(NMAuditLogger.Keys.Target, target, b);
			Add(NMAuditLogger.Keys.Result, NMAuditLogger.AuditConstants.Failure, b);
			Add(NMAuditLogger.Keys.Description, description, b);
			if (appId != null)
			{
				Add(NMAuditLogger.Keys.Appid, appId.ToString(), b);
			}
			if (containerId != null)
			{
				Add(NMAuditLogger.Keys.Containerid, containerId.ToString(), b);
			}
			return b.ToString();
		}

		/// <summary>Create a readable and parseable audit log string for a failed event.</summary>
		/// <param name="user">User who made the service request.</param>
		/// <param name="operation">Operation requested by the user.</param>
		/// <param name="target">The target on which the operation is being performed.</param>
		/// <param name="description">
		/// Some additional information as to why the operation
		/// failed.
		/// </param>
		/// <param name="appId">ApplicationId in which operation was performed.</param>
		/// <param name="containerId">
		/// Container Id in which operation was performed.
		/// <br /><br />
		/// Note that the
		/// <see cref="NMAuditLogger"/>
		/// uses tabs ('\t') as a key-val delimiter
		/// and hence the value fields should not contains tabs ('\t').
		/// </param>
		public static void LogFailure(string user, string operation, string target, string
			 description, ApplicationId appId, ContainerId containerId)
		{
			if (Log.IsWarnEnabled())
			{
				Log.Warn(CreateFailureLog(user, operation, target, description, appId, containerId
					));
			}
		}

		/// <summary>Create a readable and parseable audit log string for a failed event.</summary>
		/// <param name="user">User who made the service request.</param>
		/// <param name="operation">Operation requested by the user.</param>
		/// <param name="target">The target on which the operation is being performed.</param>
		/// <param name="description">
		/// Some additional information as to why the operation
		/// failed.
		/// <br /><br />
		/// Note that the
		/// <see cref="NMAuditLogger"/>
		/// uses tabs ('\t') as a key-val delimiter
		/// and hence the value fields should not contains tabs ('\t').
		/// </param>
		public static void LogFailure(string user, string operation, string target, string
			 description)
		{
			if (Log.IsWarnEnabled())
			{
				Log.Warn(CreateFailureLog(user, operation, target, description, null, null));
			}
		}

		/// <summary>A helper api to add remote IP address</summary>
		internal static void AddRemoteIP(StringBuilder b)
		{
			IPAddress ip = Org.Apache.Hadoop.Ipc.Server.GetRemoteIp();
			// ip address can be null for testcases
			if (ip != null)
			{
				Add(NMAuditLogger.Keys.Ip, ip.GetHostAddress(), b);
			}
		}

		/// <summary>
		/// Adds the first key-val pair to the passed builder in the following format
		/// key=value
		/// </summary>
		internal static void Start(NMAuditLogger.Keys key, string value, StringBuilder b)
		{
			b.Append(key.ToString()).Append(NMAuditLogger.AuditConstants.KeyValSeparator).Append
				(value);
		}

		/// <summary>
		/// Appends the key-val pair to the passed builder in the following format
		/// <pair-delim>key=value
		/// </summary>
		internal static void Add(NMAuditLogger.Keys key, string value, StringBuilder b)
		{
			b.Append(NMAuditLogger.AuditConstants.PairSeparator).Append(key.ToString()).Append
				(NMAuditLogger.AuditConstants.KeyValSeparator).Append(value);
		}
	}
}
