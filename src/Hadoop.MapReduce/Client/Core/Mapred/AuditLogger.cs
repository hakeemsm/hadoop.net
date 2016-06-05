using System.Net;
using System.Text;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Ipc;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>Manages MapReduce audit logs.</summary>
	/// <remarks>
	/// Manages MapReduce audit logs. Audit logs provides information about
	/// authorization/authentication events (success/failure).
	/// Audit log format is written as key=value pairs.
	/// </remarks>
	internal class AuditLogger
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(AuditLogger));

		internal enum Keys
		{
			User,
			Operation,
			Target,
			Result,
			Ip,
			Permissions,
			Description
		}

		internal class Constants
		{
			internal const string Success = "SUCCESS";

			internal const string Failure = "FAILURE";

			internal const string KeyValSeparator = "=";

			internal const char PairSeparator = '\t';

			internal const string Jobtracker = "JobTracker";

			internal const string RefreshQueue = "REFRESH_QUEUE";

			internal const string RefreshNodes = "REFRESH_NODES";

			internal const string UnauthorizedUser = "Unauthorized user";
			// Some constants used by others using AuditLogger.
			// Some commonly used targets
			// Some commonly used operations
			// Some commonly used descriptions
		}

		/// <summary>A helper api for creating an audit log for a successful event.</summary>
		/// <remarks>
		/// A helper api for creating an audit log for a successful event.
		/// This is factored out for testing purpose.
		/// </remarks>
		internal static string CreateSuccessLog(string user, string operation, string target
			)
		{
			StringBuilder b = new StringBuilder();
			Start(AuditLogger.Keys.User, user, b);
			AddRemoteIP(b);
			Add(AuditLogger.Keys.Operation, operation, b);
			Add(AuditLogger.Keys.Target, target, b);
			Add(AuditLogger.Keys.Result, AuditLogger.Constants.Success, b);
			return b.ToString();
		}

		/// <summary>Create a readable and parseable audit log string for a successful event.
		/// 	</summary>
		/// <param name="user">User who made the service request to the JobTracker.</param>
		/// <param name="operation">Operation requested by the user</param>
		/// <param name="target">
		/// The target on which the operation is being performed. Most
		/// commonly operated targets are jobs, JobTracker, queues etc
		/// <br /><br />
		/// Note that the
		/// <see cref="AuditLogger"/>
		/// uses tabs ('\t') as a key-val delimiter
		/// and hence the value fields should not contains tabs ('\t').
		/// </param>
		internal static void LogSuccess(string user, string operation, string target)
		{
			if (Log.IsInfoEnabled())
			{
				Log.Info(CreateSuccessLog(user, operation, target));
			}
		}

		/// <summary>A helper api for creating an audit log for a failure event.</summary>
		/// <remarks>
		/// A helper api for creating an audit log for a failure event.
		/// This is factored out for testing purpose.
		/// </remarks>
		internal static string CreateFailureLog(string user, string operation, string perm
			, string target, string description)
		{
			StringBuilder b = new StringBuilder();
			Start(AuditLogger.Keys.User, user, b);
			AddRemoteIP(b);
			Add(AuditLogger.Keys.Operation, operation, b);
			Add(AuditLogger.Keys.Target, target, b);
			Add(AuditLogger.Keys.Result, AuditLogger.Constants.Failure, b);
			Add(AuditLogger.Keys.Description, description, b);
			Add(AuditLogger.Keys.Permissions, perm, b);
			return b.ToString();
		}

		/// <summary>Create a readable and parseable audit log string for a failed event.</summary>
		/// <param name="user">User who made the service request to the JobTracker.</param>
		/// <param name="operation">Operation requested by the user</param>
		/// <param name="perm">Target permissions like JobACLs for jobs, QueueACLs for queues.
		/// 	</param>
		/// <param name="target">
		/// The target on which the operation is being performed. Most
		/// commonly operated targets are jobs, JobTracker, queues etc
		/// </param>
		/// <param name="description">
		/// Some additional information as to why the operation
		/// failed.
		/// <br /><br />
		/// Note that the
		/// <see cref="AuditLogger"/>
		/// uses tabs ('\t') as a key-val delimiter
		/// and hence the value fields should not contains tabs ('\t').
		/// </param>
		internal static void LogFailure(string user, string operation, string perm, string
			 target, string description)
		{
			if (Log.IsWarnEnabled())
			{
				Log.Warn(CreateFailureLog(user, operation, perm, target, description));
			}
		}

		/// <summary>A helper api to add remote IP address</summary>
		internal static void AddRemoteIP(StringBuilder b)
		{
			IPAddress ip = Server.GetRemoteIp();
			// ip address can be null for testcases
			if (ip != null)
			{
				Add(AuditLogger.Keys.Ip, ip.GetHostAddress(), b);
			}
		}

		/// <summary>
		/// Adds the first key-val pair to the passed builder in the following format
		/// key=value
		/// </summary>
		internal static void Start(AuditLogger.Keys key, string value, StringBuilder b)
		{
			b.Append(key.ToString()).Append(AuditLogger.Constants.KeyValSeparator).Append(value
				);
		}

		/// <summary>
		/// Appends the key-val pair to the passed builder in the following format
		/// <pair-delim>key=value
		/// </summary>
		internal static void Add(AuditLogger.Keys key, string value, StringBuilder b)
		{
			b.Append(AuditLogger.Constants.PairSeparator).Append(key.ToString()).Append(AuditLogger.Constants
				.KeyValSeparator).Append(value);
		}
	}
}
