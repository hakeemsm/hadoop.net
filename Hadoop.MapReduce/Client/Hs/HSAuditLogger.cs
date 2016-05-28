using System.Net;
using System.Text;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Ipc;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS
{
	public class HSAuditLogger
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(HSAuditLogger));

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

		public class AuditConstants
		{
			internal const string Success = "SUCCESS";

			internal const string Failure = "FAILURE";

			internal const string KeyValSeparator = "=";

			internal const char PairSeparator = '\t';

			public const string UnauthorizedUser = "Unauthorized user";
			// Some commonly used descriptions
		}

		/// <summary>Create a readable and parseable audit log string for a successful event.
		/// 	</summary>
		/// <param name="user">User who made the service request.</param>
		/// <param name="operation">Operation requested by the user.</param>
		/// <param name="target">
		/// The target on which the operation is being performed.
		/// <br />
		/// <br />
		/// Note that the
		/// <see cref="HSAuditLogger"/>
		/// uses tabs ('\t') as a key-val
		/// delimiter and hence the value fields should not contains tabs
		/// ('\t').
		/// </param>
		public static void LogSuccess(string user, string operation, string target)
		{
			if (Log.IsInfoEnabled())
			{
				Log.Info(CreateSuccessLog(user, operation, target));
			}
		}

		/// <summary>A helper api for creating an audit log for a successful event.</summary>
		internal static string CreateSuccessLog(string user, string operation, string target
			)
		{
			StringBuilder b = new StringBuilder();
			Start(HSAuditLogger.Keys.User, user, b);
			AddRemoteIP(b);
			Add(HSAuditLogger.Keys.Operation, operation, b);
			Add(HSAuditLogger.Keys.Target, target, b);
			Add(HSAuditLogger.Keys.Result, HSAuditLogger.AuditConstants.Success, b);
			return b.ToString();
		}

		/// <summary>A helper api to add remote IP address</summary>
		internal static void AddRemoteIP(StringBuilder b)
		{
			IPAddress ip = Server.GetRemoteIp();
			// ip address can be null for testcases
			if (ip != null)
			{
				Add(HSAuditLogger.Keys.Ip, ip.GetHostAddress(), b);
			}
		}

		/// <summary>
		/// Appends the key-val pair to the passed builder in the following format
		/// <pair-delim>key=value
		/// </summary>
		internal static void Add(HSAuditLogger.Keys key, string value, StringBuilder b)
		{
			b.Append(HSAuditLogger.AuditConstants.PairSeparator).Append(key.ToString()).Append
				(HSAuditLogger.AuditConstants.KeyValSeparator).Append(value);
		}

		/// <summary>
		/// Adds the first key-val pair to the passed builder in the following format
		/// key=value
		/// </summary>
		internal static void Start(HSAuditLogger.Keys key, string value, StringBuilder b)
		{
			b.Append(key.ToString()).Append(HSAuditLogger.AuditConstants.KeyValSeparator).Append
				(value);
		}

		/// <summary>Create a readable and parseable audit log string for a failed event.</summary>
		/// <param name="user">User who made the service request.</param>
		/// <param name="operation">Operation requested by the user.</param>
		/// <param name="perm">Target permissions.</param>
		/// <param name="target">The target on which the operation is being performed.</param>
		/// <param name="description">
		/// Some additional information as to why the operation failed.
		/// <br />
		/// <br />
		/// Note that the
		/// <see cref="HSAuditLogger"/>
		/// uses tabs ('\t') as a key-val
		/// delimiter and hence the value fields should not contains tabs
		/// ('\t').
		/// </param>
		public static void LogFailure(string user, string operation, string perm, string 
			target, string description)
		{
			if (Log.IsWarnEnabled())
			{
				Log.Warn(CreateFailureLog(user, operation, perm, target, description));
			}
		}

		/// <summary>A helper api for creating an audit log for a failure event.</summary>
		internal static string CreateFailureLog(string user, string operation, string perm
			, string target, string description)
		{
			StringBuilder b = new StringBuilder();
			Start(HSAuditLogger.Keys.User, user, b);
			AddRemoteIP(b);
			Add(HSAuditLogger.Keys.Operation, operation, b);
			Add(HSAuditLogger.Keys.Target, target, b);
			Add(HSAuditLogger.Keys.Result, HSAuditLogger.AuditConstants.Failure, b);
			Add(HSAuditLogger.Keys.Description, description, b);
			Add(HSAuditLogger.Keys.Permissions, perm, b);
			return b.ToString();
		}
	}
}
