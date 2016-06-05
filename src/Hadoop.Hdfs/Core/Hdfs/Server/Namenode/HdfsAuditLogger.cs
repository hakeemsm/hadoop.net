using System.Net;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// Extension of
	/// <see cref="AuditLogger"/>
	/// .
	/// </summary>
	public abstract class HdfsAuditLogger : AuditLogger
	{
		public virtual void LogAuditEvent(bool succeeded, string userName, IPAddress addr
			, string cmd, string src, string dst, FileStatus status)
		{
			LogAuditEvent(succeeded, userName, addr, cmd, src, dst, status, null, null);
		}

		/// <summary>
		/// Same as
		/// <see cref="LogAuditEvent(bool, string, System.Net.IPAddress, string, string, string, Org.Apache.Hadoop.FS.FileStatus)
		/// 	"/>
		/// with additional parameters related to logging delegation token tracking
		/// IDs.
		/// </summary>
		/// <param name="succeeded">Whether authorization succeeded.</param>
		/// <param name="userName">Name of the user executing the request.</param>
		/// <param name="addr">Remote address of the request.</param>
		/// <param name="cmd">The requested command.</param>
		/// <param name="src">Path of affected source file.</param>
		/// <param name="dst">Path of affected destination file (if any).</param>
		/// <param name="stat">
		/// File information for operations that change the file's metadata
		/// (permissions, owner, times, etc).
		/// </param>
		/// <param name="ugi">
		/// UserGroupInformation of the current user, or null if not logging
		/// token tracking information
		/// </param>
		/// <param name="dtSecretManager">
		/// The token secret manager, or null if not logging
		/// token tracking information
		/// </param>
		public abstract void LogAuditEvent(bool succeeded, string userName, IPAddress addr
			, string cmd, string src, string dst, FileStatus stat, UserGroupInformation ugi, 
			DelegationTokenSecretManager dtSecretManager);

		public abstract void Initialize(Configuration arg1);
	}
}
