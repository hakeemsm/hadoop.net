using System.Net;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Interface defining an audit logger.</summary>
	public interface AuditLogger
	{
		/// <summary>Called during initialization of the logger.</summary>
		/// <param name="conf">The configuration object.</param>
		void Initialize(Configuration conf);

		/// <summary>Called to log an audit event.</summary>
		/// <remarks>
		/// Called to log an audit event.
		/// <p>
		/// This method must return as quickly as possible, since it's called
		/// in a critical section of the NameNode's operation.
		/// </remarks>
		/// <param name="succeeded">Whether authorization succeeded.</param>
		/// <param name="userName">Name of the user executing the request.</param>
		/// <param name="addr">Remote address of the request.</param>
		/// <param name="cmd">The requested command.</param>
		/// <param name="src">Path of affected source file.</param>
		/// <param name="dst">Path of affected destination file (if any).</param>
		/// <param name="stat">
		/// File information for operations that change the file's
		/// metadata (permissions, owner, times, etc).
		/// </param>
		void LogAuditEvent(bool succeeded, string userName, IPAddress addr, string cmd, string
			 src, string dst, FileStatus stat);
	}
}
