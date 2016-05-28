using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web
{
	/// <summary>An implementation of a protocol for accessing filesystems over HTTPS.</summary>
	/// <remarks>
	/// An implementation of a protocol for accessing filesystems over HTTPS. The
	/// following implementation provides a limited, read-only interface to a
	/// filesystem over HTTPS.
	/// </remarks>
	/// <seealso cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.ListPathsServlet"/>
	/// <seealso cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.FileDataServlet"/>
	public class HsftpFileSystem : HftpFileSystem
	{
		public static readonly Text TokenKind = new Text("HSFTP delegation");

		public const string Scheme = "hsftp";

		/// <summary>Return the protocol scheme for the FileSystem.</summary>
		/// <remarks>
		/// Return the protocol scheme for the FileSystem.
		/// <p/>
		/// </remarks>
		/// <returns><code>hsftp</code></returns>
		public override string GetScheme()
		{
			return Scheme;
		}

		/// <summary>Return the underlying protocol that is used to talk to the namenode.</summary>
		protected internal override string GetUnderlyingProtocol()
		{
			return "https";
		}

		protected internal override void InitTokenAspect()
		{
			tokenAspect = new TokenAspect<HsftpFileSystem>(this, tokenServiceName, TokenKind);
		}

		protected override int GetDefaultPort()
		{
			return GetConf().GetInt(DFSConfigKeys.DfsNamenodeHttpsPortKey, DFSConfigKeys.DfsNamenodeHttpsPortDefault
				);
		}
	}
}
