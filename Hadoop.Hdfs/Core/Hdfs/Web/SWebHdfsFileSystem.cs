using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web
{
	public class SWebHdfsFileSystem : WebHdfsFileSystem
	{
		public static readonly Text TokenKind = new Text("SWEBHDFS delegation");

		public const string Scheme = "swebhdfs";

		public override string GetScheme()
		{
			return Scheme;
		}

		protected internal override string GetTransportScheme()
		{
			return "https";
		}

		protected internal override Text GetTokenKind()
		{
			return TokenKind;
		}

		[VisibleForTesting]
		protected override int GetDefaultPort()
		{
			return GetConf().GetInt(DFSConfigKeys.DfsNamenodeHttpsPortKey, DFSConfigKeys.DfsNamenodeHttpsPortDefault
				);
		}
	}
}
