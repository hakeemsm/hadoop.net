using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	public class KeyUpdateCommand : DatanodeCommand
	{
		private readonly ExportedBlockKeys keys;

		internal KeyUpdateCommand()
			: this(new ExportedBlockKeys())
		{
		}

		public KeyUpdateCommand(ExportedBlockKeys keys)
			: base(DatanodeProtocol.DnaAccesskeyupdate)
		{
			this.keys = keys;
		}

		public virtual ExportedBlockKeys GetExportedKeys()
		{
			return this.keys;
		}
	}
}
