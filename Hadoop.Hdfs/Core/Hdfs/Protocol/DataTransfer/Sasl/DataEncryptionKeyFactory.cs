using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer.Sasl
{
	/// <summary>
	/// Creates a new
	/// <see cref="Org.Apache.Hadoop.Hdfs.Security.Token.Block.DataEncryptionKey"/>
	/// on demand.
	/// </summary>
	public interface DataEncryptionKeyFactory
	{
		/// <summary>Creates a new DataEncryptionKey.</summary>
		/// <returns>DataEncryptionKey newly created</returns>
		/// <exception cref="System.IO.IOException">for any error</exception>
		DataEncryptionKey NewDataEncryptionKey();
	}
}
