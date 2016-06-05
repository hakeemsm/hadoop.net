using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer
{
	/// <summary>A little struct class to wrap an InputStream and an OutputStream.</summary>
	public class IOStreamPair
	{
		public readonly InputStream @in;

		public readonly OutputStream @out;

		public IOStreamPair(InputStream @in, OutputStream @out)
		{
			this.@in = @in;
			this.@out = @out;
		}
	}
}
