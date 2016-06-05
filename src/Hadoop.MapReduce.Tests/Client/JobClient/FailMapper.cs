using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop
{
	public class FailMapper : MapReduceBase, Mapper<WritableComparable, Writable, WritableComparable
		, Writable>
	{
		// Mapper that fails
		/// <exception cref="System.IO.IOException"/>
		public virtual void Map(WritableComparable key, Writable value, OutputCollector<WritableComparable
			, Writable> @out, Reporter reporter)
		{
			// NOTE- the next line is required for the TestDebugScript test to succeed
			System.Console.Error.WriteLine("failing map");
			throw new RuntimeException("failing map");
		}
	}
}
