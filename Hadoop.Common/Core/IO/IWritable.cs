using System.IO;

namespace Hadoop.Common.Core.IO
{
	/// <summary>
	/// A serializable object which implements a simple, efficient, serialization
	/// protocol, based on
	/// <see cref="System.IO.BinaryWriter"/>
	/// and
	/// <see cref="System.IO.BinaryReader"/>
	/// .
	/// <p>Any <code>key</code> or <code>value</code> type in the Hadoop Map-Reduce
	/// framework implements this interface.</p>
	/// <p>Implementations typically implement a static <code>read(BinaryReader)</code>
	/// method which constructs a new instance, calls
	/// <see cref="ReadFields(System.IO.BinaryReader)"/>
	/// 
	/// and returns the instance.</p>
	/// <p>Example:</p>
	/// <p><blockquote><pre>
	/// public class MyWritable : IWritable {
	/// // Some data
	/// private int counter;
	/// private long timestamp;
	/// public void write(DataOutput out) throws IOException {
	/// out.writeInt(counter);
	/// out.writeLong(timestamp);
	/// }
	/// public void readFields(BinaryReader in) throws IOException {
	/// counter = in.readInt();
	/// timestamp = in.readLong();
	/// }
	/// public static MyWritable read(BinaryReader in) throws IOException {
	/// MyWritable w = new MyWritable();
	/// w.readFields(in);
	/// return w;
	/// }
	/// }
	/// </pre></blockquote></p>
	/// </summary>
	public interface IWritable
	{
		/// <summary>Serialize the fields of this object to <code>out</code>.</summary>
		/// <param name="writer"><code>DataOuput</code> to serialize this object into.</param>
		/// <exception cref="System.IO.IOException"/>
		void Write(BinaryWriter writer);

		/// <summary>Deserialize the fields of this object from <code>in</code>.</summary>
		/// <remarks>
		/// Deserialize the fields of this object from <code>in</code>.
		/// <p>For efficiency, implementations should attempt to re-use storage in the
		/// existing object where possible.</p>
		/// </remarks>
		/// <param name="reader"><code>BinaryReader</code> to deseriablize this object from.</param>
		/// <exception cref="System.IO.IOException"/>
		void ReadFields(BinaryReader reader);
	}
}
