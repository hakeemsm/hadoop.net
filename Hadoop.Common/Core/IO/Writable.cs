namespace Hadoop.Common.Core.IO
{
	/// <summary>
	/// A serializable object which implements a simple, efficient, serialization
	/// protocol, based on
	/// <see cref="System.IO.DataInput"/>
	/// and
	/// <see cref="System.IO.DataOutput"/>
	/// .
	/// <p>Any <code>key</code> or <code>value</code> type in the Hadoop Map-Reduce
	/// framework implements this interface.</p>
	/// <p>Implementations typically implement a static <code>read(DataInput)</code>
	/// method which constructs a new instance, calls
	/// <see cref="ReadFields(System.IO.DataInput)"/>
	/// 
	/// and returns the instance.</p>
	/// <p>Example:</p>
	/// <p><blockquote><pre>
	/// public class MyWritable implements Writable {
	/// // Some data
	/// private int counter;
	/// private long timestamp;
	/// public void write(DataOutput out) throws IOException {
	/// out.writeInt(counter);
	/// out.writeLong(timestamp);
	/// }
	/// public void readFields(DataInput in) throws IOException {
	/// counter = in.readInt();
	/// timestamp = in.readLong();
	/// }
	/// public static MyWritable read(DataInput in) throws IOException {
	/// MyWritable w = new MyWritable();
	/// w.readFields(in);
	/// return w;
	/// }
	/// }
	/// </pre></blockquote></p>
	/// </summary>
	public interface Writable
	{
		/// <summary>Serialize the fields of this object to <code>out</code>.</summary>
		/// <param name="out"><code>DataOuput</code> to serialize this object into.</param>
		/// <exception cref="System.IO.IOException"/>
		void Write(DataOutput @out);

		/// <summary>Deserialize the fields of this object from <code>in</code>.</summary>
		/// <remarks>
		/// Deserialize the fields of this object from <code>in</code>.
		/// <p>For efficiency, implementations should attempt to re-use storage in the
		/// existing object where possible.</p>
		/// </remarks>
		/// <param name="in"><code>DataInput</code> to deseriablize this object from.</param>
		/// <exception cref="System.IO.IOException"/>
		void ReadFields(DataInput @in);
	}
}
