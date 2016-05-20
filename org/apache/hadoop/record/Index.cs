using Sharpen;

namespace org.apache.hadoop.record
{
	/// <summary>Interface that acts as an iterator for deserializing maps.</summary>
	/// <remarks>
	/// Interface that acts as an iterator for deserializing maps.
	/// The deserializer returns an instance that the record uses to
	/// read vectors and maps. An example of usage is as follows:
	/// <code>
	/// Index idx = startVector(...);
	/// while (!idx.done()) {
	/// .... // read element of a vector
	/// idx.incr();
	/// }
	/// </code>
	/// </remarks>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public interface Index
	{
		bool done();

		void incr();
	}
}
