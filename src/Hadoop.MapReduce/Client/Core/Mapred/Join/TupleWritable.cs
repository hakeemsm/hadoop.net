using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Join
{
	/// <summary>
	/// Writable type storing multiple
	/// <see cref="Org.Apache.Hadoop.IO.Writable"/>
	/// s.
	/// This is *not* a general-purpose tuple type. In almost all cases, users are
	/// encouraged to implement their own serializable types, which can perform
	/// better validation and provide more efficient encodings than this class is
	/// capable. TupleWritable relies on the join framework for type safety and
	/// assumes its instances will rarely be persisted, assumptions not only
	/// incompatible with, but contrary to the general case.
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.IO.Writable"/>
	public class TupleWritable : Org.Apache.Hadoop.Mapreduce.Lib.Join.TupleWritable
	{
		/// <summary>Create an empty tuple with no allocated storage for writables.</summary>
		public TupleWritable()
			: base()
		{
		}

		/// <summary>
		/// Initialize tuple with storage; unknown whether any of them contain
		/// &quot;written&quot; values.
		/// </summary>
		public TupleWritable(Writable[] vals)
			: base(vals)
		{
		}

		/// <summary>Record that the tuple contains an element at the position provided.</summary>
		internal override void SetWritten(int i)
		{
			written.Set(i);
		}

		/// <summary>
		/// Record that the tuple does not contain an element at the position
		/// provided.
		/// </summary>
		internal override void ClearWritten(int i)
		{
			written.Clear(i);
		}

		/// <summary>
		/// Clear any record of which writables have been written to, without
		/// releasing storage.
		/// </summary>
		internal override void ClearWritten()
		{
			written.Clear();
		}
	}
}
