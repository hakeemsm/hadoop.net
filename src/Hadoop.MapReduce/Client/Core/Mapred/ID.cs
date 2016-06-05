using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// A general identifier, which internally stores the id
	/// as an integer.
	/// </summary>
	/// <remarks>
	/// A general identifier, which internally stores the id
	/// as an integer. This is the super class of
	/// <see cref="JobID"/>
	/// ,
	/// <see cref="TaskID"/>
	/// and
	/// <see cref="TaskAttemptID"/>
	/// .
	/// </remarks>
	/// <seealso cref="JobID"/>
	/// <seealso cref="TaskID"/>
	/// <seealso cref="TaskAttemptID"/>
	public abstract class ID : ID
	{
		/// <summary>constructs an ID object from the given int</summary>
		public ID(int id)
			: base(id)
		{
		}

		protected internal ID()
		{
		}
	}
}
