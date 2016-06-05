using System.IO;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
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
	public abstract class ID : WritableComparable<ID>
	{
		protected internal const char Separator = '_';

		protected internal int id;

		/// <summary>constructs an ID object from the given int</summary>
		public ID(int id)
		{
			this.id = id;
		}

		protected internal ID()
		{
		}

		/// <summary>returns the int which represents the identifier</summary>
		public virtual int GetId()
		{
			return id;
		}

		public override string ToString()
		{
			return id.ToString();
		}

		public override int GetHashCode()
		{
			return id;
		}

		public override bool Equals(object o)
		{
			if (this == o)
			{
				return true;
			}
			if (o == null)
			{
				return false;
			}
			if (o.GetType() == this.GetType())
			{
				ID that = (ID)o;
				return this.id == that.id;
			}
			else
			{
				return false;
			}
		}

		/// <summary>Compare IDs by associated numbers</summary>
		public virtual int CompareTo(ID that)
		{
			return this.id - that.id;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			this.id = @in.ReadInt();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			@out.WriteInt(id);
		}
	}
}
