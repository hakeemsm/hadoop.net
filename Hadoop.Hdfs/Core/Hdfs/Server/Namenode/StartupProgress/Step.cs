using Org.Apache.Commons.Lang.Builder;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress
{
	/// <summary>
	/// A step performed by the namenode during a
	/// <see cref="Phase"/>
	/// of startup.
	/// </summary>
	public class Step : Comparable<Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress.Step
		>
	{
		private static readonly AtomicInteger Sequence = new AtomicInteger();

		private readonly string file;

		private readonly int sequenceNumber;

		private readonly long size;

		private readonly StepType type;

		/// <summary>Creates a new Step.</summary>
		/// <param name="type">StepType type of step</param>
		public Step(StepType type)
			: this(type, null, long.MinValue)
		{
		}

		/// <summary>Creates a new Step.</summary>
		/// <param name="file">String file</param>
		public Step(string file)
			: this(null, file, long.MinValue)
		{
		}

		/// <summary>Creates a new Step.</summary>
		/// <param name="file">String file</param>
		/// <param name="size">long size in bytes</param>
		public Step(string file, long size)
			: this(null, file, size)
		{
		}

		/// <summary>Creates a new Step.</summary>
		/// <param name="type">StepType type of step</param>
		/// <param name="file">String file</param>
		public Step(StepType type, string file)
			: this(type, file, long.MinValue)
		{
		}

		/// <summary>Creates a new Step.</summary>
		/// <param name="type">StepType type of step</param>
		/// <param name="file">String file</param>
		/// <param name="size">long size in bytes</param>
		public Step(StepType type, string file, long size)
		{
			this.file = file;
			this.sequenceNumber = Sequence.IncrementAndGet();
			this.size = size;
			this.type = type;
		}

		public virtual int CompareTo(Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress.Step
			 other)
		{
			// Sort steps by file and then sequentially within the file to achieve the
			// desired order.  There is no concurrent map structure in the JDK that
			// maintains insertion order, so instead we attach a sequence number to each
			// step and sort on read.
			return new CompareToBuilder().Append(file, other.file).Append(sequenceNumber, other
				.sequenceNumber).ToComparison();
		}

		public override bool Equals(object otherObj)
		{
			if (otherObj == null || otherObj.GetType() != GetType())
			{
				return false;
			}
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress.Step other = (Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress.Step
				)otherObj;
			return new EqualsBuilder().Append(this.file, other.file).Append(this.size, other.
				size).Append(this.type, other.type).IsEquals();
		}

		/// <summary>Returns the optional file name, possibly null.</summary>
		/// <returns>String optional file name, possibly null</returns>
		public virtual string GetFile()
		{
			return file;
		}

		/// <summary>Returns the optional size in bytes, possibly Long.MIN_VALUE if undefined.
		/// 	</summary>
		/// <returns>long optional size in bytes, possibly Long.MIN_VALUE</returns>
		public virtual long GetSize()
		{
			return size;
		}

		/// <summary>Returns the optional step type, possibly null.</summary>
		/// <returns>StepType optional step type, possibly null</returns>
		public virtual StepType GetType()
		{
			return type;
		}

		public override int GetHashCode()
		{
			return new HashCodeBuilder().Append(file).Append(size).Append(type).ToHashCode();
		}
	}
}
