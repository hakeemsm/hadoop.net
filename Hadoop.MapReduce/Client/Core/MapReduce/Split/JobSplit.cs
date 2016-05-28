using System;
using System.IO;
using System.Text;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Split
{
	/// <summary>
	/// This class groups the fundamental classes associated with
	/// reading/writing splits.
	/// </summary>
	/// <remarks>
	/// This class groups the fundamental classes associated with
	/// reading/writing splits. The split information is divided into
	/// two parts based on the consumer of the information. The two
	/// parts are the split meta information, and the raw split
	/// information. The first part is consumed by the JobTracker to
	/// create the tasks' locality data structures. The second part is
	/// used by the maps at runtime to know what to do!
	/// These pieces of information are written to two separate files.
	/// The metainformation file is slurped by the JobTracker during
	/// job initialization. A map task gets the meta information during
	/// the launch and it reads the raw split bytes directly from the
	/// file.
	/// </remarks>
	public class JobSplit
	{
		internal const int MetaSplitVersion = 1;

		internal static readonly byte[] MetaSplitFileHeader;

		static JobSplit()
		{
			try
			{
				MetaSplitFileHeader = Sharpen.Runtime.GetBytesForString("META-SPL", "UTF-8");
			}
			catch (UnsupportedEncodingException u)
			{
				throw new RuntimeException(u);
			}
		}

		public static readonly JobSplit.TaskSplitMetaInfo EmptyTaskSplit = new JobSplit.TaskSplitMetaInfo
			();

		/// <summary>This represents the meta information about the task split.</summary>
		/// <remarks>
		/// This represents the meta information about the task split.
		/// The main fields are
		/// - start offset in actual split
		/// - data length that will be processed in this split
		/// - hosts on which this split is local
		/// </remarks>
		public class SplitMetaInfo : Writable
		{
			private long startOffset;

			private long inputDataLength;

			private string[] locations;

			public SplitMetaInfo()
			{
			}

			public SplitMetaInfo(string[] locations, long startOffset, long inputDataLength)
			{
				this.locations = locations;
				this.startOffset = startOffset;
				this.inputDataLength = inputDataLength;
			}

			/// <exception cref="System.IO.IOException"/>
			public SplitMetaInfo(InputSplit split, long startOffset)
			{
				try
				{
					this.locations = split.GetLocations();
					this.inputDataLength = split.GetLength();
					this.startOffset = startOffset;
				}
				catch (Exception ie)
				{
					throw new IOException(ie);
				}
			}

			public virtual string[] GetLocations()
			{
				return locations;
			}

			public virtual long GetStartOffset()
			{
				return startOffset;
			}

			public virtual long GetInputDataLength()
			{
				return inputDataLength;
			}

			public virtual void SetInputDataLocations(string[] locations)
			{
				this.locations = locations;
			}

			public virtual void SetInputDataLength(long length)
			{
				this.inputDataLength = length;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(DataInput @in)
			{
				int len = WritableUtils.ReadVInt(@in);
				locations = new string[len];
				for (int i = 0; i < locations.Length; i++)
				{
					locations[i] = Text.ReadString(@in);
				}
				startOffset = WritableUtils.ReadVLong(@in);
				inputDataLength = WritableUtils.ReadVLong(@in);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(DataOutput @out)
			{
				WritableUtils.WriteVInt(@out, locations.Length);
				for (int i = 0; i < locations.Length; i++)
				{
					Text.WriteString(@out, locations[i]);
				}
				WritableUtils.WriteVLong(@out, startOffset);
				WritableUtils.WriteVLong(@out, inputDataLength);
			}

			public override string ToString()
			{
				StringBuilder buf = new StringBuilder();
				buf.Append("data-size : " + inputDataLength + "\n");
				buf.Append("start-offset : " + startOffset + "\n");
				buf.Append("locations : " + "\n");
				foreach (string loc in locations)
				{
					buf.Append("  " + loc + "\n");
				}
				return buf.ToString();
			}
		}

		/// <summary>
		/// This represents the meta information about the task split that the
		/// JobTracker creates
		/// </summary>
		public class TaskSplitMetaInfo
		{
			private JobSplit.TaskSplitIndex splitIndex;

			private long inputDataLength;

			private string[] locations;

			public TaskSplitMetaInfo()
			{
				this.splitIndex = new JobSplit.TaskSplitIndex();
				this.locations = new string[0];
			}

			public TaskSplitMetaInfo(JobSplit.TaskSplitIndex splitIndex, string[] locations, 
				long inputDataLength)
			{
				this.splitIndex = splitIndex;
				this.locations = locations;
				this.inputDataLength = inputDataLength;
			}

			/// <exception cref="System.Exception"/>
			/// <exception cref="System.IO.IOException"/>
			public TaskSplitMetaInfo(InputSplit split, long startOffset)
				: this(new JobSplit.TaskSplitIndex(string.Empty, startOffset), split.GetLocations
					(), split.GetLength())
			{
			}

			public TaskSplitMetaInfo(string[] locations, long startOffset, long inputDataLength
				)
				: this(new JobSplit.TaskSplitIndex(string.Empty, startOffset), locations, inputDataLength
					)
			{
			}

			public virtual JobSplit.TaskSplitIndex GetSplitIndex()
			{
				return splitIndex;
			}

			public virtual string GetSplitLocation()
			{
				return splitIndex.GetSplitLocation();
			}

			public virtual long GetInputDataLength()
			{
				return inputDataLength;
			}

			public virtual string[] GetLocations()
			{
				return locations;
			}

			public virtual long GetStartOffset()
			{
				return splitIndex.GetStartOffset();
			}
		}

		/// <summary>
		/// This represents the meta information about the task split that the
		/// task gets
		/// </summary>
		public class TaskSplitIndex
		{
			private string splitLocation;

			private long startOffset;

			public TaskSplitIndex()
				: this(string.Empty, 0)
			{
			}

			public TaskSplitIndex(string splitLocation, long startOffset)
			{
				this.splitLocation = splitLocation;
				this.startOffset = startOffset;
			}

			public virtual long GetStartOffset()
			{
				return startOffset;
			}

			public virtual string GetSplitLocation()
			{
				return splitLocation;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(DataInput @in)
			{
				splitLocation = Org.Apache.Hadoop.IO.Text.ReadString(@in);
				startOffset = WritableUtils.ReadVLong(@in);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(DataOutput @out)
			{
				Org.Apache.Hadoop.IO.Text.WriteString(@out, splitLocation);
				WritableUtils.WriteVLong(@out, startOffset);
			}
		}
	}
}
