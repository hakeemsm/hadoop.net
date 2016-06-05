using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Task.Reduce
{
	/// <summary><code>IFile.InMemoryReader</code> to read map-outputs present in-memory.
	/// 	</summary>
	public class InMemoryReader<K, V> : IFile.Reader<K, V>
	{
		private readonly TaskAttemptID taskAttemptId;

		private readonly MergeManagerImpl<K, V> merger;

		private readonly DataInputBuffer memDataIn = new DataInputBuffer();

		private readonly int start;

		private readonly int length;

		/// <exception cref="System.IO.IOException"/>
		public InMemoryReader(MergeManagerImpl<K, V> merger, TaskAttemptID taskAttemptId, 
			byte[] data, int start, int length, Configuration conf)
			: base(conf, null, length - start, null, null)
		{
			this.merger = merger;
			this.taskAttemptId = taskAttemptId;
			buffer = data;
			bufferSize = (int)fileLength;
			memDataIn.Reset(buffer, start, length - start);
			this.start = start;
			this.length = length;
		}

		public override void Reset(int offset)
		{
			memDataIn.Reset(buffer, start + offset, length - start - offset);
			bytesRead = offset;
			eof = false;
		}

		/// <exception cref="System.IO.IOException"/>
		public override long GetPosition()
		{
			// InMemoryReader does not initialize streams like Reader, so in.getPos()
			// would not work. Instead, return the number of uncompressed bytes read,
			// which will be correct since in-memory data is not compressed.
			return bytesRead;
		}

		public override long GetLength()
		{
			return fileLength;
		}

		private void DumpOnError()
		{
			FilePath dumpFile = new FilePath("../output/" + taskAttemptId + ".dump");
			System.Console.Error.WriteLine("Dumping corrupt map-output of " + taskAttemptId +
				 " to " + dumpFile.GetAbsolutePath());
			try
			{
				using (FileOutputStream fos = new FileOutputStream(dumpFile))
				{
					fos.Write(buffer, 0, bufferSize);
				}
			}
			catch (IOException)
			{
				System.Console.Error.WriteLine("Failed to dump map-output of " + taskAttemptId);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool NextRawKey(DataInputBuffer key)
		{
			try
			{
				if (!PositionToNextRecord(memDataIn))
				{
					return false;
				}
				// Setup the key
				int pos = memDataIn.GetPosition();
				byte[] data = memDataIn.GetData();
				key.Reset(data, pos, currentKeyLength);
				// Position for the next value
				long skipped = memDataIn.Skip(currentKeyLength);
				if (skipped != currentKeyLength)
				{
					throw new IOException("Rec# " + recNo + ": Failed to skip past key of length: " +
						 currentKeyLength);
				}
				// Record the byte
				bytesRead += currentKeyLength;
				return true;
			}
			catch (IOException ioe)
			{
				DumpOnError();
				throw;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void NextRawValue(DataInputBuffer value)
		{
			try
			{
				int pos = memDataIn.GetPosition();
				byte[] data = memDataIn.GetData();
				value.Reset(data, pos, currentValueLength);
				// Position for the next record
				long skipped = memDataIn.Skip(currentValueLength);
				if (skipped != currentValueLength)
				{
					throw new IOException("Rec# " + recNo + ": Failed to skip past value of length: "
						 + currentValueLength);
				}
				// Record the byte
				bytesRead += currentValueLength;
				++recNo;
			}
			catch (IOException ioe)
			{
				DumpOnError();
				throw;
			}
		}

		public override void Close()
		{
			// Release
			dataIn = null;
			buffer = null;
			// Inform the MergeManager
			if (merger != null)
			{
				merger.Unreserve(bufferSize);
			}
		}
	}
}
