using System;
using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools.OfflineImageViewer
{
	/// <summary>This is the tool for analyzing file sizes in the namespace image.</summary>
	/// <remarks>
	/// This is the tool for analyzing file sizes in the namespace image. In order to
	/// run the tool one should define a range of integers <tt>[0, maxSize]</tt> by
	/// specifying <tt>maxSize</tt> and a <tt>step</tt>. The range of integers is
	/// divided into segments of size <tt>step</tt>:
	/// <tt>[0, s<sub>1</sub>, ..., s<sub>n-1</sub>, maxSize]</tt>, and the visitor
	/// calculates how many files in the system fall into each segment
	/// <tt>[s<sub>i-1</sub>, s<sub>i</sub>)</tt>. Note that files larger than
	/// <tt>maxSize</tt> always fall into the very last segment.
	/// <h3>Input.</h3>
	/// <ul>
	/// <li><tt>filename</tt> specifies the location of the image file;</li>
	/// <li><tt>maxSize</tt> determines the range <tt>[0, maxSize]</tt> of files
	/// sizes considered by the visitor;</li>
	/// <li><tt>step</tt> the range is divided into segments of size step.</li>
	/// </ul>
	/// <h3>Output.</h3> The output file is formatted as a tab separated two column
	/// table: Size and NumFiles. Where Size represents the start of the segment, and
	/// numFiles is the number of files form the image which size falls in this
	/// segment.
	/// </remarks>
	internal sealed class FileDistributionCalculator
	{
		private const long MaxSizeDefault = unchecked((long)(0x2000000000L));

		private const int IntervalDefault = unchecked((int)(0x200000));

		private const int MaxIntervals = unchecked((int)(0x8000000));

		private readonly Configuration conf;

		private readonly long maxSize;

		private readonly int steps;

		private readonly TextWriter @out;

		private readonly int[] distribution;

		private int totalFiles;

		private int totalDirectories;

		private int totalBlocks;

		private long totalSpace;

		private long maxFileSize;

		internal FileDistributionCalculator(Configuration conf, long maxSize, int steps, 
			TextWriter @out)
		{
			// 1/8 TB = 2^37
			// 2 MB = 2^21
			// 128 M = 2^27
			this.conf = conf;
			this.maxSize = maxSize == 0 ? MaxSizeDefault : maxSize;
			this.steps = steps == 0 ? IntervalDefault : steps;
			this.@out = @out;
			long numIntervals = this.maxSize / this.steps;
			// avoid OutOfMemoryError when allocating an array
			Preconditions.CheckState(numIntervals <= MaxIntervals, "Too many distribution intervals (maxSize/step): "
				 + numIntervals + ", should be less than " + (MaxIntervals + 1) + ".");
			this.distribution = new int[1 + (int)(numIntervals)];
		}

		/// <exception cref="System.IO.IOException"/>
		internal void Visit(RandomAccessFile file)
		{
			if (!FSImageUtil.CheckFileFormat(file))
			{
				throw new IOException("Unrecognized FSImage");
			}
			FsImageProto.FileSummary summary = FSImageUtil.LoadSummary(file);
			using (FileInputStream @in = new FileInputStream(file.GetFD()))
			{
				foreach (FsImageProto.FileSummary.Section s in summary.GetSectionsList())
				{
					if (FSImageFormatProtobuf.SectionName.FromString(s.GetName()) != FSImageFormatProtobuf.SectionName
						.Inode)
					{
						continue;
					}
					@in.GetChannel().Position(s.GetOffset());
					InputStream @is = FSImageUtil.WrapInputStreamForCompression(conf, summary.GetCodec
						(), new BufferedInputStream(new LimitInputStream(@in, s.GetLength())));
					Run(@is);
					Output();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void Run(InputStream @in)
		{
			FsImageProto.INodeSection s = FsImageProto.INodeSection.ParseDelimitedFrom(@in);
			for (int i = 0; i < s.GetNumInodes(); ++i)
			{
				FsImageProto.INodeSection.INode p = FsImageProto.INodeSection.INode.ParseDelimitedFrom
					(@in);
				if (p.GetType() == FsImageProto.INodeSection.INode.Type.File)
				{
					++totalFiles;
					FsImageProto.INodeSection.INodeFile f = p.GetFile();
					totalBlocks += f.GetBlocksCount();
					long fileSize = 0;
					foreach (HdfsProtos.BlockProto b in f.GetBlocksList())
					{
						fileSize += b.GetNumBytes();
					}
					maxFileSize = Math.Max(fileSize, maxFileSize);
					totalSpace += fileSize * f.GetReplication();
					int bucket = fileSize > maxSize ? distribution.Length - 1 : (int)Math.Ceil((double
						)fileSize / steps);
					++distribution[bucket];
				}
				else
				{
					if (p.GetType() == FsImageProto.INodeSection.INode.Type.Directory)
					{
						++totalDirectories;
					}
				}
				if (i % (1 << 20) == 0)
				{
					@out.WriteLine("Processed " + i + " inodes.");
				}
			}
		}

		private void Output()
		{
			// write the distribution into the output file
			@out.Write("Size\tNumFiles\n");
			for (int i = 0; i < distribution.Length; i++)
			{
				if (distribution[i] != 0)
				{
					@out.Write(((long)i * steps) + "\t" + distribution[i]);
					@out.Write('\n');
				}
			}
			@out.Write("totalFiles = " + totalFiles + "\n");
			@out.Write("totalDirectories = " + totalDirectories + "\n");
			@out.Write("totalBlocks = " + totalBlocks + "\n");
			@out.Write("totalSpace = " + totalSpace + "\n");
			@out.Write("maxFileSize = " + maxFileSize + "\n");
		}
	}
}
