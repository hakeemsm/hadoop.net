using System;
using System.Collections.Generic;
using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools.OfflineImageViewer
{
	/// <summary>File size distribution visitor.</summary>
	/// <remarks>
	/// File size distribution visitor.
	/// <h3>Description.</h3>
	/// This is the tool for analyzing file sizes in the namespace image.
	/// In order to run the tool one should define a range of integers
	/// <tt>[0, maxSize]</tt> by specifying <tt>maxSize</tt> and a <tt>step</tt>.
	/// The range of integers is divided into segments of size <tt>step</tt>:
	/// <tt>[0, s<sub>1</sub>, ..., s<sub>n-1</sub>, maxSize]</tt>,
	/// and the visitor calculates how many files in the system fall into
	/// each segment <tt>[s<sub>i-1</sub>, s<sub>i</sub>)</tt>.
	/// Note that files larger than <tt>maxSize</tt> always fall into
	/// the very last segment.
	/// <h3>Input.</h3>
	/// <ul>
	/// <li><tt>filename</tt> specifies the location of the image file;</li>
	/// <li><tt>maxSize</tt> determines the range <tt>[0, maxSize]</tt> of files
	/// sizes considered by the visitor;</li>
	/// <li><tt>step</tt> the range is divided into segments of size step.</li>
	/// </ul>
	/// <h3>Output.</h3>
	/// The output file is formatted as a tab separated two column table:
	/// Size and NumFiles. Where Size represents the start of the segment,
	/// and numFiles is the number of files form the image which size falls in
	/// this segment.
	/// </remarks>
	internal class FileDistributionVisitor : TextWriterImageVisitor
	{
		private readonly List<ImageVisitor.ImageElement> elemS = new List<ImageVisitor.ImageElement
			>();

		private const long MaxSizeDefault = unchecked((long)(0x2000000000L));

		private const int IntervalDefault = unchecked((int)(0x200000));

		private int[] distribution;

		private long maxSize;

		private int step;

		private int totalFiles;

		private int totalDirectories;

		private int totalBlocks;

		private long totalSpace;

		private long maxFileSize;

		private FileDistributionVisitor.FileContext current;

		private bool inInode = false;

		/// <summary>File or directory information.</summary>
		private class FileContext
		{
			internal string path;

			internal long fileSize;

			internal int numBlocks;

			internal int replication;
			// 1/8 TB = 2^37
			// 2 MB = 2^21
		}

		/// <exception cref="System.IO.IOException"/>
		public FileDistributionVisitor(string filename, long maxSize, int step)
			: base(filename, false)
		{
			this.maxSize = (maxSize == 0 ? MaxSizeDefault : maxSize);
			this.step = (step == 0 ? IntervalDefault : step);
			long numIntervals = this.maxSize / this.step;
			if (numIntervals >= int.MaxValue)
			{
				throw new IOException("Too many distribution intervals " + numIntervals);
			}
			this.distribution = new int[1 + (int)(numIntervals)];
			this.totalFiles = 0;
			this.totalDirectories = 0;
			this.totalBlocks = 0;
			this.totalSpace = 0;
			this.maxFileSize = 0;
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void Start()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void Finish()
		{
			Output();
			base.Finish();
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void FinishAbnormally()
		{
			System.Console.Out.WriteLine("*** Image processing finished abnormally.  Ending ***"
				);
			Output();
			base.FinishAbnormally();
		}

		/// <exception cref="System.IO.IOException"/>
		private void Output()
		{
			// write the distribution into the output file
			Write("Size\tNumFiles\n");
			for (int i = 0; i < distribution.Length; i++)
			{
				Write(((long)i * step) + "\t" + distribution[i] + "\n");
			}
			System.Console.Out.WriteLine("totalFiles = " + totalFiles);
			System.Console.Out.WriteLine("totalDirectories = " + totalDirectories);
			System.Console.Out.WriteLine("totalBlocks = " + totalBlocks);
			System.Console.Out.WriteLine("totalSpace = " + totalSpace);
			System.Console.Out.WriteLine("maxFileSize = " + maxFileSize);
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void LeaveEnclosingElement()
		{
			ImageVisitor.ImageElement elem = elemS.Pop();
			if (elem != ImageVisitor.ImageElement.Inode && elem != ImageVisitor.ImageElement.
				InodeUnderConstruction)
			{
				return;
			}
			inInode = false;
			if (current.numBlocks < 0)
			{
				totalDirectories++;
				return;
			}
			totalFiles++;
			totalBlocks += current.numBlocks;
			totalSpace += current.fileSize * current.replication;
			if (maxFileSize < current.fileSize)
			{
				maxFileSize = current.fileSize;
			}
			int high;
			if (current.fileSize > maxSize)
			{
				high = distribution.Length - 1;
			}
			else
			{
				high = (int)Math.Ceil((double)current.fileSize / step);
			}
			distribution[high]++;
			if (totalFiles % 1000000 == 1)
			{
				System.Console.Out.WriteLine("Files processed: " + totalFiles + "  Current: " + current
					.path);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void Visit(ImageVisitor.ImageElement element, string value)
		{
			if (inInode)
			{
				switch (element)
				{
					case ImageVisitor.ImageElement.InodePath:
					{
						current.path = (value.Equals(string.Empty) ? "/" : value);
						break;
					}

					case ImageVisitor.ImageElement.Replication:
					{
						current.replication = System.Convert.ToInt32(value);
						break;
					}

					case ImageVisitor.ImageElement.NumBytes:
					{
						current.fileSize += long.Parse(value);
						break;
					}

					default:
					{
						break;
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void VisitEnclosingElement(ImageVisitor.ImageElement element)
		{
			elemS.Push(element);
			if (element == ImageVisitor.ImageElement.Inode || element == ImageVisitor.ImageElement
				.InodeUnderConstruction)
			{
				current = new FileDistributionVisitor.FileContext();
				inInode = true;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void VisitEnclosingElement(ImageVisitor.ImageElement element, ImageVisitor.ImageElement
			 key, string value)
		{
			elemS.Push(element);
			if (element == ImageVisitor.ImageElement.Inode || element == ImageVisitor.ImageElement
				.InodeUnderConstruction)
			{
				inInode = true;
			}
			else
			{
				if (element == ImageVisitor.ImageElement.Blocks)
				{
					current.numBlocks = System.Convert.ToInt32(value);
				}
			}
		}
	}
}
