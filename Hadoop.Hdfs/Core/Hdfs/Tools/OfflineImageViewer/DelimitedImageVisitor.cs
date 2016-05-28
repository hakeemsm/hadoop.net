using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools.OfflineImageViewer
{
	/// <summary>
	/// A DelimitedImageVisitor generates a text representation of the fsimage,
	/// with each element separated by a delimiter string.
	/// </summary>
	/// <remarks>
	/// A DelimitedImageVisitor generates a text representation of the fsimage,
	/// with each element separated by a delimiter string.  All of the elements
	/// common to both inodes and inodes-under-construction are included. When
	/// processing an fsimage with a layout version that did not include an
	/// element, such as AccessTime, the output file will include a column
	/// for the value, but no value will be included.
	/// Individual block information for each file is not currently included.
	/// The default delimiter is tab, as this is an unlikely value to be included
	/// an inode path or other text metadata.  The delimiter value can be via the
	/// constructor.
	/// </remarks>
	internal class DelimitedImageVisitor : TextWriterImageVisitor
	{
		private const string defaultDelimiter = "\t";

		private readonly List<ImageVisitor.ImageElement> elemQ = new List<ImageVisitor.ImageElement
			>();

		private long fileSize = 0l;

		private readonly ICollection<ImageVisitor.ImageElement> elementsToTrack;

		private readonly AbstractMap<ImageVisitor.ImageElement, string> elements = new Dictionary
			<ImageVisitor.ImageElement, string>();

		private readonly string delimiter;

		/// <exception cref="System.IO.IOException"/>
		public DelimitedImageVisitor(string filename)
			: this(filename, false)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public DelimitedImageVisitor(string outputFile, bool printToScreen)
			: this(outputFile, printToScreen, defaultDelimiter)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public DelimitedImageVisitor(string outputFile, bool printToScreen, string delimiter
			)
			: base(outputFile, printToScreen)
		{
			{
				// Elements of fsimage we're interested in tracking
				// Values for each of the elements in elementsToTrack
				elementsToTrack = new AList<ImageVisitor.ImageElement>();
				// This collection determines what elements are tracked and the order
				// in which they are output
				Sharpen.Collections.AddAll(elementsToTrack, ImageVisitor.ImageElement.InodePath, 
					ImageVisitor.ImageElement.Replication, ImageVisitor.ImageElement.ModificationTime
					, ImageVisitor.ImageElement.AccessTime, ImageVisitor.ImageElement.BlockSize, ImageVisitor.ImageElement
					.NumBlocks, ImageVisitor.ImageElement.NumBytes, ImageVisitor.ImageElement.NsQuota
					, ImageVisitor.ImageElement.DsQuota, ImageVisitor.ImageElement.PermissionString, 
					ImageVisitor.ImageElement.UserName, ImageVisitor.ImageElement.GroupName);
			}
			this.delimiter = delimiter;
			Reset();
		}

		/// <summary>
		/// Reset the values of the elements we're tracking in order to handle
		/// the next file
		/// </summary>
		private void Reset()
		{
			elements.Clear();
			foreach (ImageVisitor.ImageElement e in elementsToTrack)
			{
				elements[e] = null;
			}
			fileSize = 0l;
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void LeaveEnclosingElement()
		{
			ImageVisitor.ImageElement elem = elemQ.Pop();
			// If we're done with an inode, write out our results and start over
			if (elem == ImageVisitor.ImageElement.Inode || elem == ImageVisitor.ImageElement.
				InodeUnderConstruction)
			{
				WriteLine();
				Write("\n");
				Reset();
			}
		}

		/// <summary>
		/// Iterate through all the elements we're tracking and, if a value was
		/// recorded for it, write it out.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void WriteLine()
		{
			IEnumerator<ImageVisitor.ImageElement> it = elementsToTrack.GetEnumerator();
			while (it.HasNext())
			{
				ImageVisitor.ImageElement e = it.Next();
				string v = null;
				if (e == ImageVisitor.ImageElement.NumBytes)
				{
					v = fileSize.ToString();
				}
				else
				{
					v = elements[e];
				}
				if (v != null)
				{
					Write(v);
				}
				if (it.HasNext())
				{
					Write(delimiter);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void Visit(ImageVisitor.ImageElement element, string value)
		{
			// Explicitly label the root path
			if (element == ImageVisitor.ImageElement.InodePath && value.Equals(string.Empty))
			{
				value = "/";
			}
			// Special case of file size, which is sum of the num bytes in each block
			if (element == ImageVisitor.ImageElement.NumBytes)
			{
				fileSize += long.Parse(value);
			}
			if (elements.Contains(element) && element != ImageVisitor.ImageElement.NumBytes)
			{
				elements[element] = value;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void VisitEnclosingElement(ImageVisitor.ImageElement element)
		{
			elemQ.Push(element);
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void VisitEnclosingElement(ImageVisitor.ImageElement element, ImageVisitor.ImageElement
			 key, string value)
		{
			// Special case as numBlocks is an attribute of the blocks element
			if (key == ImageVisitor.ImageElement.NumBlocks && elements.Contains(ImageVisitor.ImageElement
				.NumBlocks))
			{
				elements[key] = value;
			}
			elemQ.Push(element);
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void Start()
		{
		}
		/* Nothing to do */
	}
}
