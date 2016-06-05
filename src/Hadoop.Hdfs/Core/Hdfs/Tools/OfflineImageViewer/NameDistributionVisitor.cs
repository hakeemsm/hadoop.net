using System;
using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools.OfflineImageViewer
{
	/// <summary>File name distribution visitor.</summary>
	/// <remarks>
	/// File name distribution visitor.
	/// <p>
	/// It analyzes file names in fsimage and prints the following information:
	/// <li>Number of unique file names</li>
	/// <li>Number file names and the corresponding number range of files that use
	/// these same names</li>
	/// <li>Heap saved if the file name objects are reused</li>
	/// </remarks>
	public class NameDistributionVisitor : TextWriterImageVisitor
	{
		internal Dictionary<string, int> counts = new Dictionary<string, int>();

		/// <exception cref="System.IO.IOException"/>
		public NameDistributionVisitor(string filename, bool printToScreen)
			: base(filename, printToScreen)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void Finish()
		{
			int BytearrayOverhead = 24;
			Write("Total unique file names " + counts.Count);
			// Columns: Frequency of file occurrence, savings in heap, total files using
			// the name and number of file names
			long[][] stats = new long[][] { new long[] { 100000, 0, 0, 0 }, new long[] { 10000
				, 0, 0, 0 }, new long[] { 1000, 0, 0, 0 }, new long[] { 100, 0, 0, 0 }, new long
				[] { 10, 0, 0, 0 }, new long[] { 5, 0, 0, 0 }, new long[] { 4, 0, 0, 0 }, new long
				[] { 3, 0, 0, 0 }, new long[] { 2, 0, 0, 0 } };
			int highbound = int.MinValue;
			foreach (KeyValuePair<string, int> entry in counts)
			{
				highbound = Math.Max(highbound, entry.Value);
				for (int i = 0; i < stats.Length; i++)
				{
					if (entry.Value >= stats[i][0])
					{
						stats[i][1] += (BytearrayOverhead + entry.Key.Length) * (entry.Value - 1);
						stats[i][2] += entry.Value;
						stats[i][3]++;
						break;
					}
				}
			}
			long lowbound = 0;
			long totalsavings = 0;
			foreach (long[] stat in stats)
			{
				lowbound = stat[0];
				totalsavings += stat[1];
				string range = lowbound == highbound ? " " + lowbound : " between " + lowbound + 
					"-" + highbound;
				Write("\n" + stat[3] + " names are used by " + stat[2] + " files" + range + " times. Heap savings ~"
					 + stat[1] + " bytes.");
				highbound = (int)stat[0] - 1;
			}
			Write("\n\nTotal saved heap ~" + totalsavings + "bytes.\n");
			base.Finish();
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void Visit(ImageVisitor.ImageElement element, string value)
		{
			if (element == ImageVisitor.ImageElement.InodePath)
			{
				string filename = Sharpen.Runtime.Substring(value, value.LastIndexOf("/") + 1);
				if (counts.Contains(filename))
				{
					counts[filename] = counts[filename] + 1;
				}
				else
				{
					counts[filename] = 1;
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void LeaveEnclosingElement()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void Start()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void VisitEnclosingElement(ImageVisitor.ImageElement element)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void VisitEnclosingElement(ImageVisitor.ImageElement element, ImageVisitor.ImageElement
			 key, string value)
		{
		}
	}
}
