using System;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools.OfflineImageViewer
{
	/// <summary>
	/// IndentedImageVisitor walks over an FSImage and displays its structure
	/// using indenting to organize sections within the image file.
	/// </summary>
	internal class IndentedImageVisitor : TextWriterImageVisitor
	{
		/// <exception cref="System.IO.IOException"/>
		public IndentedImageVisitor(string filename)
			: base(filename)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public IndentedImageVisitor(string filename, bool printToScreen)
			: base(filename, printToScreen)
		{
		}

		private readonly DepthCounter dc = new DepthCounter();

		// to track leading spacing
		/// <exception cref="System.IO.IOException"/>
		internal override void Start()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void Finish()
		{
			base.Finish();
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void FinishAbnormally()
		{
			System.Console.Out.WriteLine("*** Image processing finished abnormally.  Ending ***"
				);
			base.FinishAbnormally();
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void LeaveEnclosingElement()
		{
			dc.DecLevel();
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void Visit(ImageVisitor.ImageElement element, string value)
		{
			PrintIndents();
			Write(element + " = " + value + "\n");
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void Visit(ImageVisitor.ImageElement element, long value)
		{
			if ((element == ImageVisitor.ImageElement.DelegationTokenIdentifierExpiryTime) ||
				 (element == ImageVisitor.ImageElement.DelegationTokenIdentifierIssueDate) || (element
				 == ImageVisitor.ImageElement.DelegationTokenIdentifierMaxDate))
			{
				Visit(element, Sharpen.Extensions.CreateDate(value).ToString());
			}
			else
			{
				Visit(element, System.Convert.ToString(value));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void VisitEnclosingElement(ImageVisitor.ImageElement element)
		{
			PrintIndents();
			Write(element + "\n");
			dc.IncLevel();
		}

		// Print element, along with associated key/value pair, in brackets
		/// <exception cref="System.IO.IOException"/>
		internal override void VisitEnclosingElement(ImageVisitor.ImageElement element, ImageVisitor.ImageElement
			 key, string value)
		{
			PrintIndents();
			Write(element + " [" + key + " = " + value + "]\n");
			dc.IncLevel();
		}

		/// <summary>Print an appropriate number of spaces for the current level.</summary>
		/// <remarks>
		/// Print an appropriate number of spaces for the current level.
		/// FsImages can potentially be millions of lines long, so caching can
		/// significantly speed up output.
		/// </remarks>
		private static readonly string[] indents = new string[] { string.Empty, "  ", "    "
			, "      ", "        ", "          ", "            " };

		/// <exception cref="System.IO.IOException"/>
		private void PrintIndents()
		{
			try
			{
				Write(indents[dc.GetLevel()]);
			}
			catch (IndexOutOfRangeException)
			{
				// There's no reason in an fsimage would need a deeper indent
				for (int i = 0; i < dc.GetLevel(); i++)
				{
					Write(" ");
				}
			}
		}
	}
}
