using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Hdfs.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools.OfflineImageViewer
{
	/// <summary>
	/// An XmlImageVisitor walks over an fsimage structure and writes out
	/// an equivalent XML document that contains the fsimage's components.
	/// </summary>
	public class XmlImageVisitor : TextWriterImageVisitor
	{
		private readonly List<ImageVisitor.ImageElement> tagQ = new List<ImageVisitor.ImageElement
			>();

		/// <exception cref="System.IO.IOException"/>
		public XmlImageVisitor(string filename)
			: base(filename, false)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public XmlImageVisitor(string filename, bool printToScreen)
			: base(filename, printToScreen)
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
			Write("\n<!-- Error processing image file.  Exiting -->\n");
			base.FinishAbnormally();
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void LeaveEnclosingElement()
		{
			if (tagQ.Count == 0)
			{
				throw new IOException("Tried to exit non-existent enclosing element " + "in FSImage file"
					);
			}
			ImageVisitor.ImageElement element = tagQ.Pop();
			Write("</" + element.ToString() + ">\n");
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void Start()
		{
			Write("<?xml version=\"1.0\" ?>\n");
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void Visit(ImageVisitor.ImageElement element, string value)
		{
			WriteTag(element.ToString(), value);
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void VisitEnclosingElement(ImageVisitor.ImageElement element)
		{
			Write("<" + element.ToString() + ">\n");
			tagQ.Push(element);
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void VisitEnclosingElement(ImageVisitor.ImageElement element, ImageVisitor.ImageElement
			 key, string value)
		{
			Write("<" + element.ToString() + " " + key + "=\"" + value + "\">\n");
			tagQ.Push(element);
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteTag(string tag, string value)
		{
			Write("<" + tag + ">" + XMLUtils.MangleXmlString(value, true) + "</" + tag + ">\n"
				);
		}
	}
}
